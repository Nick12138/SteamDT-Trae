from pathlib import Path
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, BigInteger,
    ForeignKey, Index, UniqueConstraint, DateTime, func, text
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker


# 确保数据目录存在
Path("data").mkdir(parents=True, exist_ok=True)

# SQLite 数据库文件
DATABASE_URL = "sqlite:///data/app.db"

engine = create_engine(
    DATABASE_URL,
    future=True,
    echo=False,
    connect_args={
        # 允许跨线程使用同一连接池中的连接（每个线程获取独立 Session）
        "check_same_thread": False,
        # SQLite 锁竞争时等待时间（毫秒为单位在 PRAGMA 中设置，这里为秒）
        "timeout": 30,
    },
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

Base = declarative_base()


class Item(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    market_hash_name = Column(String(255), nullable=False, unique=True, index=True)

    platforms = relationship("Platform", back_populates="item", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "name": self.name,
            "marketHashName": self.market_hash_name,
            "platformList": [p.to_dict() for p in self.platforms],
        }


class Platform(Base):
    __tablename__ = "platforms"
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(64), nullable=False)
    platform_item_id = Column(String(64), nullable=True)

    item = relationship("Item", back_populates="platforms")

    __table_args__ = (
        UniqueConstraint("item_id", "name", name="uq_platform_item_name"),
        Index("idx_platform_name", "name"),
    )

    def to_dict(self):
        return {
            "name": self.name,
            "itemId": self.platform_item_id,
        }


class Price(Base):
    __tablename__ = "prices"
    id = Column(Integer, primary_key=True)
    market_hash_name = Column(String(255), nullable=False, index=True)
    platform = Column(String(64), nullable=True, index=True)
    platform_item_id = Column(String(64), nullable=True)
    # 新增关系列：指向 items / platforms（为兼容旧数据可为空）
    item_id = Column(Integer, ForeignKey("items.id", ondelete="SET NULL"), nullable=True, index=True)
    platform_id = Column(Integer, ForeignKey("platforms.id", ondelete="SET NULL"), nullable=True, index=True)
    sell_price = Column(Float, nullable=True)
    bidding_price = Column(Float, nullable=True)
    # 新增计数字段
    sell_count = Column(Integer, nullable=True)
    bidding_count = Column(Integer, nullable=True)
    update_time = Column(BigInteger, nullable=True)  # 可能是毫秒时间戳
    # 北京时间文本（人类可读），例如 2025-10-15 16:22:02
    update_time_text = Column(String(32), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_prices_mhn_platform", "market_hash_name", "platform"),
        Index("idx_prices_item_platform", "item_id", "platform_id", "update_time", "created_at"),
    )


def init_db():
    Base.metadata.create_all(engine)
    # 启用 WAL 与 busy_timeout 以改善并发写入
    try:
        with engine.connect() as conn:
            conn.execute(text('PRAGMA journal_mode=WAL'))
            conn.execute(text('PRAGMA busy_timeout=30000'))
    except Exception:
        # 非关键错误，忽略
        pass
    migrate_prices_table()


def migrate_prices_table():
    """
    迁移 prices 表：添加 item_id 与 platform_id 列（若不存在），并尽力补齐数据。
    注意：SQLite 的 ALTER 能力有限，这里仅添加列与索引，不添加外键约束。
    """
    try:
        with engine.connect() as conn:
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(prices)")).fetchall()]
            need_item = "item_id" not in cols
            need_platform = "platform_id" not in cols
            need_sell_count = "sell_count" not in cols
            need_bidding_count = "bidding_count" not in cols
            need_update_time_text = "update_time_text" not in cols
            if need_item:
                conn.execute(text('ALTER TABLE "prices" ADD COLUMN item_id INTEGER'))
            if need_platform:
                conn.execute(text('ALTER TABLE "prices" ADD COLUMN platform_id INTEGER'))
            if need_sell_count:
                conn.execute(text('ALTER TABLE "prices" ADD COLUMN sell_count INTEGER'))
            if need_bidding_count:
                conn.execute(text('ALTER TABLE "prices" ADD COLUMN bidding_count INTEGER'))
            if need_update_time_text:
                conn.execute(text('ALTER TABLE "prices" ADD COLUMN update_time_text TEXT'))
            # 索引创建（若不存在）
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_prices_item_platform ON prices (item_id, platform_id, update_time, created_at)'))
    except Exception:
        # 若迁移失败，不中断应用启动；后续写入仍可按旧结构工作
        pass

    # 尝试补齐 item_id 与 platform_id
    try:
        sess = SessionLocal()
        try:
            # 仅处理缺少 item_id 的记录，避免重复工作
            orphan_rows = sess.query(Price).filter(Price.item_id == None).limit(5000).all()
            # 简单规范平台名称（与 app 中逻辑一致）
            def canon(name: str):
                p = (name or "").strip().upper()
                if p == "C5":
                    return "C5GAME"
                if p == "HALO":
                    return "HALOSKINS"
                return p
            # 将 market_hash_name 映射到 item
            # 为减少查询次数，预取所有相关 Item
            mhns = list({r.market_hash_name for r in orphan_rows if r.market_hash_name})
            items = sess.query(Item).filter(Item.market_hash_name.in_(mhns)).all()
            mhn_to_item = {it.market_hash_name: it for it in items}
            # 对每行尝试补齐 item_id 与 platform_id
            for r in orphan_rows:
                it = mhn_to_item.get(r.market_hash_name)
                if it:
                    r.item_id = it.id
                    # 补齐 platform_id
                    cname = canon(r.platform)
                    if cname:
                        plat = (
                            sess.query(Platform)
                            .filter(Platform.item_id == it.id, Platform.name == cname)
                            .one_or_none()
                        )
                        if plat:
                            r.platform_id = plat.id
            sess.commit()
        except Exception:
            sess.rollback()
            # 不中断启动
        finally:
            sess.close()
    except Exception:
        # 不中断启动
        pass

    # 统一 update_time 存储为毫秒：将历史上以秒存储的记录乘以 1000
    try:
        with engine.begin() as conn:
            conn.execute(text(
                """
                UPDATE prices
                SET update_time = update_time * 1000
                WHERE update_time IS NOT NULL AND update_time < 1000000000000
                """
            ))
    except Exception:
        # 出错不影响应用启动
        pass

    # 回填北京时间文本：对有时间戳但没有文本的记录，填充 'YYYY-MM-DD HH:MM:SS'
    try:
        with engine.begin() as conn:
            conn.execute(text(
                """
                UPDATE prices
                SET update_time_text = datetime(update_time / 1000, 'unixepoch', '+8 hours')
                WHERE update_time IS NOT NULL AND (update_time_text IS NULL OR update_time_text = '')
                """
            ))
    except Exception:
        # 不中断启动
        pass