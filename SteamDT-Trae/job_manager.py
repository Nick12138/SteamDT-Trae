import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict, Any

from sqlalchemy import func
from db import SessionLocal, Item, Platform, Price


def canonical_platform_name(name: str) -> str:
    p = (name or "").strip().upper()
    if p == "C5":
        return "C5GAME"
    if p == "HALO":
        return "HALOSKINS"
    return p


def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None


def _to_int(x):
    try:
        return int(x)
    except Exception:
        return None


def _format_beijing_text(ms: Optional[int]) -> Optional[str]:
    try:
        if ms is None:
            return None
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone(timedelta(hours=8)))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None


class PriceBatchJob:
    """后台价格批量抓取任务：每分钟执行一次，每次处理固定数量的饰品。"""

    def __init__(self, client, get_session, batch_size: int = 100, interval_sec: int = 60):
        self.client = client
        self.get_session = get_session
        self.default_batch_size = max(1, int(batch_size))
        self.default_interval = max(1, int(interval_sec))

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._lock = threading.Lock()

        # 状态字段
        self.running: bool = False
        self.paused: bool = False
        self.max_id: int = 0
        self.completed_count: int = 0
        self.current_start_id: int = 0
        self.batch_size: int = self.default_batch_size
        self.interval_sec: int = self.default_interval
        self.last_processed_range: Optional[Tuple[int, int]] = None
        self.next_run_ts: Optional[float] = None

    # 公开控制方法
    def start(self, start_id: Optional[int] = None, batch_size: Optional[int] = None, interval_sec: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            if self.running:
                return self.status()
            self._stop_event.clear()
            self._pause_event.clear()
            self.paused = False
            self.running = True
            self.batch_size = max(1, int(batch_size or self.default_batch_size))
            self.interval_sec = max(1, int(interval_sec or self.default_interval))

            # 计算最大ID
            sess = self.get_session()
            try:
                self.max_id = int(sess.query(func.max(Item.id)).scalar() or 0)
            finally:
                sess.close()

            self.current_start_id = int(start_id or 1)
            self.completed_count = max(0, self.current_start_id - 1)
            self.last_processed_range = None
            self.next_run_ts = None

            self._thread = threading.Thread(target=self._loop, name="PriceBatchJob", daemon=True)
            self._thread.start()
        return self.status()

    def pause(self) -> Dict[str, Any]:
        with self._lock:
            if self.running and not self.paused:
                self.paused = True
                self._pause_event.set()
        return self.status()

    def resume(self) -> Dict[str, Any]:
        with self._lock:
            if self.running and self.paused:
                self.paused = False
                self._pause_event.clear()
        return self.status()

    def stop(self) -> Dict[str, Any]:
        with self._lock:
            if self.running:
                self._stop_event.set()
        # 等待线程退出
        t = None
        with self._lock:
            t = self._thread
        if t and t.is_alive():
            try:
                t.join(timeout=2.0)
            except Exception:
                pass
        with self._lock:
            self.running = False
            self.paused = False
            self._thread = None
            self.next_run_ts = None
        return self.status()

    # 主循环
    def _loop(self):
        while not self._stop_event.is_set():
            # 暂停等待
            while self.paused and not self._stop_event.is_set():
                time.sleep(0.2)

            if self._stop_event.is_set():
                break

            # 执行一次区间
            try:
                self._run_one_range()
            except Exception:
                # 忽略单次异常，继续下一轮
                pass

            # 完成后检查是否结束
            with self._lock:
                done = self.completed_count >= self.max_id
                self.next_run_ts = time.time() + self.interval_sec
            if done:
                # 自动停止
                self.stop()
                break

            # 间隔等待
            remain = self.interval_sec
            while remain > 0 and not self._stop_event.is_set():
                if self.paused:
                    break
                time.sleep(0.2)
                remain -= 0.2

    def _run_one_range(self):
        with self._lock:
            start_id = self.current_start_id
            end_id = min(self.max_id, start_id + self.batch_size - 1)

        if start_id > end_id or start_id <= 0:
            return

        sess = self.get_session()
        try:
            items = (
                sess.query(Item)
                .filter(Item.id >= start_id, Item.id <= end_id)
                .order_by(Item.id.asc())
                .all()
            )
            if not items:
                return
            names: List[str] = [it.market_hash_name for it in items if it.market_hash_name]
        finally:
            sess.close()

        if not names:
            with self._lock:
                self.last_processed_range = (start_id, end_id)
                self.completed_count = end_id
                self.current_start_id = end_id + 1
            return

        # 调用批量接口
        resp = self.client.get_price_batch(names)

        # 写入数据库
        sess2 = self.get_session()
        try:
            data_list: List[Dict[str, Any]] = []
            if isinstance(resp, dict):
                if isinstance(resp.get("data"), list):
                    data_list = resp.get("data")
                elif isinstance(resp.get("items"), list):
                    data_list = resp.get("items")
                elif isinstance(resp.get("results"), list):
                    data_list = resp.get("results")
            elif isinstance(resp, list):
                data_list = resp

            for it in (data_list or []):
                mhn = (it.get("marketHashName") or it.get("market_hash_name") or "").strip()
                if not mhn:
                    continue
                plats = it.get("platforms") or it.get("platformList") or it.get("dataList") or []
                if isinstance(plats, dict):
                    plats = [plats]
                # item 映射（若存在）
                item_rec = sess2.query(Item).filter(Item.market_hash_name == mhn).one_or_none()
                item_id_val = item_rec.id if item_rec else None
                for p in plats:
                    plat_name_raw = (p.get("platform") or p.get("name") or p.get("plat") or "")
                    plat_name = canonical_platform_name(plat_name_raw)
                    pid = (p.get("itemId") or p.get("platformItemId") or p.get("platform_item_id") or None)
                    sell = _to_float(p.get("sell_price") or p.get("sellPrice") or p.get("sell") or p.get("price"))
                    buy = _to_float(p.get("bidding_price") or p.get("biddingPrice") or p.get("buy") or p.get("buy_price"))
                    sell_count = _to_int(p.get("sell_count") or p.get("sellCount"))
                    bidding_count = _to_int(p.get("bidding_count") or p.get("biddingCount"))
                    ut = p.get("update_time") or p.get("updateTime")
                    ut_int = _to_int(ut) if ut is not None else None
                    # 统一为毫秒时间戳：若为秒（10位）则乘以 1000
                    if ut_int is not None and ut_int < 1000000000000:
                        ut_int = ut_int * 1000

                    plat_id_val = None
                    if item_id_val:
                        plat_rec = (
                            sess2.query(Platform)
                            .filter(Platform.item_id == item_id_val, Platform.name == plat_name)
                            .one_or_none()
                        )
                        plat_id_val = plat_rec.id if plat_rec else None

                    row = Price(
                        market_hash_name=mhn,
                        platform=plat_name,
                        platform_item_id=str(pid) if pid is not None else None,
                        item_id=item_id_val,
                        platform_id=plat_id_val,
                        sell_price=sell,
                        bidding_price=buy,
                        sell_count=sell_count,
                        bidding_count=bidding_count,
                        update_time=ut_int,
                        update_time_text=_format_beijing_text(ut_int),
                    )
                    sess2.add(row)
            sess2.commit()
        except Exception:
            sess2.rollback()
            raise
        finally:
            sess2.close()

        with self._lock:
            self.last_processed_range = (start_id, end_id)
            self.completed_count = end_id
            self.current_start_id = end_id + 1

    def status(self) -> Dict[str, Any]:
        with self._lock:
            percent = 0
            if self.max_id > 0:
                percent = int((self.completed_count / self.max_id) * 100)
            next_sec = None
            if self.next_run_ts:
                next_sec = max(0, int(self.next_run_ts - time.time()))
            return {
                "running": self.running,
                "paused": self.paused,
                "state": ("paused" if self.paused else ("running" if self.running else "idle")),
                "maxId": self.max_id,
                "completedCount": self.completed_count,
                "percent": percent,
                "currentStartId": self.current_start_id,
                "currentEndIdNext": min(self.max_id, self.current_start_id + self.batch_size - 1) if self.current_start_id > 0 else 0,
                "lastProcessedRange": self.last_processed_range,
                "nextRunSeconds": next_sec,
                "intervalSec": self.interval_sec,
                "batchSize": self.batch_size,
            }


class DualApiSequentialJob:
    """双 API 顺序交替批量抓取任务：API1 与 API2 交替执行，每轮固定间隔。"""

    def __init__(self, client1, client2, get_session, batch_size: int = 100, interval_sec: int = 30):
        self.client1 = client1
        self.client2 = client2
        self.get_session = get_session
        self.default_batch_size = max(1, int(batch_size))
        self.default_interval = max(1, int(interval_sec))

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._lock = threading.Lock()

        # 状态字段
        self.running: bool = False
        self.paused: bool = False
        self.max_id: int = 0
        self.completed_count: int = 0
        self.current_start_id: int = 0
        self.batch_size: int = self.default_batch_size
        self.interval_sec: int = self.default_interval
        self.last_processed_range: Optional[Tuple[int, int]] = None
        self.next_run_ts: Optional[float] = None
        self.next_client_id: int = 1  # 1 或 2
        self.last_error: Optional[str] = None

    # 公开控制方法
    def start(self, start_id: Optional[int] = None, batch_size: Optional[int] = None, interval_sec: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            if self.running:
                return self.status()
            # 检查两把 key
            if not (self.client1 and getattr(self.client1, "api_key", None)):
                return {"running": False, "paused": False, "error": "未配置 STEAMDT_API_KEY_1"}
            if not (self.client2 and getattr(self.client2, "api_key", None)):
                return {"running": False, "paused": False, "error": "未配置 STEAMDT_API_KEY_2"}

            self._stop_event.clear()
            self._pause_event.clear()
            self.paused = False
            self.running = True
            self.batch_size = max(1, int(batch_size or self.default_batch_size))
            self.interval_sec = max(1, int(interval_sec or self.default_interval))

            # 计算最大ID
            sess = self.get_session()
            try:
                self.max_id = int(sess.query(func.max(Item.id)).scalar() or 0)
            finally:
                sess.close()

            self.current_start_id = int(start_id or 1)
            self.completed_count = max(0, self.current_start_id - 1)
            self.last_processed_range = None
            self.next_run_ts = None
            self.next_client_id = 1
            self.last_error = None

            self._thread = threading.Thread(target=self._loop, name="DualApiSequentialJob", daemon=True)
            self._thread.start()
        return self.status()

    def pause(self) -> Dict[str, Any]:
        with self._lock:
            if self.running and not self.paused:
                self.paused = True
                self._pause_event.set()
        return self.status()

    def resume(self) -> Dict[str, Any]:
        with self._lock:
            if self.running and self.paused:
                self.paused = False
                self._pause_event.clear()
        return self.status()

    def stop(self) -> Dict[str, Any]:
        with self._lock:
            if self.running:
                self._stop_event.set()
        # 等待线程退出
        t = None
        with self._lock:
            t = self._thread
        if t and t.is_alive():
            try:
                t.join(timeout=2.0)
            except Exception:
                pass
        with self._lock:
            self.running = False
            self.paused = False
            self._thread = None
            self.next_run_ts = None
        return self.status()

    # 主循环
    def _loop(self):
        while not self._stop_event.is_set():
            # 暂停等待
            while self.paused and not self._stop_event.is_set():
                time.sleep(0.2)

            if self._stop_event.is_set():
                break

            # 执行一次区间（按当前客户端）
            try:
                self._run_one_range()
                self.last_error = None
            except Exception as e:
                # 记录错误但继续下一轮
                self.last_error = str(e)

            # 完成后检查是否结束
            with self._lock:
                done = self.completed_count >= self.max_id
                self.next_run_ts = time.time() + self.interval_sec
            if done:
                # 自动停止
                self.stop()
                break

            # 间隔等待
            remain = self.interval_sec
            while remain > 0 and not self._stop_event.is_set():
                if self.paused:
                    break
                time.sleep(0.2)
                remain -= 0.2

    def _run_one_range(self):
        with self._lock:
            start_id = self.current_start_id
            end_id = min(self.max_id, start_id + self.batch_size - 1)
            client_id = self.next_client_id

        if start_id > end_id or start_id <= 0:
            return

        # 读取该区间的名称
        sess = self.get_session()
        try:
            items = (
                sess.query(Item)
                .filter(Item.id >= start_id, Item.id <= end_id)
                .order_by(Item.id.asc())
                .all()
            )
            if not items:
                # 更新游标并返回
                with self._lock:
                    self.last_processed_range = (start_id, end_id)
                    self.completed_count = end_id
                    self.current_start_id = end_id + 1
                return
            names: List[str] = [it.market_hash_name for it in items if it.market_hash_name]
        finally:
            sess.close()

        if not names:
            with self._lock:
                self.last_processed_range = (start_id, end_id)
                self.completed_count = end_id
                self.current_start_id = end_id + 1
            return

        # 选择客户端并请求
        cli = self.client1 if client_id == 1 else self.client2
        resp = cli.get_price_batch(names)

        # 写入数据库（与 PriceBatchJob 保持一致逻辑）
        sess2 = self.get_session()
        try:
            data_list: List[Dict[str, Any]] = []
            if isinstance(resp, dict):
                if isinstance(resp.get("data"), list):
                    data_list = resp.get("data")
                elif isinstance(resp.get("items"), list):
                    data_list = resp.get("items")
                elif isinstance(resp.get("results"), list):
                    data_list = resp.get("results")
            elif isinstance(resp, list):
                data_list = resp

            for it in (data_list or []):
                mhn = (it.get("marketHashName") or it.get("market_hash_name") or "").strip()
                if not mhn:
                    continue
                plats = it.get("platforms") or it.get("platformList") or it.get("dataList") or []
                if isinstance(plats, dict):
                    plats = [plats]
                # item 映射（若存在）
                item_rec = sess2.query(Item).filter(Item.market_hash_name == mhn).one_or_none()
                item_id_val = item_rec.id if item_rec else None
                for p in plats:
                    plat_name_raw = (p.get("platform") or p.get("name") or p.get("plat") or "")
                    plat_name = canonical_platform_name(plat_name_raw)
                    pid = (p.get("itemId") or p.get("platformItemId") or p.get("platform_item_id") or None)
                    sell = _to_float(p.get("sell_price") or p.get("sellPrice") or p.get("sell") or p.get("price"))
                    buy = _to_float(p.get("bidding_price") or p.get("biddingPrice") or p.get("buy") or p.get("buy_price"))
                    sell_count = _to_int(p.get("sell_count") or p.get("sellCount"))
                    bidding_count = _to_int(p.get("bidding_count") or p.get("biddingCount"))
                    ut = p.get("update_time") or p.get("updateTime")
                    ut_int = _to_int(ut) if ut is not None else None
                    if ut_int is not None and ut_int < 1000000000000:
                        ut_int = ut_int * 1000

                    plat_id_val = None
                    if item_id_val:
                        plat_rec = (
                            sess2.query(Platform)
                            .filter(Platform.item_id == item_id_val, Platform.name == plat_name)
                            .one_or_none()
                        )
                        plat_id_val = plat_rec.id if plat_rec else None

                    row = Price(
                        market_hash_name=mhn,
                        platform=plat_name,
                        platform_item_id=str(pid) if pid is not None else None,
                        item_id=item_id_val,
                        platform_id=plat_id_val,
                        sell_price=sell,
                        bidding_price=buy,
                        sell_count=sell_count,
                        bidding_count=bidding_count,
                        update_time=ut_int,
                        update_time_text=_format_beijing_text(ut_int),
                    )
                    sess2.add(row)
            sess2.commit()
        except Exception:
            sess2.rollback()
            raise
        finally:
            sess2.close()

        # 更新状态：交替客户端与游标推进
        with self._lock:
            self.last_processed_range = (start_id, end_id)
            self.completed_count = end_id
            self.current_start_id = end_id + 1
            self.next_client_id = 2 if client_id == 1 else 1

    def status(self) -> Dict[str, Any]:
        with self._lock:
            percent = 0
            if self.max_id > 0:
                percent = int((self.completed_count / self.max_id) * 100)
            next_sec = None
            if self.next_run_ts:
                next_sec = max(0, int(self.next_run_ts - time.time()))
            return {
                "running": self.running,
                "paused": self.paused,
                "state": ("paused" if self.paused else ("running" if self.running else "idle")),
                "maxId": self.max_id,
                "completedCount": self.completed_count,
                "percent": percent,
                "currentStartId": self.current_start_id,
                "currentEndIdNext": min(self.max_id, self.current_start_id + self.batch_size - 1) if self.current_start_id > 0 else 0,
                "lastProcessedRange": self.last_processed_range,
                "nextRunSeconds": next_sec,
                "intervalSec": self.interval_sec,
                "batchSize": self.batch_size,
                "nextClientId": self.next_client_id,
                "alternating": True,
                "lastError": self.last_error,
            }