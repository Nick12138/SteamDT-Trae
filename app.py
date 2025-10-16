import os
import json
from datetime import datetime, timedelta, timezone
import threading
import time
from flask import Flask, render_template, request, jsonify, send_file
from pathlib import Path
from dotenv import load_dotenv

from steamdt_client import SteamDTClient
from job_bp import create_job_blueprint, create_dual_job_blueprint
from sqlalchemy import func, or_, and_
from sqlalchemy.orm import joinedload
from db import SessionLocal, init_db, Item, Platform, Price

# 加载 .env 环境变量
load_dotenv()


def create_app():
    app = Flask(__name__)

    # 配置与客户端
    api_key = os.getenv("STEAMDT_API_KEY")
    client = SteamDTClient(api_key=api_key)
    # 测试页面用：两把不同的 API key（优先使用 _1/_2，其次 A/B）
    api_key_1 = os.getenv("STEAMDT_API_KEY_1") or os.getenv("STEAMDT_API_KEY_A")
    api_key_2 = os.getenv("STEAMDT_API_KEY_2") or os.getenv("STEAMDT_API_KEY_B")
    client1 = SteamDTClient(api_key=api_key_1) if api_key_1 else None
    client2 = SteamDTClient(api_key=api_key_2) if api_key_2 else None

    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    base_info_path = data_dir / "base.json"

    # 初始化数据库
    init_db()

    def _format_beijing_text(ms: int | None) -> str | None:
        try:
            if ms is None:
                return None
            dt = datetime.fromtimestamp(ms / 1000, tz=timezone(timedelta(hours=8)))
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return None

    def get_session():
        return SessionLocal()

    @app.route("/")
    def index():
        return render_template("index.html")
    # 测试双 API 页面
    @app.route("/test/dual-api")
    def test_dual_api_page():
        return render_template("test_dual_api.html")
    # 注册定时任务控制蓝图
    app.register_blueprint(create_job_blueprint(client, get_session))
    # 注册双 API 顺序交替任务蓝图
    app.register_blueprint(create_dual_job_blueprint(client1, client2, get_session))

    # 获取 Steam 饰品基础信息并入库（同时保留本地 JSON）
    @app.route("/api/base/fetch", methods=["POST"])
    def fetch_base_info():
        try:
            data = client.get_base_info()
            # 保存到本地文件（便于可视检查与备份）
            with base_info_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            # 写入数据库
            payload_list = data.get("data") if isinstance(data, dict) else None
            items = payload_list if isinstance(payload_list, list) else []

            sess = get_session()
            upsert_count = 0
            try:
                for it in items:
                    mhn = (it.get("marketHashName") or "").strip()
                    name = (it.get("name") or "").strip()
                    if not mhn:
                        continue
                    obj = sess.query(Item).filter(Item.market_hash_name == mhn).one_or_none()
                    if obj is None:
                        obj = Item(market_hash_name=mhn, name=name)
                        sess.add(obj)
                        upsert_count += 1
                    else:
                        # 更新名称（如有变化）
                        obj.name = name or obj.name
                    # 平台信息 upsert
                    plats = it.get("platformList") or []
                    for p in plats:
                        pname = (p.get("name") or "").strip()
                        pid = (p.get("itemId") or "").strip()
                        if not pname:
                            continue
                        existing = (
                            sess.query(Platform)
                            .filter(Platform.item_id == obj.id, Platform.name == pname)
                            .one_or_none()
                        )
                        if existing is None:
                            sess.add(Platform(item_id=obj.id, name=pname, platform_item_id=pid))
                        else:
                            existing.platform_item_id = pid or existing.platform_item_id
                sess.commit()
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()

            return jsonify({
                "success": True,
                "saved": str(base_info_path),
                "count": len(items),
                "upserted": upsert_count
            })
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 从本地 JSON 导入价格（覆盖旧数据）
    @app.route("/api/admin/price/import_payload", methods=["POST"])
    def admin_price_import_payload():
        try:
            payload = request.get_json(silent=True, force=True) or {}
            items = []
            if isinstance(payload, list):
                items = payload
            elif isinstance(payload, dict):
                # 支持根对象为聚合结构，例如 { responses: [...] }
                responses = payload.get("responses")
                if isinstance(responses, list) and responses:
                    for resp in responses:
                        if not isinstance(resp, dict):
                            continue
                        for key in ("data", "items", "results"):
                            val = resp.get(key)
                            if isinstance(val, list):
                                items.extend(val)
                                break
                # 兼容常见根键
                if not items:
                    for key in ("data", "items", "results"):
                        val = payload.get(key)
                        if isinstance(val, list):
                            items = val
                            break
                # 如果是单条目结构
                if not items and (payload.get("marketHashName") or payload.get("platforms") or payload.get("platformList") or payload.get("dataList")):
                    items = [payload]
            if not items:
                return jsonify({"success": False, "error": "payload为空或格式不正确"}), 400

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

            overwritten = 0
            inserted = 0
            skipped = 0
            items_processed = 0
            platforms_processed = 0

            sess = get_session()
            try:
                for it in items:
                    mhn = (it.get("marketHashName") or it.get("market_hash_name") or "").strip()
                    if not mhn:
                        skipped += 1
                        continue
                    items_processed += 1

                    item_rec = sess.query(Item).filter(Item.market_hash_name == mhn).one_or_none()
                    item_id_val = item_rec.id if item_rec else None

                    # 支持多种平台列表键
                    plats = it.get("platforms") or it.get("platformList") or it.get("prices") or it.get("dataList") or []
                    if isinstance(plats, dict):
                        plats = [plats]

                    plat_map = {}
                    if item_id_val is not None:
                        existing_plats = sess.query(Platform).filter(Platform.item_id == item_id_val).all()
                        plat_map = {p.name: p for p in existing_plats}

                    for p in plats:
                        p_name_raw = (p.get("platform") or p.get("name") or p.get("plat") or "").strip()
                        if not p_name_raw:
                            skipped += 1
                            continue
                        platforms_processed += 1
                        canon = canonical_platform_name(p_name_raw)

                        # 兼容不同命名的 平台条目ID
                        plat_item_id = p.get("itemId") or p.get("platformItemId") or p.get("platform_item_id")
                        sell_price = _to_float(p.get("sellPrice") if p.get("sellPrice") is not None else p.get("sell_price"))
                        bidding_price = _to_float(p.get("biddingPrice") if p.get("biddingPrice") is not None else p.get("bidding_price"))
                        sell_count = _to_int(p.get("sellCount") if p.get("sellCount") is not None else p.get("sell_count"))
                        bidding_count = _to_int(p.get("biddingCount") if p.get("biddingCount") is not None else p.get("bidding_count"))
                        # 统一为毫秒时间戳：兼容 updateTime / update_time
                        _ut_raw = p.get("updateTime") if p.get("updateTime") is not None else p.get("update_time")
                        update_time = _to_int(_ut_raw) if _ut_raw is not None else None
                        if update_time is not None and update_time < 1000000000000:
                            update_time = update_time * 1000

                        platform_id_val = None
                        if item_id_val is not None:
                            plat_rec = plat_map.get(canon)
                            if not plat_rec and plat_item_id:
                                plat_rec = Platform(item_id=item_id_val, name=canon, platform_item_id=str(plat_item_id))
                                sess.add(plat_rec)
                                sess.flush()
                                plat_map[canon] = plat_rec
                            platform_id_val = plat_rec.id if plat_rec else None

                        if item_id_val is not None and platform_id_val is not None:
                            cnt = sess.query(Price).filter(Price.item_id == item_id_val, Price.platform_id == platform_id_val).delete(synchronize_session=False)
                            overwritten += cnt
                        else:
                            cnt = sess.query(Price).filter(Price.market_hash_name == mhn, Price.platform == canon).delete(synchronize_session=False)
                            overwritten += cnt

                        new_row = Price(
                            market_hash_name=mhn,
                            platform=canon,
                            platform_item_id=str(plat_item_id) if plat_item_id is not None else None,
                            item_id=item_id_val,
                            platform_id=platform_id_val,
                            sell_price=sell_price,
                            bidding_price=bidding_price,
                            sell_count=sell_count,
                            bidding_count=bidding_count,
                            update_time=update_time,
                            update_time_text=_format_beijing_text(update_time),
                        )
                        sess.add(new_row)
                        inserted += 1

                sess.commit()
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()

            return jsonify({
                "success": True,
                "message": "价格导入完成",
                "itemsProcessed": items_processed,
                "platformsProcessed": platforms_processed,
                "overwritten": overwritten,
                "inserted": inserted,
                "skipped": skipped,
            })
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 测试接口：按固定 ID 范围调用不同 API（不入库，仅返回原始数据）
    @app.route("/api/test/dual/fetch", methods=["POST"])
    def test_dual_fetch():
        try:
            payload = request.get_json(silent=True) or {}
            client_id = int(payload.get("clientId") or 0)
            if client_id not in (1, 2):
                return jsonify({"success": False, "error": "clientId 需为 1 或 2"}), 400

            # 固定区间
            if client_id == 1:
                start_id, end_id = 1, 100
            else:
                start_id, end_id = 101, 200

            # 选择客户端
            cli = client1 if client_id == 1 else client2
            if cli is None or not cli.api_key:
                return jsonify({"success": False, "error": f"未配置 STEAMDT_API_KEY_{client_id}"}), 400

            # 从数据库取对应范围的名称
            sess = get_session()
            try:
                items = (
                    sess.query(Item)
                    .filter(Item.id >= start_id, Item.id <= end_id)
                    .order_by(Item.id.asc())
                    .all()
                )
                names = [it.market_hash_name for it in items if it.market_hash_name]
            finally:
                sess.close()

            if not names:
                return jsonify({"success": False, "error": "指定ID范围无有效条目（请先刷新基础信息）"}), 400

            # 调用批量接口（不落库）
            data = cli.get_price_batch(names)
            return jsonify({
                "success": True,
                "clientId": client_id,
                "range": [start_id, end_id],
                "count": len(names),
                "data": data,
            })
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500
    # 从本地 base.json 导入到数据库（存在则跳过，不更新）
    @app.route("/api/base/import_local", methods=["POST"])
    def import_base_from_local():
        try:
            if not base_info_path.exists():
                return jsonify({"success": False, "error": "本地基础信息文件不存在，请先刷新并下载。"}), 404
            with base_info_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            data_obj = payload.get("data") if isinstance(payload, dict) else None
            items = data_obj if isinstance(data_obj, list) else []

            sess = get_session()
            inserted_items = 0
            skipped_items = 0
            inserted_platforms = 0
            try:
                for it in items:
                    mhn = (it.get("marketHashName") or "").strip()
                    name = (it.get("name") or "").strip()
                    if not mhn:
                        continue
                    exists = sess.query(Item).filter(Item.market_hash_name == mhn).one_or_none()
                    if exists is not None:
                        skipped_items += 1
                        # 跳过已存在的条目，不更新名称或平台
                        continue
                    obj = Item(market_hash_name=mhn, name=name)
                    sess.add(obj)
                    sess.flush()  # 获取 obj.id
                    inserted_items += 1
                    for p in (it.get("platformList") or []):
                        pname = (p.get("name") or "").strip()
                        pid = (p.get("itemId") or "").strip()
                        if not pname:
                            continue
                        sess.add(Platform(item_id=obj.id, name=pname, platform_item_id=pid))
                        inserted_platforms += 1
                sess.commit()
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()

            return jsonify({
                "success": True,
                "inserted": inserted_items,
                "skipped": skipped_items,
                "platforms_inserted": inserted_platforms
            })
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 通过上传的 JSON 负载导入到数据库（文件选择后前端读取并上传）
    @app.route("/api/base/import_payload", methods=["POST"])
    def import_base_from_payload():
        try:
            # 尝试从 JSON body 读取
            payload = request.get_json(silent=True, force=True)
            # 若走 multipart/form-data，则尝试从文件读取
            if payload is None and "file" in request.files:
                try:
                    file = request.files["file"]
                    payload = json.load(file.stream)
                except Exception:
                    payload = None
            if payload is None:
                return jsonify({"success": False, "error": "缺少有效的 JSON 内容"}), 400

            data_obj = payload.get("data") if isinstance(payload, dict) else (payload if isinstance(payload, list) else None)
            items = data_obj if isinstance(data_obj, list) else []
            if not isinstance(items, list):
                return jsonify({"success": False, "error": "JSON 格式不正确，应包含 data 数组或为数组"}), 400

            sess = get_session()
            inserted_items = 0
            skipped_items = 0
            inserted_platforms = 0
            try:
                for it in items:
                    mhn = (it.get("marketHashName") or "").strip()
                    name = (it.get("name") or "").strip()
                    if not mhn:
                        continue
                    exists = sess.query(Item).filter(Item.market_hash_name == mhn).one_or_none()
                    if exists is not None:
                        skipped_items += 1
                        continue
                    obj = Item(market_hash_name=mhn, name=name)
                    sess.add(obj)
                    sess.flush()
                    inserted_items += 1
                    for p in (it.get("platformList") or []):
                        pname = (p.get("name") or "").strip()
                        pid = (p.get("itemId") or "").strip()
                        if not pname:
                            continue
                        sess.add(Platform(item_id=obj.id, name=pname, platform_item_id=pid))
                        inserted_platforms += 1
                sess.commit()
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()

            return jsonify({
                "success": True,
                "inserted": inserted_items,
                "skipped": skipped_items,
                "platforms_inserted": inserted_platforms
            })
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 从数据库读取基础信息并筛选
    # 平台别名映射：统一筛选逻辑
    PLATFORM_ALIASES = {
        "BUFF": ["BUFF"],
        "C5": ["C5", "C5GAME"],
        "C5GAME": ["C5GAME", "C5"],
        "YOUPIN": ["YOUPIN", "YOUPIN898"],
        "HALO": ["HALO", "HALOSKINS"],
        "HALOSKINS": ["HALOSKINS", "HALO"],
        "STEAM": ["STEAM"],
        "WAXPEER": ["WAXPEER"],
        "SKINPORT": ["SKINPORT"],
        "DMARKET": ["DMARKET"],
    }

    def normalize_platform_filter(name: str):
        p = (name or "").strip().upper()
        if not p:
            return []
        return [x.upper() for x in PLATFORM_ALIASES.get(p, [p])]

    def canonical_platform_name(name: str):
        p = (name or "").strip().upper()
        if p == "C5":
            return "C5GAME"
        if p == "HALO":
            return "HALOSKINS"
        return p

    @app.route("/api/base", methods=["GET"])
    def get_base_info():
        q = request.args.get("q", "").strip()
        raw_platform = request.args.get("platform", "").strip()
        aliases = normalize_platform_filter(raw_platform)
        try:
            sess = get_session()
            try:
                query = sess.query(Item)
                if q:
                    like = f"%{q}%"
                    query = query.filter((Item.name.ilike(like)) | (Item.market_hash_name.ilike(like)))
                if aliases:
                    query = query.join(Platform, Item.id == Platform.item_id).filter(func.upper(Platform.name).in_(aliases))
                results = query.options(joinedload(Item.platforms)).all()
                data = [r.to_dict() for r in results]
                return jsonify({"success": True, "data": data, "count": len(data)})
            finally:
                sess.close()
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 导出 CSV（从数据库按当前筛选）
    @app.route("/api/base/export/csv", methods=["GET"]) 
    def export_base_csv():
        q = request.args.get("q", "").strip()
        raw_platform = request.args.get("platform", "").strip()
        aliases = normalize_platform_filter(raw_platform)
        try:
            sess = get_session()
            try:
                query = sess.query(Item)
                if q:
                    like = f"%{q}%"
                    query = query.filter((Item.name.ilike(like)) | (Item.market_hash_name.ilike(like)))
                if aliases:
                    query = query.join(Platform, Item.id == Platform.item_id).filter(func.upper(Platform.name).in_(aliases))
                results = query.options(joinedload(Item.platforms)).all()
                filtered = [r.to_dict() for r in results]
            finally:
                sess.close()

            # 生成 CSV
            from io import StringIO
            import csv
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(["name", "marketHashName", "platform", "itemId"])
            for item in filtered:
                plats = item.get("platformList") or []
                if not plats:
                    writer.writerow([item.get("name"), item.get("marketHashName"), "", ""])
                else:
                    for p in plats:
                        writer.writerow([item.get("name"), item.get("marketHashName"), p.get("name"), p.get("itemId")])

            # 保存到临时文件
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = data_dir / f"base_export_{ts}.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as f:
                f.write(output.getvalue())
            return send_file(str(csv_path), mimetype="text/csv", as_attachment=True, download_name=csv_path.name)
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 导出 JSON（从数据库按当前筛选）
    @app.route("/api/base/export/json", methods=["GET"]) 
    def export_base_json():
        q = request.args.get("q", "").strip()
        raw_platform = request.args.get("platform", "").strip()
        aliases = normalize_platform_filter(raw_platform)
        try:
            sess = get_session()
            try:
                query = sess.query(Item)
                if q:
                    like = f"%{q}%"
                    query = query.filter((Item.name.ilike(like)) | (Item.market_hash_name.ilike(like)))
                if aliases:
                    query = query.join(Platform, Item.id == Platform.item_id).filter(func.upper(Platform.name).in_(aliases))
                results = query.options(joinedload(Item.platforms)).all()
                filtered = [r.to_dict() for r in results]
            finally:
                sess.close()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_path = data_dir / f"base_export_{ts}.json"
            with json_path.open("w", encoding="utf-8") as f:
                json.dump(filtered, f, ensure_ascii=False, indent=2)
            return send_file(str(json_path), mimetype="application/json", as_attachment=True, download_name=json_path.name)
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 数据库管理页面
    @app.route("/admin/db")
    def admin_db_page():
        return render_template("admin_db.html")

    # 管理接口：列表查询
    @app.route("/api/admin/items", methods=["GET"])
    def admin_list_items():
        q = request.args.get("q", "").strip()
        raw_platform = request.args.get("platform", "").strip()
        aliases = normalize_platform_filter(raw_platform)
        limit = max(1, min(int(request.args.get("limit", 50)), 200))
        offset = max(0, int(request.args.get("offset", 0)))
        sort_by = (request.args.get("sortBy", "") or "").strip().lower()
        order = (request.args.get("order", "asc") or "").strip().lower()
        # 规范 sortBy 值
        if sort_by in ("item_id", "id"):  
            sort_by = "id"
        elif sort_by in ("minprice", "min_price", "minsellprice", "price"):
            sort_by = "min_price"
        else:
            sort_by = "default"
        try:
            sess = get_session()
            try:
                query = sess.query(Item)
                if q:
                    like = f"%{q}%"
                    query = query.filter((Item.name.ilike(like)) | (Item.market_hash_name.ilike(like)))
                if aliases:
                    query = query.join(Platform, Item.id == Platform.item_id).filter(func.upper(Platform.name).in_(aliases))
                # 统计总数（在排序与联接之前计算）
                total = query.count()

                # 排序逻辑
                if sort_by == "id":
                    if order == "desc":
                        query = query.order_by(Item.id.desc())
                    else:
                        query = query.order_by(Item.id.asc())
                elif sort_by == "min_price":
                    # 依据有效最低售卖价排序（sell_price > 0 且非空）
                    min_subq = (
                        sess.query(
                            Price.item_id,
                            func.min(Price.sell_price).label("min_sell_price")
                        )
                        .filter(Price.sell_price != None, Price.sell_price > 0)
                        .group_by(Price.item_id)
                        .subquery()
                    )
                    query = query.outerjoin(min_subq, Item.id == min_subq.c.item_id)
                    # 使用 COALESCE 确保无价的条目排在最后
                    if order == "desc":
                        sort_expr = func.coalesce(min_subq.c.min_sell_price, -1.0)
                        query = query.order_by(sort_expr.desc(), Item.id.asc())
                    else:
                        sort_expr = func.coalesce(min_subq.c.min_sell_price, 999999999.0)
                        query = query.order_by(sort_expr.asc(), Item.id.asc())
                else:
                    # 默认按 marketHashName 升序
                    query = query.order_by(Item.market_hash_name.asc())

                results = (
                    query.options(joinedload(Item.platforms))
                    .offset(offset)
                    .limit(limit)
                    .all()
                )
                return jsonify({
                    "success": True,
                    "data": [r.to_dict() for r in results],
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                })
            finally:
                sess.close()
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 管理接口：创建新条目（可选带平台）
    @app.route("/api/admin/item", methods=["POST"])
    def admin_create_item():
        try:
            payload = request.get_json(silent=True, force=True) or {}
            mhn = (payload.get("marketHashName") or "").strip()
            name = (payload.get("name") or "").strip()
            plats = payload.get("platformList") or []
            if not mhn:
                return jsonify({"success": False, "error": "缺少 marketHashName"}), 400
            sess = get_session()
            try:
                exists = sess.query(Item).filter(Item.market_hash_name == mhn).one_or_none()
                if exists is not None:
                    return jsonify({"success": False, "error": "条目已存在"}), 409
                obj = Item(market_hash_name=mhn, name=name)
                sess.add(obj)
                sess.flush()
                created_platforms = 0
                for p in plats:
                    pname = canonical_platform_name((p.get("name") or "").strip())
                    pid = (p.get("itemId") or "").strip()
                    if not pname:
                        continue
                    sess.add(Platform(item_id=obj.id, name=pname, platform_item_id=pid))
                    created_platforms += 1
                sess.commit()
                return jsonify({"success": True, "item": obj.to_dict(), "platforms": created_platforms})
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 管理接口：规范化平台名称（将 C5->C5GAME、HALO->HALOSKINS，并合并重复）
    @app.route("/api/admin/platforms/normalize", methods=["POST"])
    def admin_normalize_platforms():
        try:
            sess = get_session()
            normalized = 0
            merged = 0
            try:
                items = sess.query(Item).options(joinedload(Item.platforms)).all()
                for it in items:
                    by_canon = {}
                    # 遍历平台并按规范名归并
                    for p in list(it.platforms):
                        cname = canonical_platform_name(p.name)
                        existing = by_canon.get(cname)
                        if existing is None:
                            # 将当前记录重命名为规范名
                            if (p.name or "").upper() != cname:
                                p.name = cname
                                normalized += 1
                            by_canon[cname] = p
                        else:
                            # 合并重复：保留已有记录，补充 itemId，删除当前重复记录
                            if (not existing.platform_item_id) and p.platform_item_id:
                                existing.platform_item_id = p.platform_item_id
                            sess.delete(p)
                            merged += 1
                sess.commit()
                return jsonify({"success": True, "normalized": normalized, "merged": merged})
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 管理接口：删除指定条目
    @app.route("/api/admin/item", methods=["DELETE"])
    def admin_delete_item():
        mhn = request.args.get("marketHashName", "").strip()
        if not mhn:
            return jsonify({"success": False, "error": "缺少参数 marketHashName"}), 400
        try:
            sess = get_session()
            try:
                obj = sess.query(Item).filter(Item.market_hash_name == mhn).one_or_none()
                if obj is None:
                    return jsonify({"success": False, "error": "条目不存在"}), 404
                sess.delete(obj)
                sess.commit()
                return jsonify({"success": True})
            finally:
                sess.close()
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 管理接口：清空数据库（谨慎）
    @app.route("/api/admin/clear", methods=["POST"])
    def admin_clear_db():
        try:
            sess = get_session()
            try:
                # 先删平台，再删条目
                sess.query(Platform).delete()
                sess.query(Item).delete()
                sess.commit()
                return jsonify({"success": True})
            finally:
                sess.close()
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 通过 marketHashName 查询单价（各平台）
    @app.route("/api/price/single", methods=["GET"]) 
    def price_single():
        name = request.args.get("marketHashName", "").strip()
        if not name:
            return jsonify({"success": False, "error": "缺少参数 marketHashName"}), 400
        try:
            data = client.get_price_single(name)
            return jsonify(data)
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 批量查询价格
    @app.route("/api/price/batch", methods=["POST"]) 
    def price_batch():
        try:
            payload = request.get_json(force=True)
            names = payload.get("marketHashNames") if isinstance(payload, dict) else None
            if not names or not isinstance(names, list):
                return jsonify({"success": False, "error": "请提供 marketHashNames 列表"}), 400
            data = client.get_price_batch(names)
            return jsonify(data)
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 近7天均价
    @app.route("/api/price/avg", methods=["GET"]) 
    def price_avg():
        name = request.args.get("marketHashName", "").strip()
        if not name:
            return jsonify({"success": False, "error": "缺少参数 marketHashName"}), 400
        try:
            data = client.get_price_avg(name)
            return jsonify(data)
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 管理页价格查询：从数据库读取各平台最新记录
    @app.route("/api/admin/price/single", methods=["GET"]) 
    def admin_price_single():
        name = request.args.get("marketHashName", "").strip()
        if not name:
            return jsonify({"success": False, "error": "缺少参数 marketHashName"}), 400
        try:
            sess = get_session()
            try:
                # 兼容：优先通过 item_id 读取，其次使用旧的 marketHashName 匹配
                item = sess.query(Item).filter(Item.market_hash_name == name).one_or_none()
                rows = (
                    sess.query(Price)
                    .filter(
                        or_(
                            Price.market_hash_name == name,
                            Price.item_id == (item.id if item else None)
                        )
                    )
                    .order_by(Price.update_time.desc(), Price.created_at.desc())
                    .all()
                )
                # 预取涉及的平台，避免 N+1
                pid_set = {r.platform_id for r in rows if r.platform_id is not None}
                plat_map = {}
                if pid_set:
                    plats = sess.query(Platform).filter(Platform.id.in_(list(pid_set))).all()
                    plat_map = {p.id: p for p in plats}

                def canonical_platform_name(name: str):
                    p = (name or "").strip().upper()
                    if p == "C5":
                        return "C5GAME"
                    if p == "HALO":
                        return "HALOSKINS"
                    return p

                latest_by_platform = {}
                for r in rows:
                    pkey = canonical_platform_name(r.platform) if r.platform else None
                    if not pkey and r.platform_id and r.platform_id in plat_map:
                        pkey = canonical_platform_name(plat_map[r.platform_id].name)
                    pkey = pkey or "UNKNOWN"
                    if pkey not in latest_by_platform:
                        latest_by_platform[pkey] = r
                platforms = []
                for p, r in latest_by_platform.items():
                    plat_item_id = r.platform_item_id
                    if not plat_item_id and r.platform_id and r.platform_id in plat_map:
                        plat_item_id = plat_map[r.platform_id].platform_item_id
                    platforms.append({
                        "platform": p,
                        "itemId": plat_item_id,
                        "sell_price": r.sell_price,
                        "bidding_price": r.bidding_price,
                        "sell_count": r.sell_count,
                        "bidding_count": r.bidding_count,
                        "update_time": r.update_time,
                        "update_time_text": r.update_time_text,
                        "created_at": r.created_at.isoformat() if r.created_at else None
                    })
                return jsonify({
                    "success": True,
                    "source": "db",
                    "marketHashName": name,
                    "count": len(platforms),
                    "platforms": platforms
                })
            finally:
                sess.close()
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 管理页均价查询：近7天数据库均值
    @app.route("/api/admin/price/avg", methods=["GET"]) 
    def admin_price_avg():
        name = request.args.get("marketHashName", "").strip()
        if not name:
            return jsonify({"success": False, "error": "缺少参数 marketHashName"}), 400
        try:
            now = datetime.utcnow()
            window_days = 7
            threshold_dt = now - timedelta(days=window_days)
            threshold_ms = int(threshold_dt.timestamp() * 1000)

            sess = get_session()
            try:
                item = sess.query(Item).filter(Item.market_hash_name == name).one_or_none()
                rows = (
                    sess.query(Price)
                    .filter(
                        or_(
                            Price.market_hash_name == name,
                            Price.item_id == (item.id if item else None)
                        )
                    )
                    .filter(
                        or_(
                            Price.update_time != None,
                            Price.created_at != None
                        )
                    )
                    .filter(
                        or_(
                            and_(Price.update_time != None, Price.update_time >= threshold_ms),
                            and_(Price.update_time == None, Price.created_at >= threshold_dt)
                        )
                    )
                    .all()
                )

                per_platform = {}
                # 预取涉及的平台
                pid_set = {r.platform_id for r in rows if r.platform_id is not None}
                plat_map = {}
                if pid_set:
                    plats = sess.query(Platform).filter(Platform.id.in_(list(pid_set))).all()
                    plat_map = {p.id: p for p in plats}

                def canonical_platform_name(name: str):
                    p = (name or "").strip().upper()
                    if p == "C5":
                        return "C5GAME"
                    if p == "HALO":
                        return "HALOSKINS"
                    return p

                for r in rows:
                    key = canonical_platform_name(r.platform) if r.platform else None
                    if not key and r.platform_id and r.platform_id in plat_map:
                        key = canonical_platform_name(plat_map[r.platform_id].name)
                    key = key or "UNKNOWN"
                    if key not in per_platform:
                        per_platform[key] = {"sell": [], "buy": []}
                    if r.sell_price is not None:
                        per_platform[key]["sell"].append(r.sell_price)
                    if r.bidding_price is not None:
                        per_platform[key]["buy"].append(r.bidding_price)

                def avg(values):
                    return (sum(values) / len(values)) if values else None

                platforms = []
                all_sell = []
                all_buy = []
                for p, vals in per_platform.items():
                    s_avg = avg(vals["sell"])
                    b_avg = avg(vals["buy"])
                    platforms.append({
                        "platform": p,
                        "sell_avg": s_avg,
                        "bidding_avg": b_avg,
                        "sell_samples": len(vals["sell"]),
                        "bidding_samples": len(vals["buy"])
                    })
                    all_sell.extend(vals["sell"])
                    all_buy.extend(vals["buy"])

                overall = {
                    "sell_avg": avg(all_sell),
                    "bidding_avg": avg(all_buy),
                    "sell_samples": len(all_sell),
                    "bidding_samples": len(all_buy)
                }

                return jsonify({
                    "success": True,
                    "source": "db",
                    "marketHashName": name,
                    "windowDays": window_days,
                    "platforms": platforms,
                    "overall": overall
                })
            finally:
                sess.close()
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # 批量按 ID 范围查询价格、写入数据库，并导出 JSON
    @app.route("/api/admin/price/batch_by_id", methods=["POST"])
    def admin_price_batch_by_id():
        try:
            payload = request.get_json(silent=True, force=True) or {}
            # 支持 idRange 形如 "1-100"，或显式 startId/endId
            id_range = (payload.get("idRange") or payload.get("range") or "").strip()
            start_id = payload.get("startId")
            end_id = payload.get("endId")

            if id_range and (start_id is None or end_id is None):
                import re
                m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", id_range)
                if not m:
                    return jsonify({"success": False, "error": "idRange 格式不正确，应为 例如 1-100"}), 400
                start_id = int(m.group(1))
                end_id = int(m.group(2))
            if start_id is None or end_id is None:
                return jsonify({"success": False, "error": "请提供 idRange 或 startId/endId"}), 400
            if start_id > end_id:
                start_id, end_id = end_id, start_id

            sess = get_session()
            try:
                items = (
                    sess.query(Item)
                    .filter(Item.id >= start_id, Item.id <= end_id)
                    .order_by(Item.id.asc())
                    .all()
                )
            finally:
                sess.close()

            if not items:
                return jsonify({"success": False, "error": "给定 ID 范围内没有条目"}), 404

            names = [(it.market_hash_name or "").strip() for it in items if (it.market_hash_name or "").strip()]
            if not names:
                return jsonify({"success": False, "error": "条目缺少有效的 marketHashName"}), 400

            # 分批每 100 个调用批量接口
            def chunk(lst, size):
                for i in range(0, len(lst), size):
                    yield lst[i:i+size]

            all_responses = []
            inserted_count = 0
            now_dt = datetime.utcnow()

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

            sess = get_session()
            try:
                for chunk_names in chunk(names, 100):
                    resp = client.get_price_batch(chunk_names)
                    all_responses.append(resp)
                    # 解析并写入 Price
                    data_list = []
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
                        # 支持多种平台列表键：platforms/platformList/dataList
                        plats = it.get("platforms") or it.get("platformList") or it.get("dataList") or []
                        if isinstance(plats, dict):
                            plats = [plats]
                        for p in plats:
                            # 兼容不同的平台名键
                            plat_name_raw = (p.get("platform") or p.get("name") or p.get("plat") or "")
                            plat_name = canonical_platform_name(plat_name_raw)
                            # 兼容不同的平台条目ID键
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

                            # 填充 item_id 与 platform_id（兼容旧数据无平台映射的情况）
                            item_rec = sess.query(Item).filter(Item.market_hash_name == mhn).one_or_none()
                            item_id_val = item_rec.id if item_rec else None
                            plat_id_val = None
                            if item_id_val:
                                plat_rec = (
                                    sess.query(Platform)
                                    .filter(Platform.item_id == item_id_val, Platform.name == plat_name)
                                    .one_or_none()
                                )
                                plat_id_val = plat_rec.id if plat_rec else None

                            row = Price(
                                market_hash_name=mhn,
                                platform=plat_name,
                                platform_item_id=pid,
                                item_id=item_id_val,
                                platform_id=plat_id_val,
                                sell_price=sell,
                                bidding_price=buy,
                                sell_count=sell_count,
                                bidding_count=bidding_count,
                                update_time=ut_int,
                                update_time_text=_format_beijing_text(ut_int),
                            )
                            sess.add(row)
                            inserted_count += 1
                sess.commit()
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()

            # 导出合并 JSON 到 data/<start>-<end>.json
            out_name = f"{start_id}-{end_id}.json"
            out_path = data_dir / out_name
            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump({
                        "success": True,
                        "startId": start_id,
                        "endId": end_id,
                        "count": len(names),
                        "chunks": len(list(chunk(names, 100))),
                        "responses": all_responses,
                        "savedAt": now_dt.isoformat() + "Z",
                    }, f, ensure_ascii=False, indent=2)
            except Exception as e:
                return jsonify({"success": False, "error": f"导出失败: {str(e)}"}), 500

            return jsonify({
                "success": True,
                "startId": start_id,
                "endId": end_id,
                "itemCount": len(items),
                "processedNames": len(names),
                "insertedRows": inserted_count,
                "saved": str(out_path),
            })
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)