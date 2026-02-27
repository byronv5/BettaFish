"""
监控仪表板 Flask Blueprint
提供舆情监控数据 API 及爬虫任务管理接口
"""

import json
import threading
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
from urllib.parse import quote_plus

from flask import Blueprint, render_template, jsonify, request
from loguru import logger
from sqlalchemy import create_engine, text

monitoring_bp = Blueprint('monitoring', __name__, url_prefix='/monitoring')

# ===== 数据目录 =====
_BASE_DIR = Path(__file__).resolve().parent
_CONFIGS_FILE = _BASE_DIR / 'crawler_configs.json'
_COOKIES_FILE = _BASE_DIR / 'crawler_cookies.json'

# ===== 内存中的运行状态 =====
_running_tasks: dict = {}
_running_lock = threading.Lock()

# ===== 数据库引擎缓存 =====
_engine = None
_engine_lock = threading.Lock()


def _build_db_url():
    from config import settings
    dialect = (settings.DB_DIALECT or "postgresql").lower()
    if dialect in ("postgresql", "postgres"):
        return (
            f"postgresql+psycopg://{settings.DB_USER}:{quote_plus(str(settings.DB_PASSWORD))}"
            f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        )
    return (
        f"mysql+pymysql://{settings.DB_USER}:{quote_plus(str(settings.DB_PASSWORD))}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}?charset={settings.DB_CHARSET}"
    )


def _get_engine():
    global _engine
    with _engine_lock:
        if _engine is None:
            _engine = create_engine(_build_db_url(), pool_pre_ping=True, pool_size=3, max_overflow=5)
        return _engine


def _reset_engine():
    """重置引擎（配置变更后调用）"""
    global _engine
    with _engine_lock:
        if _engine:
            try:
                _engine.dispose()
            except Exception:
                pass
        _engine = None


# ===== 平台定义 =====
PLATFORMS = [
    {"code": "xhs",   "name": "小红书", "icon": "📕"},
    {"code": "dy",    "name": "抖音",   "icon": "🎵"},
    {"code": "ks",    "name": "快手",   "icon": "⚡"},
    {"code": "bili",  "name": "B站",    "icon": "📺"},
    {"code": "wb",    "name": "微博",   "icon": "🌐"},
    {"code": "tieba", "name": "贴吧",   "icon": "📋"},
    {"code": "zhihu", "name": "知乎",   "icon": "🔵"},
]

# 评论表配置: (表名, 时间字段, 是否毫秒时间戳, ip_location字段)
COMMENT_TABLES = [
    # 统一使用 add_ts（爬虫入库时间，13位毫秒时间戳）作为时间维度
    # create_time 是内容在平台上的原始发布时间，可能是历史数据，不反映爬取活跃度
    ("xhs_note_comment",       "add_ts", True,  "ip_location"),
    ("douyin_aweme_comment",   "add_ts", True,  "ip_location"),
    ("kuaishou_video_comment", "add_ts", True,  None),
    ("bilibili_video_comment", "add_ts", True,  None),
    ("weibo_note_comment",     "add_ts", True,  "ip_location"),
    ("tieba_comment",          "add_ts", True,  "ip_location"),
    ("zhihu_comment",          "add_ts", True,  "ip_location"),
]

# 评论表 → (父表, 评论表FK列, 父表PK列)，用于按 source_keyword 过滤
COMMENT_JOIN_MAP = {
    "xhs_note_comment":       ("xhs_note",      "note_id",    "note_id"),
    "douyin_aweme_comment":   ("douyin_aweme",   "aweme_id",   "aweme_id"),
    "kuaishou_video_comment": ("kuaishou_video", "video_id",   "video_id"),
    "bilibili_video_comment": ("bilibili_video", "video_id",   "video_id"),
    "weibo_note_comment":     ("weibo_note",     "note_id",    "note_id"),
    "tieba_comment":          ("tieba_note",     "note_id",    "note_id"),
    "zhihu_comment":          ("zhihu_content",  "content_id", "content_id"),
}


def _cst_day_range_ms(date_str: str):
    """将 YYYY-MM-DD 解析为 CST (UTC+8) 当天的毫秒时间戳范围 (start_ms, end_ms)，失败返回 None。"""
    try:
        from datetime import date as _date, timezone as _tz
        CST = _tz(timedelta(hours=8))
        d = _date.fromisoformat(date_str)
        start = int(datetime.combine(d, datetime.min.time()).replace(tzinfo=CST).timestamp()) * 1000
        return start, start + 86_400_000
    except Exception:
        return None


def _kw_exists(table: str, keyword: str | None):
    """返回 (exists_sql_fragment, extra_params) 用于按关键词过滤评论表。
    通过 EXISTS 子查询关联父表的 source_keyword 字段。
    """
    if not keyword:
        return "", {}
    info = COMMENT_JOIN_MAP.get(table)
    if not info:
        return "", {}
    parent_table, comment_fk, parent_pk = info
    sql = (
        f"AND EXISTS ("
        f"SELECT 1 FROM {parent_table} _pk "
        f"WHERE _pk.{parent_pk} = {table}.{comment_fk} AND _pk.source_keyword = :kw"
        f")"
    )
    return sql, {"kw": keyword}


# ===== MonitoringService =====
class MonitoringService:
    """舆情监控数据服务"""

    def get_latest_topic(self, keyword: str = None):
        """获取最新话题"""
        try:
            engine = _get_engine()
            with engine.connect() as conn:
                if keyword:
                    row = conn.execute(text(
                        """
                        SELECT topic_id, topic_name, topic_description, keywords, extract_date
                        FROM daily_topics
                        WHERE keywords LIKE :kw
                        ORDER BY extract_date DESC, id DESC
                        LIMIT 1
                        """
                    ), {"kw": f"%{keyword}%"}).fetchone()
                else:
                    row = conn.execute(text(
                        """
                        SELECT topic_id, topic_name, topic_description, keywords, extract_date
                        FROM daily_topics
                        ORDER BY extract_date DESC, id DESC
                        LIMIT 1
                        """
                    )).fetchone()

                if not row:
                    return {
                        "topic_id": None,
                        "topic_name": "暂无话题",
                        "topic_description": "数据库中尚无话题数据",
                        "keywords": [],
                        "extract_date": "",
                    }

                kw_list = []
                if row[3]:
                    try:
                        raw = row[3]
                        if isinstance(raw, str):
                            kw_list = json.loads(raw) if raw.startswith('[') else [k.strip() for k in raw.split(',') if k.strip()]
                        elif isinstance(raw, list):
                            kw_list = raw
                    except Exception:
                        kw_list = [k.strip() for k in str(row[3]).split(',') if k.strip()]

                return {
                    "topic_id": row[0],
                    "topic_name": row[1],
                    "topic_description": row[2] or "",
                    "keywords": kw_list,
                    "extract_date": str(row[4]) if row[4] else "",
                }
        except Exception as exc:
            logger.warning(f"get_latest_topic 失败: {exc}")
            _reset_engine()
            return {
                "topic_id": None,
                "topic_name": "数据库连接失败",
                "topic_description": str(exc),
                "keywords": [],
                "extract_date": "",
            }

    def get_trend_data(self, days: int = 6, keyword: str = None, date_str: str = None):
        """获取最近 N 天的每日评论数趋势（以北京时间 UTC+8 为基准）"""
        try:
            engine = _get_engine()
            from datetime import timezone, date as _date
            CST = timezone(timedelta(hours=8))
            if date_str:
                try:
                    today = _date.fromisoformat(date_str)
                except Exception:
                    today = datetime.now(CST).date()
            else:
                today = datetime.now(CST).date()
            start_date = today - timedelta(days=days - 1)
            date_range = [(start_date + timedelta(days=i)).isoformat() for i in range(days)]
            daily_counts: dict = defaultdict(int)

            with engine.connect() as conn:
                dialect_name = engine.dialect.name
                for table, ts_col, is_ms, _ in COMMENT_TABLES:
                    try:
                        start_ts = int(datetime.combine(start_date, datetime.min.time()).replace(tzinfo=CST).timestamp())
                        end_ts = int(datetime.combine(today + timedelta(days=1), datetime.min.time()).replace(tzinfo=CST).timestamp())
                        if is_ms:
                            start_ts *= 1000
                            end_ts *= 1000

                        kw_sql, kw_params = _kw_exists(table, keyword)
                        params = {"start_ts": start_ts, "end_ts": end_ts, **kw_params}

                        if dialect_name == "postgresql":
                            divisor = "::float / 1000" if is_ms else ""
                            sql = text(f"""
                                SELECT TO_CHAR(TO_TIMESTAMP({ts_col}{divisor}) AT TIME ZONE 'Asia/Shanghai', 'YYYY-MM-DD') AS d,
                                       COUNT(*) AS cnt
                                FROM {table}
                                WHERE {ts_col} >= :start_ts AND {ts_col} < :end_ts
                                {kw_sql}
                                GROUP BY d
                            """)
                        else:
                            expr = f"{ts_col} / 1000" if is_ms else ts_col
                            sql = text(f"""
                                SELECT DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME({expr}), '+00:00', '+08:00'), '%Y-%m-%d') AS d,
                                       COUNT(*) AS cnt
                                FROM {table}
                                WHERE {ts_col} >= :start_ts AND {ts_col} < :end_ts
                                {kw_sql}
                                GROUP BY d
                            """)

                        rows = conn.execute(sql, params).fetchall()
                        for row in rows:
                            daily_counts[str(row[0])] += int(row[1])
                    except Exception as e:
                        logger.debug(f"trend skip {table}: {e}")

            return {
                "dates": date_range,
                "comment_counts": [daily_counts.get(d, 0) for d in date_range],
            }
        except Exception as exc:
            logger.warning(f"get_trend_data 失败: {exc}")
            _reset_engine()
            return {"dates": [], "comment_counts": []}

    def get_sentiment_data(self, keyword: str = None, date_str: str = None, days: int = 6):
        """获取情感倾向分布（基于 ML 模型写入的 sentiment_label 字段聚合，与趋势图相同的日期范围）
        sentiment_label 约定：0=负面  1=中性  2=正面  NULL=未分析（历史数据，不计入）
        """
        try:
            engine = _get_engine()

            from datetime import timezone, date as _date
            CST = timezone(timedelta(hours=8))
            if date_str:
                try:
                    today = _date.fromisoformat(date_str)
                except Exception:
                    today = datetime.now(CST).date()
            else:
                today = datetime.now(CST).date()
            start_date = today - timedelta(days=days - 1)
            start_ts = int(datetime.combine(start_date, datetime.min.time()).replace(tzinfo=CST).timestamp()) * 1000
            end_ts = int(datetime.combine(today + timedelta(days=1), datetime.min.time()).replace(tzinfo=CST).timestamp()) * 1000
            date_where = "AND add_ts >= :ds AND add_ts < :de"
            date_params: dict = {"ds": start_ts, "de": end_ts}

            # label → count: 0=负面, 1=中性, 2=正面
            label_counts: dict = {0: 0, 1: 0, 2: 0}

            with engine.connect() as conn:
                for table, ts_col, _, _ in COMMENT_TABLES:
                    try:
                        kw_sql, kw_params = _kw_exists(table, keyword)
                        params = {**date_params, **kw_params}
                        rows = conn.execute(text(
                            f"SELECT sentiment_label, COUNT(*) FROM {table}"
                            f" WHERE sentiment_label IS NOT NULL {date_where} {kw_sql}"
                            f" GROUP BY sentiment_label"
                        ), params).fetchall()
                        for label, cnt in rows:
                            if label in label_counts:
                                label_counts[label] += cnt
                    except Exception as e:
                        logger.debug(f"sentiment skip {table}: {e}")

            return {
                "positive": label_counts[2],
                "neutral":  label_counts[1],
                "negative": label_counts[0],
            }

        except Exception as exc:
            logger.warning(f"get_sentiment_data 失败: {exc}")
            _reset_engine()
            return {"positive": 0, "neutral": 0, "negative": 0}

    def get_location_data(self, keyword: str = None, date_str: str = None, days: int = 6):
        """获取地域分布 TOP10（与趋势图相同的日期范围）"""
        try:
            engine = _get_engine()
            location_counts: dict = defaultdict(int)

            from datetime import timezone, date as _date
            CST = timezone(timedelta(hours=8))
            if date_str:
                try:
                    today = _date.fromisoformat(date_str)
                except Exception:
                    today = datetime.now(CST).date()
            else:
                today = datetime.now(CST).date()
            start_date = today - timedelta(days=days - 1)
            start_ts = int(datetime.combine(start_date, datetime.min.time()).replace(tzinfo=CST).timestamp()) * 1000
            end_ts = int(datetime.combine(today + timedelta(days=1), datetime.min.time()).replace(tzinfo=CST).timestamp()) * 1000
            date_where = "AND add_ts >= :ds AND add_ts < :de"
            date_params: dict = {"ds": start_ts, "de": end_ts}

            with engine.connect() as conn:
                for table, _, _, loc_col in COMMENT_TABLES:
                    if not loc_col:
                        continue
                    try:
                        kw_sql, kw_params = _kw_exists(table, keyword)
                        params = {**date_params, **kw_params}
                        rows = conn.execute(text(
                            f"SELECT {loc_col}, COUNT(*) AS cnt FROM {table} "
                            f"WHERE {loc_col} IS NOT NULL AND {loc_col} != '' {date_where} {kw_sql} "
                            f"GROUP BY {loc_col}"
                        ), params).fetchall()
                        for loc, cnt in rows:
                            loc_str = (str(loc) or "").strip()
                            loc_short = (loc_str.replace("省", "").replace("市", "")
                                         .replace("自治区", "").replace("壮族", "").strip())
                            if loc_short:
                                location_counts[loc_short] += int(cnt)
                    except Exception as e:
                        logger.debug(f"location skip {table}: {e}")

            top10 = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            return {loc: cnt for loc, cnt in top10}

        except Exception as exc:
            logger.warning(f"get_location_data 失败: {exc}")
            _reset_engine()
            return {}

    def get_speed_data(self, keyword: str = None, date_str: str = None):
        """获取 24 小时传播速度分布（以北京时间 UTC+8 为基准）"""
        try:
            engine = _get_engine()
            from datetime import timezone, date as _date
            CST = timezone(timedelta(hours=8))
            if date_str:
                try:
                    target_date = _date.fromisoformat(date_str)
                except Exception:
                    target_date = datetime.now(CST).date()
            else:
                target_date = datetime.now(CST).date()
            start_ts = int(datetime.combine(target_date, datetime.min.time()).replace(tzinfo=CST).timestamp())
            end_ts = start_ts + 86400
            hourly: dict = defaultdict(int)

            with engine.connect() as conn:
                dialect_name = engine.dialect.name
                for table, ts_col, is_ms, _ in COMMENT_TABLES:
                    try:
                        _start = start_ts * 1000 if is_ms else start_ts
                        _end = end_ts * 1000 if is_ms else end_ts

                        kw_sql, kw_params = _kw_exists(table, keyword)
                        params = {"start_ts": _start, "end_ts": _end, **kw_params}

                        if dialect_name == "postgresql":
                            divisor = "::float / 1000" if is_ms else ""
                            sql = text(f"""
                                SELECT EXTRACT(HOUR FROM TO_TIMESTAMP({ts_col}{divisor}) AT TIME ZONE 'Asia/Shanghai')::int AS h,
                                       COUNT(*) AS cnt
                                FROM {table}
                                WHERE {ts_col} >= :start_ts AND {ts_col} < :end_ts
                                {kw_sql}
                                GROUP BY h ORDER BY h
                            """)
                        else:
                            expr = f"{ts_col} / 1000" if is_ms else ts_col
                            sql = text(f"""
                                SELECT HOUR(CONVERT_TZ(FROM_UNIXTIME({expr}), '+00:00', '+08:00')) AS h, COUNT(*) AS cnt
                                FROM {table}
                                WHERE {ts_col} >= :start_ts AND {ts_col} < :end_ts
                                {kw_sql}
                                GROUP BY h ORDER BY h
                            """)

                        rows = conn.execute(sql, params).fetchall()
                        for h, cnt in rows:
                            hourly[int(h)] += int(cnt)
                    except Exception as e:
                        logger.debug(f"speed skip {table}: {e}")

            hours = [f"{h:02d}:00" for h in range(24)]
            counts = [hourly.get(h, 0) for h in range(24)]
            return {"hours": hours, "comment_counts": counts}

        except Exception as exc:
            logger.warning(f"get_speed_data 失败: {exc}")
            _reset_engine()
            return {"hours": [], "comment_counts": []}

    def get_keywords(self, date_str: str = None):
        """
        返回分组关键词：
          crawler: 爬虫配置中的关键词（来自 crawler_configs.json，保序去重）
          daily:   每日新闻分析关键词（来自 daily_topics，已排除 crawler 中已有的）
        爬虫配置页标签只展示 daily；监控仪表板下拉展示两组。
        """
        # ① 爬虫配置关键词（保持配置顺序，去重）
        crawler_kws: list = []
        seen_crawler: set = set()
        try:
            with _configs_lock:
                configs = _load_configs()
            for cfg in configs:
                for kw in cfg.get("config", {}).get("keywords", []):
                    kw = kw.strip()
                    if kw and kw not in seen_crawler:
                        seen_crawler.add(kw)
                        crawler_kws.append(kw)
        except Exception as e:
            logger.debug(f"get_keywords crawler_configs failed: {e}")

        # ② 每日新闻分析关键词（来自 daily_topics，保持原始顺序，去掉已在 crawler 中的）
        daily_kws: list = []
        daily_kws_seen: set = set()
        try:
            engine = _get_engine()
            with engine.connect() as conn:
                dt_date_where = ""
                dt_date_params: dict = {}
                if date_str:
                    dt_date_where = "AND extract_date = :ed"
                    dt_date_params = {"ed": date_str}
                rows = conn.execute(text(
                    f"SELECT keywords FROM daily_topics "
                    f"WHERE keywords IS NOT NULL {dt_date_where} "
                    f"ORDER BY extract_date DESC LIMIT 1"
                ), dt_date_params).fetchall()
                for (kw_field,) in rows:
                    if not kw_field:
                        continue
                    try:
                        kws = json.loads(kw_field) if kw_field.startswith('[') else [k.strip() for k in kw_field.split(',')]
                        for k in kws:
                            k = k.strip()
                            if k and k not in daily_kws_seen:
                                daily_kws_seen.add(k)
                                daily_kws.append(k)
                    except Exception:
                        k = kw_field.strip()
                        if k and k not in daily_kws_seen:
                            daily_kws_seen.add(k)
                            daily_kws.append(k)
        except Exception as exc:
            logger.warning(f"get_keywords daily_topics 失败: {exc}")
            _reset_engine()

        daily_only = [k for k in daily_kws if k not in seen_crawler][:100]
        return {
            "crawler": crawler_kws,
            "daily":   daily_only,
        }

    def get_comment_list(self, sentiment: int | None, keyword: str | None,
                         days: int = 6, page: int = 1, page_size: int = 20):
        """按情感标签分页查询评论明细，跨全部7张评论表合并。"""
        # 各表字段映射: (平台显示名, nickname列, avatar列, ip_location列, like_count列, pictures列)
        TABLE_FIELDS = {
            "xhs_note_comment":       ("小红书", "nickname",      "avatar",      "ip_location", "like_count",         "pictures"),
            "douyin_aweme_comment":   ("抖音",   "nickname",      "avatar",      "ip_location", "like_count",         "pictures"),
            "kuaishou_video_comment": ("快手",   "nickname",      "avatar",      None,          None,                 None),
            "bilibili_video_comment": ("B站",    "nickname",      "avatar",      None,          "like_count",         None),
            "weibo_note_comment":     ("微博",   "nickname",      "avatar",      "ip_location", "comment_like_count", None),
            "tieba_comment":          ("贴吧",   "user_nickname", "user_avatar", "ip_location", None,                 None),
            "zhihu_comment":          ("知乎",   "user_nickname", "user_avatar", "ip_location", "like_count",         None),
        }
        try:
            engine = _get_engine()
            from datetime import timezone, date as _date
            CST = timezone(timedelta(hours=8))
            today = datetime.now(CST).date()
            start_date = today - timedelta(days=days - 1)
            start_ts = int(datetime.combine(start_date, datetime.min.time()).replace(tzinfo=CST).timestamp()) * 1000
            end_ts   = int(datetime.combine(today + timedelta(days=1), datetime.min.time()).replace(tzinfo=CST).timestamp()) * 1000

            sentiment_sql = "" if sentiment is None else "AND sentiment_label = :sentiment"
            date_where    = "AND add_ts >= :ds AND add_ts < :de"
            base_params: dict = {"ds": start_ts, "de": end_ts}
            if sentiment is not None:
                base_params["sentiment"] = sentiment

            rows_all = []
            with engine.connect() as conn:
                for table, (platform, nick_col, avatar_col, ip_col, like_col, pic_col) in TABLE_FIELDS.items():
                    try:
                        kw_sql, kw_params = _kw_exists(table, keyword)
                        params = {**base_params, **kw_params}
                        nick_expr   = nick_col   if nick_col   else "NULL"
                        avatar_expr = avatar_col if avatar_col else "NULL"
                        ip_expr     = ip_col     if ip_col     else "NULL"
                        like_expr   = like_col   if like_col   else "NULL"
                        pic_expr    = pic_col    if pic_col    else "NULL"
                        sql = text(
                            f"SELECT add_ts, {nick_expr}, {avatar_expr}, {ip_expr},"
                            f" content, {like_expr}, {pic_expr}, sentiment_label"
                            f" FROM {table}"
                            f" WHERE sentiment_label IS NOT NULL {date_where} {sentiment_sql} {kw_sql}"
                            f" ORDER BY add_ts DESC LIMIT 200"
                        )
                        for r in conn.execute(sql, params).fetchall():
                            rows_all.append({
                                "platform": platform,
                                "add_ts": r[0],
                                "nickname": r[1] or "",
                                "avatar": r[2] or "",
                                "ip_location": r[3] or "",
                                "content": r[4] or "",
                                "like_count": str(r[5]) if r[5] is not None else "",
                                "pictures": r[6] or "",
                                "sentiment_label": r[7],
                            })
                    except Exception as e:
                        logger.debug(f"comment_list skip {table}: {e}")

            rows_all.sort(key=lambda x: x["add_ts"], reverse=True)
            total = len(rows_all)
            offset = (page - 1) * page_size
            items = rows_all[offset: offset + page_size]
            return {"total": total, "page": page, "page_size": page_size, "items": items}
        except Exception as exc:
            logger.warning(f"get_comment_list 失败: {exc}")
            _reset_engine()
            return {"total": 0, "page": 1, "page_size": page_size, "items": []}


# ===== Crawler Config Manager =====
_configs_lock = threading.Lock()


_CST = timezone(timedelta(hours=8))


def _normalize_created_at(ts: str) -> str:
    """将旧 ISO 格式（UTC naive）转为 CST 格式字符串，已正确格式化的直接返回。"""
    if not ts:
        return ts
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:  # naive → 视为 UTC，转 CST
            dt = dt.replace(tzinfo=timezone.utc).astimezone(_CST)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return ts


def _load_configs():
    try:
        if _CONFIGS_FILE.exists():
            configs = json.loads(_CONFIGS_FILE.read_text(encoding='utf-8'))
            for c in configs:
                if 'created_at' in c:
                    c['created_at'] = _normalize_created_at(c['created_at'])
            return configs
    except Exception as exc:
        logger.warning(f"加载爬虫配置失败: {exc}")
    return []


def _save_configs(configs):
    try:
        _CONFIGS_FILE.write_text(json.dumps(configs, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception as exc:
        logger.error(f"保存爬虫配置失败: {exc}")


def _load_cookies():
    try:
        if _COOKIES_FILE.exists():
            return json.loads(_COOKIES_FILE.read_text(encoding='utf-8'))
    except Exception:
        pass
    return {}


def _save_cookies(cookies):
    try:
        _COOKIES_FILE.write_text(json.dumps(cookies, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception as exc:
        logger.error(f"保存Cookie失败: {exc}")


# ===== Crawling task runner =====
def _run_crawl_task(task_id: str, keywords: list, platform: str):
    """在后台线程中执行爬取任务"""
    key = f"{'_'.join(keywords[:1])}_{platform}"
    with _running_lock:
        _running_tasks[key] = {
            "status": "running",
            "task_id": task_id,
            "keywords": keywords,
            "platform": platform,
            "started_at": datetime.now().isoformat(),
        }
    try:
        from ai_mock_crawler import run_mock_crawl
        result = run_mock_crawl(keywords, platform)
        with _running_lock:
            status = "completed" if result.get("success") else "failed"
            _running_tasks[key]["status"] = status
            _running_tasks[key]["finished_at"] = datetime.now().isoformat()
            _running_tasks[key]["notes_count"] = result.get("notes_count", 0)
            _running_tasks[key]["comments_count"] = result.get("comments_count", 0)
            if not result.get("success"):
                _running_tasks[key]["error"] = result.get("error", "未知错误")
    except Exception as exc:
        logger.exception(f"模拟爬取任务失败: {exc}")
        with _running_lock:
            _running_tasks[key] = {
                "status": "failed",
                "error": str(exc),
                "task_id": task_id,
                "platform": platform,
            }


# ===== Scheduler =====
_scheduler_thread = None
_scheduler_stop = threading.Event()


def _scheduler_loop():
    """简单的定时调度器，每分钟检查一次是否有到期任务"""
    import time
    last_run: dict = {}

    while not _scheduler_stop.is_set():
        try:
            with _configs_lock:
                configs = _load_configs()
            now = datetime.now()
            interval_map = {"5min": 5, "10min": 10, "30min": 30, "1h": 60}
            for task in configs:
                cfg = task.get("config", {})
                interval_mins = interval_map.get(cfg.get("interval", "1h"), 60)
                task_id = task.get("id", "")
                last = last_run.get(task_id)
                if last is None or (now - last).total_seconds() >= interval_mins * 60:
                    last_run[task_id] = now
                    keywords = cfg.get("keywords", [])
                    for platform in cfg.get("platforms", []):
                        t = threading.Thread(
                            target=_run_crawl_task,
                            args=(str(uuid.uuid4()), keywords, platform),
                            daemon=True,
                        )
                        t.start()
        except Exception as exc:
            logger.warning(f"调度器循环异常: {exc}")
        time.sleep(60)


def _start_scheduler():
    global _scheduler_thread
    if _scheduler_thread is None or not _scheduler_thread.is_alive():
        _scheduler_stop.clear()
        _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="crawler-scheduler")
        _scheduler_thread.start()
        logger.info("爬虫调度器已启动")


_start_scheduler()


# ===== Routes =====

@monitoring_bp.route('/')
def index():
    return render_template('monitoring_dashboard.html')


@monitoring_bp.route('/crawler-config')
def crawler_config_page():
    return render_template('crawler_config.html')


@monitoring_bp.route('/comments')
def comment_list_page():
    return render_template('comment_list.html')


@monitoring_bp.route('/api/comments')
def get_comments():
    sentiment_raw = request.args.get('sentiment', '')
    sentiment = int(sentiment_raw) if sentiment_raw.lstrip('-').isdigit() else None
    keyword   = request.args.get('keyword', '') or None
    days      = int(request.args.get('days', 6))
    page      = max(1, int(request.args.get('page', 1)))
    page_size = min(50, max(1, int(request.args.get('page_size', 20))))
    svc = MonitoringService()
    data = svc.get_comment_list(sentiment, keyword, days, page, page_size)
    return jsonify({"success": True, "data": data})


@monitoring_bp.route('/api/all-data')
def all_data():
    """返回仪表板所需的全部数据"""
    try:
        days = max(1, min(30, int(request.args.get('days', 6))))
        keyword = request.args.get('keyword', '').strip() or None
        date_str = request.args.get('date', '').strip() or None
        svc = MonitoringService()
        return jsonify({
            "success": True,
            "data": {
                "topic":     svc.get_latest_topic(None),  # topicCard 始终显示最新每日新闻，不受关键词筛选影响
                "trend":     svc.get_trend_data(days, keyword, date_str),
                "sentiment": svc.get_sentiment_data(keyword, date_str, days),
                "location":  svc.get_location_data(keyword, date_str, days),
                "speed":     svc.get_speed_data(keyword, date_str),
            },
        })
    except Exception as exc:
        logger.exception(f"all_data 失败: {exc}")
        return jsonify({"success": False, "message": str(exc)}), 500


@monitoring_bp.route('/api/crawler/keywords')
def get_keywords():
    """返回可用关键词列表（支持按日期过滤）"""
    try:
        date_str = request.args.get('date', '').strip() or None
        svc = MonitoringService()
        return jsonify({"success": True, "data": svc.get_keywords(date_str)})
    except Exception as exc:
        logger.warning(f"get_keywords 失败: {exc}")
        return jsonify({"success": True, "data": []})


@monitoring_bp.route('/api/crawler/platforms')
def get_platforms():
    return jsonify({"success": True, "data": PLATFORMS})


@monitoring_bp.route('/api/crawler/configs')
def get_configs():
    with _configs_lock:
        configs = _load_configs()
    with _running_lock:
        running = dict(_running_tasks)
    return jsonify({"success": True, "data": {"configs": configs, "running_details": running}})


@monitoring_bp.route('/api/crawler/config', methods=['POST'])
def add_config():
    data = request.get_json() or {}
    keywords = data.get("keywords", [])
    platforms = data.get("platforms", [])
    interval = data.get("interval", "1h")

    if not keywords or not platforms:
        return jsonify({"success": False, "message": "keywords 和 platforms 不能为空"}), 400

    # 判重：同平台且关键词有交集时，若任务仍在运行则拒绝
    kw_set = set(keywords)
    with _running_lock:
        for task_info in _running_tasks.values():
            if task_info.get("status") != "running":
                continue
            running_platform = task_info.get("platform")
            if running_platform not in platforms:
                continue
            overlap = kw_set & set(task_info.get("keywords", []))
            if overlap:
                return jsonify({
                    "success": False,
                    "message": f"平台「{running_platform}」的关键词「{'、'.join(sorted(overlap))}」正在运行中，请等待完成后再启动"
                }), 409

    task = {
        "id": str(uuid.uuid4())[:8],
        "config": {"keywords": keywords, "platforms": platforms, "interval": interval},
        "created_at": datetime.now(tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
    }
    with _configs_lock:
        configs = _load_configs()
        configs.append(task)
        _save_configs(configs)

    return jsonify({"success": True, "data": task})


@monitoring_bp.route('/api/crawler/config/<task_id>', methods=['DELETE'])
def delete_config(task_id):
    with _configs_lock:
        configs = _load_configs()
        new_configs = [c for c in configs if c.get("id") != task_id]
        if len(new_configs) == len(configs):
            return jsonify({"success": False, "message": "任务不存在"}), 404
        _save_configs(new_configs)
    return jsonify({"success": True})


@monitoring_bp.route('/api/crawler/cookies')
def get_cookies():
    cookies = _load_cookies()
    result = {}
    for platform in ["xhs", "dy", "bili", "wb", "tieba", "zhihu", "ks"]:
        val = cookies.get(platform, {}).get("value", "")
        result[platform] = {"configured": bool(val), "length": len(val)}
    return jsonify({"success": True, "data": result})


@monitoring_bp.route('/api/crawler/cookies', methods=['POST'])
def save_cookie():
    data = request.get_json() or {}
    platform = data.get("platform", "").strip()
    cookie_value = data.get("cookies", "").strip()

    if not platform or not cookie_value:
        return jsonify({"success": False, "message": "platform 和 cookies 不能为空"}), 400

    cookies = _load_cookies()
    cookies[platform] = {"value": cookie_value, "updated_at": datetime.now().isoformat()}
    _save_cookies(cookies)
    return jsonify({"success": True})
