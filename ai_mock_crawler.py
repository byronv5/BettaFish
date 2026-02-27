#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI 模拟爬虫 - 用 AI 生成模拟平台数据并入库（演示用途）

数据完全由 LLM 生成，不访问任何真实平台。
请通过官方开放 API（微博、知乎等均有开发者 API）获取真实数据。
"""

import json
import random
import time
import uuid
from datetime import datetime
from typing import Dict, List
from urllib.parse import quote_plus

from loguru import logger
from sqlalchemy import create_engine, text

# ===== 平台风格描述 =====
_PLATFORM_INFO = {
    "xhs":   {"name": "小红书",   "style": "生活分享日记体、图文并茂、积极向上、带话题标签",   "content_type": "笔记"},
    "dy":    {"name": "抖音",     "style": "短视频脚本、话题标签丰富、简洁有力",               "content_type": "视频"},
    "bili":  {"name": "B站",      "style": "知识向、分析深度、配合弹幕文化、UP主风格",         "content_type": "视频"},
    "wb":    {"name": "微博",     "style": "140字内、简短有力、带#话题标签#",                  "content_type": "帖子"},
    "tieba": {"name": "百度贴吧", "style": "讨论帖、口语化、互动感强",                        "content_type": "帖子"},
    "zhihu": {"name": "知乎",     "style": "专业严谨、引用数据、结构化长文",                   "content_type": "回答"},
    "ks":    {"name": "快手",     "style": "接地气、生活化、城乡场景",                         "content_type": "视频"},
}

_PROVINCES = [
    "北京", "上海", "广东", "江苏", "浙江", "四川", "湖北",
    "湖南", "山东", "河南", "福建", "重庆", "辽宁", "陕西", "天津",
]


def _build_db_url() -> str:
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


def _get_llm_client():
    """优先 QUERY_ENGINE，其次 INSIGHT_ENGINE，再次 MEDIA_ENGINE"""
    from config import settings
    from openai import OpenAI
    for api_key, base_url, model in [
        (settings.QUERY_ENGINE_API_KEY, settings.QUERY_ENGINE_BASE_URL, settings.QUERY_ENGINE_MODEL_NAME),
        (settings.INSIGHT_ENGINE_API_KEY, settings.INSIGHT_ENGINE_BASE_URL, settings.INSIGHT_ENGINE_MODEL_NAME),
        (settings.MEDIA_ENGINE_API_KEY, settings.MEDIA_ENGINE_BASE_URL, settings.MEDIA_ENGINE_MODEL_NAME),
    ]:
        if api_key:
            return OpenAI(api_key=api_key, base_url=base_url), model
    raise RuntimeError(
        "未配置任何 LLM API Key（QUERY_ENGINE_API_KEY / INSIGHT_ENGINE_API_KEY / MEDIA_ENGINE_API_KEY），"
        "无法生成模拟数据。"
    )


_AI_BATCH_SIZE = 4  # 每次 AI 调用生成的笔记数，避免输出过长导致 JSON 截断


def _parse_ai_json(raw: str) -> List[dict]:
    """解析 AI 返回的 JSON，兼容 markdown 代码块包裹，支持自动修复"""
    from json_repair import repair_json
    raw = raw.strip()
    if "```" in raw:
        for part in raw.split("```"):
            part = part.strip()
            if part.lower().startswith("json"):
                part = part[4:].strip()
            try:
                result = json.loads(part)
                if isinstance(result, list):
                    return result
            except Exception:
                continue
    try:
        return json.loads(raw)
    except Exception:
        result = json.loads(repair_json(raw))
        if not isinstance(result, list):
            raise ValueError(f"AI 返回的不是 JSON 数组: {type(result)}")
        return result


def _ai_generate_batch(info: dict, kw_str: str, batch_count: int, client, model: str) -> List[dict]:
    """单次 AI 调用，生成 batch_count 条笔记"""
    prompt = (
        f"你是{info['name']}平台内容模拟器。请基于以下关键词生成逼真的模拟数据。\n"
        f"关键词: {kw_str}\n"
        f"平台: {info['name']}（风格: {info['style']}）\n"
        f"需要: {batch_count} 条{info['content_type']}，每条附 10~15 条评论（每条数量随机不同）\n\n"
        "要求：\n"
        "1. 每条内容的 ip_location 和评论的 ip_location 均使用中国不同省份，尽量覆盖多个地区，不要集中在同一省份\n"
        "2. 每条内容的评论数量在 10~15 之间随机，各条内容的评论数量不同\n"
        "3. 用户昵称和评论内容要多样、真实\n"
        "4. 每条评论必须包含 sentiment_label 字段（整数：0=负面，1=中性，2=正面）\n"
        "   sentiment_label 必须与评论内容的实际情感语义完全一致：\n"
        "   - 正面评论（表达认可、支持、赞美、开心）→ sentiment_label: 2\n"
        "   - 中性评论（陈述事实、提问、无明显情感倾向）→ sentiment_label: 1\n"
        "   - 负面评论（表达批评、担忧、不满、质疑）→ sentiment_label: 0\n"
        "   三种情感都要有，大致比例：正面约40%，中性约35%，负面约25%\n\n"
        "严格按 JSON 数组格式返回，不要任何多余说明文字:\n"
        "[\n"
        "  {\n"
        '    "title": "标题（可为空字符串）",\n'
        '    "content": "正文（60-200字，贴合平台风格和关键词）",\n'
        '    "user_nickname": "真实感昵称",\n'
        '    "ip_location": "中国省份（如广东、北京、四川等，各条不同）",\n'
        '    "liked_count": "互动数（如 1234 或 2.3万）",\n'
        '    "comment_count": "评论数（纯数字字符串）",\n'
        '    "source_keyword": "本条内容对应的一个关键词",\n'
        '    "comments": [\n'
        '      {\n'
        '        "content": "评论内容（10-60字，情感语义与sentiment_label严格对应）",\n'
        '        "user_nickname": "昵称",\n'
        '        "ip_location": "中国省份（各条评论地域不同）",\n'
        '        "like_count": "点赞数字符串",\n'
        '        "sentiment_label": 2\n'
        '      }\n'
        '    ]\n'
        '  }\n'
        ']'
    )
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.85,
    )
    return _parse_ai_json(response.choices[0].message.content)


def _ai_generate(keywords: List[str], platform: str, notes_count: int, comments_per_note: int) -> List[dict]:
    """分批调用 AI 生成模拟数据，返回 note 列表"""
    info = _PLATFORM_INFO.get(platform, {"name": platform, "style": "通用", "content_type": "内容"})
    kw_str = "、".join(keywords[:5])
    client, model = _get_llm_client()

    all_notes: List[dict] = []
    remaining = notes_count
    while remaining > 0:
        batch = min(_AI_BATCH_SIZE, remaining)
        batch_notes = _ai_generate_batch(info, kw_str, batch, client, model)
        all_notes.extend(batch_notes)
        remaining -= batch
    return all_notes


_FALLBACK_COMMENTS = {
    2: [  # 正面
        "这个内容太好了，感谢分享，学到了很多！",
        "完全赞同，这正是我一直想说的观点。",
        "写得很好，逻辑清晰，支持！",
        "太有共鸣了，收藏了慢慢看。",
        "很有价值的内容，希望多出这样的好文章！",
        "强烈推荐给身边的朋友，干货满满！",
        "分析得非常到位，给作者点赞！",
    ],
    1: [  # 中性
        "这个话题确实挺复杂的，值得深入研究。",
        "请问有相关数据来源吗？想了解更多。",
        "不同角度看可能结论不一样，欢迎讨论。",
        "我也在关注这个问题，持续观察中。",
        "内容看完了，有些地方还需要消化一下。",
        "刚好在看相关资料，可以参考一下。",
        "这个领域变化挺快的，信息更新要跟上。",
    ],
    0: [  # 负面
        "感觉分析有点片面，没有考虑到实际情况。",
        "这个结论我不太认可，依据不够充分。",
        "过于乐观了，现实中问题要复杂得多。",
        "有些数据来源存疑，希望作者核实一下。",
        "观点太主观，缺乏客观依据。",
        "这种说法之前也有人提过，但效果并不好。",
    ],
}


def _fallback_generate(keywords: List[str], platform: str, notes_count: int, comments_per_note: int) -> List[dict]:
    """AI 失败时的模板回退"""
    notes = []
    for i in range(notes_count):
        kw = keywords[i % len(keywords)] if keywords else "热点"
        # 按 40/35/25 比例生成情感标签序列，保证内容和标签严格对应
        n_comments = random.randint(10, 20)
        sentiments = random.choices([0, 1, 2], weights=[25, 35, 40], k=n_comments)
        comments = []
        for s in sentiments:
            template = random.choice(_FALLBACK_COMMENTS[s])
            comments.append({
                "content": template.replace("这个", f"{kw}相关").replace("话题", f"{kw}"),
                "user_nickname": f"路人{random.randint(1000, 9999)}",
                "ip_location": random.choice(_PROVINCES),
                "like_count": str(random.randint(1, 500)),
                "sentiment_label": s,
            })
        notes.append({
            "title": f"关于{kw}的讨论 #{i + 1}",
            "content": (
                f"近期{kw}引发了广泛关注，相关数据显示该话题持续发酵。"
                f"从多个维度分析，{kw}呈现出复杂的舆情态势，值得深入研究。"
            ),
            "user_nickname": f"用户{random.randint(10000, 99999)}",
            "ip_location": random.choice(_PROVINCES),
            "liked_count": str(random.randint(50, 9999)),
            "source_keyword": kw,
            "comments": comments,
        })
    return notes


# ===== 工具函数 =====

def _now_ms() -> int:
    return int(time.time() * 1000)


def _rand_past_ms(days: int = 7) -> int:
    return _now_ms() - random.randint(0, days * 86_400_000)


def _str_id() -> str:
    return uuid.uuid4().hex[:20]


def _int_id() -> int:
    return random.randint(10 ** 14, 10 ** 15)


def _to_int(val, default: int = 0) -> int:
    try:
        s = str(val).replace("万", "0000").replace(".", "").replace(",", "").replace(" ", "")
        digits = "".join(c for c in s if c.isdigit())
        return int(digits) if digits else default
    except Exception:
        return default


# ===== 各平台插入函数 =====

def _insert_xhs(conn, notes: List[dict]):
    for note in notes:
        note_id = _str_id()
        ts = _now_ms()
        kw = note.get("source_keyword", "")
        conn.execute(text(
            'INSERT INTO xhs_note '
            '(user_id, nickname, ip_location, add_ts, last_modify_ts, note_id, type, title, "desc", '
            '"time", last_update_time, liked_count, collected_count, comment_count, share_count, source_keyword) '
            'VALUES (:uid, :nick, :ip, :ts, :ts, :note_id, :type, :title, :desc, '
            ':ctime, :ts, :liked, :collected, :cmt_cnt, :share, :kw)'
        ), {
            "uid": _str_id(), "nick": note.get("user_nickname") or note.get("user_nnickname", ""), "ip": note.get("ip_location", ""),
            "ts": ts, "note_id": note_id, "type": "normal",
            "title": note.get("title", ""), "desc": note.get("content", ""),
            "ctime": _rand_past_ms(), "liked": note.get("liked_count", "0"),
            "collected": "0", "cmt_cnt": note.get("comment_count", "0"), "share": "0",
            "kw": kw,
        })
        for c in note.get("comments", []):
            conn.execute(text(
                'INSERT INTO xhs_note_comment '
                '(user_id, nickname, ip_location, add_ts, last_modify_ts, comment_id, create_time, '
                'note_id, content, sub_comment_count, like_count, sentiment_label) '
                'VALUES (:uid, :nick, :ip, :ts, :ts, :cid, :ctime, :note_id, :content, 0, :like, :sentiment)'
            ), {
                "uid": _str_id(), "nick": c.get("user_nickname") or c.get("user_nnickname", ""), "ip": c.get("ip_location", ""),
                "ts": ts, "cid": _str_id(), "ctime": _rand_past_ms(3),
                "note_id": note_id, "content": c.get("content", ""),
                "like": c.get("like_count", "0"),
                "sentiment": c.get("sentiment_label", 1),
            })


def _insert_douyin(conn, notes: List[dict]):
    for note in notes:
        aweme_id = _str_id()
        ts = _now_ms()
        kw = note.get("source_keyword", "")
        conn.execute(text(
            'INSERT INTO douyin_aweme '
            '(user_id, nickname, ip_location, add_ts, last_modify_ts, aweme_id, aweme_type, '
            'title, "desc", create_time, liked_count, comment_count, share_count, collected_count, source_keyword) '
            'VALUES (:uid, :nick, :ip, :ts, :ts, :aweme_id, :atype, '
            ':title, :desc, :ctime, :liked, :cmt_cnt, :share, :collected, :kw)'
        ), {
            "uid": _str_id(), "nick": note.get("user_nickname") or note.get("user_nnickname", ""), "ip": note.get("ip_location", ""),
            "ts": ts, "aweme_id": aweme_id, "atype": "video",
            "title": note.get("title", ""), "desc": note.get("content", ""),
            "ctime": _rand_past_ms(), "liked": note.get("liked_count", "0"),
            "cmt_cnt": note.get("comment_count", "0"), "share": "0", "collected": "0",
            "kw": kw,
        })
        for c in note.get("comments", []):
            conn.execute(text(
                'INSERT INTO douyin_aweme_comment '
                '(user_id, nickname, ip_location, add_ts, last_modify_ts, comment_id, '
                'aweme_id, content, create_time, sub_comment_count, like_count, sentiment_label) '
                'VALUES (:uid, :nick, :ip, :ts, :ts, :cid, :aweme_id, :content, :ctime, :sub, :like, :sentiment)'
            ), {
                "uid": _str_id(), "nick": c.get("user_nickname") or c.get("user_nnickname", ""), "ip": c.get("ip_location", ""),
                "ts": ts, "cid": _str_id(), "aweme_id": aweme_id,
                "content": c.get("content", ""), "ctime": _rand_past_ms(3),
                "sub": "0", "like": c.get("like_count", "0"),
                "sentiment": c.get("sentiment_label", 1),
            })


def _insert_bilibili(conn, notes: List[dict]):
    for note in notes:
        video_id = _int_id()
        ts = _now_ms()
        kw = note.get("source_keyword", "")
        conn.execute(text(
            'INSERT INTO bilibili_video '
            '(user_id, nickname, add_ts, last_modify_ts, video_id, video_type, '
            'title, "desc", create_time, liked_count, video_play_count, video_comment, source_keyword) '
            'VALUES (:uid, :nick, :ts, :ts, :vid, :vtype, '
            ':title, :desc, :ctime, :liked, :play, :cmt, :kw)'
        ), {
            "uid": str(_int_id()), "nick": note.get("user_nickname") or note.get("user_nnickname", ""),
            "ts": ts, "vid": video_id, "vtype": "video",
            "title": note.get("title", ""), "desc": note.get("content", ""),
            "ctime": _rand_past_ms(),
            "liked": _to_int(note.get("liked_count", "0")),
            "play": str(random.randint(1000, 500000)),
            "cmt": note.get("comment_count", "0"),
            "kw": kw,
        })
        for c in note.get("comments", []):
            conn.execute(text(
                'INSERT INTO bilibili_video_comment '
                '(user_id, nickname, add_ts, last_modify_ts, comment_id, video_id, '
                'content, create_time, sub_comment_count, like_count, sentiment_label) '
                'VALUES (:uid, :nick, :ts, :ts, :cid, :vid, :content, :ctime, :sub, :like, :sentiment)'
            ), {
                "uid": str(_int_id()), "nick": c.get("user_nickname") or c.get("user_nnickname", ""),
                "ts": ts, "cid": _int_id(), "vid": video_id,
                "content": c.get("content", ""), "ctime": _rand_past_ms(3),
                "sub": "0", "like": c.get("like_count", "0"),
                "sentiment": c.get("sentiment_label", 1),
            })


def _insert_weibo(conn, notes: List[dict]):
    for note in notes:
        note_id = _str_id()
        ts = _now_ms()
        ctime = _rand_past_ms()
        dt = datetime.fromtimestamp(ctime / 1000).strftime("%Y-%m-%d %H:%M:%S")
        kw = note.get("source_keyword", "")
        conn.execute(text(
            'INSERT INTO weibo_note '
            '(user_id, nickname, ip_location, add_ts, last_modify_ts, note_id, '
            'content, create_time, create_date_time, liked_count, comments_count, shared_count, source_keyword) '
            'VALUES (:uid, :nick, :ip, :ts, :ts, :note_id, '
            ':content, :ctime, :dt, :liked, :cmt, :share, :kw)'
        ), {
            "uid": _str_id(), "nick": note.get("user_nickname") or note.get("user_nnickname", ""), "ip": note.get("ip_location", ""),
            "ts": ts, "note_id": note_id,
            "content": (note.get("title", "") + " " + note.get("content", "")).strip(),
            "ctime": ctime, "dt": dt,
            "liked": note.get("liked_count", "0"),
            "cmt": note.get("comment_count", "0"),
            "share": "0", "kw": kw,
        })
        for c in note.get("comments", []):
            c_ts = _rand_past_ms(3)
            c_dt = datetime.fromtimestamp(c_ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(text(
                'INSERT INTO weibo_note_comment '
                '(user_id, nickname, ip_location, add_ts, last_modify_ts, comment_id, note_id, '
                'content, create_time, create_date_time, comment_like_count, sub_comment_count, sentiment_label) '
                'VALUES (:uid, :nick, :ip, :ts, :ts, :cid, :note_id, '
                ':content, :ctime, :cdt, :like, :sub, :sentiment)'
            ), {
                "uid": _str_id(), "nick": c.get("user_nickname") or c.get("user_nnickname", ""), "ip": c.get("ip_location", ""),
                "ts": ts, "cid": _str_id(), "note_id": note_id,
                "content": c.get("content", ""), "ctime": c_ts, "cdt": c_dt,
                "like": c.get("like_count", "0"), "sub": "0",
                "sentiment": c.get("sentiment_label", 1),
            })


def _insert_tieba(conn, notes: List[dict]):
    for note in notes:
        note_id = _str_id()
        ts = _now_ms()
        pub_time = datetime.fromtimestamp(_rand_past_ms() / 1000).strftime("%Y-%m-%d %H:%M")
        kw = note.get("source_keyword", "")
        conn.execute(text(
            'INSERT INTO tieba_note '
            '(note_id, title, "desc", note_url, publish_time, user_nickname, tieba_name, tieba_link, '
            'total_replay_num, ip_location, add_ts, last_modify_ts, source_keyword) '
            'VALUES (:nid, :title, :desc, :url, :pt, :nick, :tname, :tlink, :num, :ip, :ts, :ts, :kw)'
        ), {
            "nid": note_id, "title": note.get("title", kw),
            "desc": note.get("content", ""),
            "url": f"https://tieba.baidu.com/p/{note_id}",
            "pt": pub_time, "nick": note.get("user_nickname") or note.get("user_nnickname", ""),
            "tname": f"{kw}吧", "tlink": f"https://tieba.baidu.com/f?kw={kw}",
            "num": len(note.get("comments", [])),
            "ip": note.get("ip_location", ""),
            "ts": ts, "kw": kw,
        })
        for c in note.get("comments", []):
            conn.execute(text(
                'INSERT INTO tieba_comment '
                '(comment_id, content, user_nickname, tieba_name, tieba_link, '
                'publish_time, ip_location, sub_comment_count, note_id, note_url, add_ts, last_modify_ts, sentiment_label) '
                'VALUES (:cid, :content, :nick, :tname, :tlink, :pt, :ip, 0, :nid, :url, :ts, :ts, :sentiment)'
            ), {
                "cid": _str_id(), "content": c.get("content", ""),
                "nick": c.get("user_nickname") or c.get("user_nnickname", ""),
                "tname": f"{kw}吧", "tlink": f"https://tieba.baidu.com/f?kw={kw}",
                "pt": datetime.fromtimestamp(_rand_past_ms(3) / 1000).strftime("%Y-%m-%d %H:%M"),
                "ip": c.get("ip_location", ""),
                "nid": note_id, "url": f"https://tieba.baidu.com/p/{note_id}",
                "ts": ts,
                "sentiment": c.get("sentiment_label", 1),
            })


def _insert_zhihu(conn, notes: List[dict]):
    for note in notes:
        content_id = _str_id()
        ts = _now_ms()
        kw = note.get("source_keyword", "")
        conn.execute(text(
            'INSERT INTO zhihu_content '
            '(content_id, content_type, content_text, content_url, title, "desc", '
            'created_time, updated_time, voteup_count, comment_count, '
            'source_keyword, user_id, user_link, user_nickname, user_avatar, user_url_token, '
            'add_ts, last_modify_ts) '
            'VALUES (:cid, :ctype, :text, :url, :title, :desc, '
            ':ctime, :ts, :vote, :cmt, :kw, :uid, :ulink, :nick, :avatar, :utoken, :ts, :ts)'
        ), {
            "cid": content_id, "ctype": "answer",
            "text": note.get("content", ""),
            "url": f"https://www.zhihu.com/question/{_str_id()}/answer/{content_id}",
            "title": note.get("title", f"关于{kw}的看法"),
            "desc": note.get("content", "")[:100],
            "ctime": _rand_past_ms(), "ts": ts,
            "vote": _to_int(note.get("liked_count", "0")),
            "cmt": _to_int(note.get("comment_count", str(len(note.get("comments", []))))),
            "kw": kw,
            "uid": _str_id(),
            "ulink": f"https://www.zhihu.com/people/{_str_id()}",
            "nick": note.get("user_nickname") or note.get("user_nnickname", ""),
            "avatar": "",
            "utoken": _str_id(),
        })
        for c in note.get("comments", []):
            conn.execute(text(
                'INSERT INTO zhihu_comment '
                '(comment_id, content, publish_time, ip_location, sub_comment_count, '
                'like_count, dislike_count, content_id, content_type, '
                'user_id, user_link, user_nickname, user_avatar, add_ts, last_modify_ts, sentiment_label) '
                'VALUES (:cid, :content, :pt, :ip, 0, :like, 0, :content_id, :ctype, '
                ':uid, :ulink, :nick, :avatar, :ts, :ts, :sentiment)'
            ), {
                "cid": _str_id(), "content": c.get("content", ""),
                "pt": _rand_past_ms(3),
                "ip": c.get("ip_location", ""),
                "like": _to_int(c.get("like_count", "0")),
                "content_id": content_id, "ctype": "answer",
                "uid": _str_id(),
                "ulink": f"https://www.zhihu.com/people/{_str_id()}",
                "nick": c.get("user_nickname") or c.get("user_nnickname", ""), "avatar": "",
                "ts": ts,
                "sentiment": c.get("sentiment_label", 1),
            })


def _insert_kuaishou(conn, notes: List[dict]):
    for note in notes:
        video_id = _str_id()
        ts = _now_ms()
        kw = note.get("source_keyword", "")
        conn.execute(text(
            'INSERT INTO kuaishou_video '
            '(user_id, nickname, add_ts, last_modify_ts, video_id, video_type, '
            'title, "desc", create_time, liked_count, viewd_count, source_keyword) '
            'VALUES (:uid, :nick, :ts, :ts, :vid, :vtype, '
            ':title, :desc, :ctime, :liked, :viewed, :kw)'
        ), {
            "uid": _str_id(), "nick": note.get("user_nickname") or note.get("user_nnickname", ""),
            "ts": ts, "vid": video_id, "vtype": "video",
            "title": note.get("title", ""), "desc": note.get("content", ""),
            "ctime": _rand_past_ms(),
            "liked": note.get("liked_count", "0"),
            "viewed": str(random.randint(5000, 500000)),
            "kw": kw,
        })
        for c in note.get("comments", []):
            conn.execute(text(
                'INSERT INTO kuaishou_video_comment '
                '(user_id, nickname, add_ts, last_modify_ts, comment_id, video_id, '
                'content, create_time, sub_comment_count, sentiment_label) '
                'VALUES (:uid, :nick, :ts, :ts, :cid, :vid, :content, :ctime, :sub, :sentiment)'
            ), {
                "uid": _str_id(), "nick": c.get("user_nickname") or c.get("user_nnickname", ""),
                "ts": ts, "cid": _int_id(), "vid": video_id,
                "content": c.get("content", ""), "ctime": _rand_past_ms(3),
                "sub": "0",
                "sentiment": c.get("sentiment_label", 1),
            })


_INSERT_MAP = {
    "xhs":   _insert_xhs,
    "dy":    _insert_douyin,
    "bili":  _insert_bilibili,
    "wb":    _insert_weibo,
    "tieba": _insert_tieba,
    "zhihu": _insert_zhihu,
    "ks":    _insert_kuaishou,
}


def run_mock_crawl(keywords: List[str], platform: str,
                   notes_count: int = 0, comments_per_note: int = 0) -> Dict:
    """
    用 AI 生成模拟数据并写入数据库（演示用途，不访问真实平台）。

    Args:
        keywords: 关键词列表
        platform: 平台代码（xhs/dy/bili/wb/tieba/zhihu/ks）
        notes_count: 内容数量，0 表示随机 10~20
        comments_per_note: 已废弃，评论数量由 AI/模板各自随机 10~20

    Returns:
        {"success": True/False, "notes_count": int, "comments_count": int, ...}
    """
    if notes_count <= 0:
        notes_count = random.randint(10, 20)
    logger.info(f"[MockCrawler] 平台={platform} 关键词={keywords} 内容数={notes_count} 开始生成模拟数据")

    if platform not in _INSERT_MAP:
        return {"success": False, "error": f"不支持的平台: {platform}"}

    try:
        # 生成数据（优先 AI，失败则回退模板）
        try:
            notes = _ai_generate(keywords, platform, notes_count, comments_per_note)
            logger.info(f"[MockCrawler] AI 成功生成 {len(notes)} 条内容")
        except Exception as ai_err:
            logger.warning(f"[MockCrawler] AI 生成失败，使用模板回退: {ai_err}")
            notes = _fallback_generate(keywords, platform, notes_count, comments_per_note)

        # 写入数据库
        engine = create_engine(_build_db_url(), pool_pre_ping=True)
        try:
            with engine.begin() as conn:
                _INSERT_MAP[platform](conn, notes)
        finally:
            engine.dispose()

        total_comments = sum(len(n.get("comments", [])) for n in notes)
        logger.info(
            f"[MockCrawler] 平台={platform} 写入完成: "
            f"{len(notes)} 条内容, {total_comments} 条评论"
        )
        return {
            "success": True,
            "notes_count": len(notes),
            "comments_count": total_comments,
            "platform": platform,
        }

    except Exception as exc:
        logger.exception(f"[MockCrawler] 平台={platform} 模拟数据生成失败: {exc}")
        return {"success": False, "error": str(exc), "platform": platform}
