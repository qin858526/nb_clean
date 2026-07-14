# -*- coding: utf-8 -*-
"""
B站视频分析QQ机器人
流程：B站链接/BV号 → 优先使用B站AI字幕/CC字幕 →
      无字幕则FFmpeg下载音频 → whisper.cpp语音转文字 →
      DeepSeek API总结 → 回复QQ群/私聊

触发规则：
  群聊 — 引用B站消息 + @机器人
  私聊 — 直接发链接/BV号
"""

import re
import json
import time
import os
os.environ.setdefault('CT2_FORCE_CPU', '1')  # 强制 ctranslate2 使用 CPU，避免加载 libcublas
import ssl
import hashlib
import urllib.parse
import asyncio
from pathlib import Path

import urllib3
import aiohttp
from dotenv import load_dotenv

import nonebot
from nonebot import on_message, logger, get_driver
from nonebot.adapters.onebot.v11 import Bot, MessageEvent, Message
from nonebot.adapters.onebot.v11 import Adapter as OneBotV11Adapter
from nonebot.rule import Rule

# ===================== 环境配置 =====================
load_dotenv()
DASHSCOPE_API_KEY = os.getenv("DEEPSEEK_API_KEY", os.getenv("DASHSCOPE_API_KEY", ""))
NAPCAT_WS_URL = os.getenv("NAPCAT_WS_URL", "ws://127.0.0.1:6099")

# ===================== 可调参数 =====================
# whisper.cpp 模型路径（GGML 量化，CPU 推理快 3-5x）
WHISPER_CLI = "/root/whisper.cpp/build/bin/whisper-cli"
WHISPER_MODEL_PATH = "/root/whisper.cpp/models/ggml-base.bin"
WHISPER_FAST_MODEL_PATH = "/root/whisper.cpp/models/ggml-tiny.bin"
COOL_DOWN_TIME = 6                   # 群聊冷却秒数
COOL_DOWN_CLEANUP_INTERVAL = 300     # 冷却字典清理间隔（秒）
LLM_TIMEOUT = 60                     # LLM 调用超时（秒）
WHISPER_TIMEOUT = 600                # Whisper 转写超时（秒）
DEEPSEEK_API_URL = os.getenv("DEEPSEEK_API_URL", "https://api.deepseek.com/v1/chat/completions")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")
AUDIO_DOWNLOAD_TIMEOUT = 60          # FFmpeg 下载超时（秒）
HTTP_TIMEOUT = 15                    # HTTP 请求超时（秒）

SAVE_DIR = Path("./bilibili_temp")
SUBTITLE_CACHE_DIR = SAVE_DIR / "subtitle_cache"
SUBTITLE_CACHE_TTL = 259200  # 字幕缓存有效期（秒），3天内重复BV号直接用缓存
BILIBILI_COOKIE = os.getenv("BILIBILI_COOKIE", "")  # B站完整登录Cookie（用于获取字幕，需包含buvid3/buvid4/bili_ticket等）

# 输入长度硬限制（仅限制元数据字段，字幕不截断——DeepSeek 128K 上下文足够）
MAX_TITLE_LEN = 200
MAX_DESC_LEN = 800
MAX_DANMAKU_LEN = 800

# ===================== SSL 与日志 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl._create_default_https_context = ssl._create_unverified_context

# ===================== NoneBot 初始化 =====================
nonebot.init(driver="~fastapi", host="0.0.0.0", port=18082, onebot_api_roots={"1722884826": "http://127.0.0.1:18081"}, env_file=None)
driver = get_driver()
driver.register_adapter(OneBotV11Adapter)

# ===================== 全局 HTTP 会话（复用连接）=====================
http_session: aiohttp.ClientSession | None = None
_http_session_lock = asyncio.Lock()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bilibili.com/",
    "Origin": "https://www.bilibili.com",
    "Cookie": BILIBILI_COOKIE,
}


async def get_http_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None:
        async with _http_session_lock:
            if http_session is None:
                http_session = aiohttp.ClientSession(
                    headers=HEADERS,
                    connector=aiohttp.TCPConnector(ssl=False),
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                )
    return http_session


async def close_http_session():
    global http_session
    if http_session:
        await http_session.close()
        http_session = None
        logger.info("🔌 HTTP 会话已关闭")


# ===================== whisper.cpp 转写（替代 openai-whisper，CPU 快 3-5x）=====================


# ===================== LLM 调用（DeepSeek API, OpenAI 兼容）=====================
# 使用 aiohttp 直接调用，不依赖第三方 SDK

# ===================== 冷却字典（TTL + 自动清理）=====================
_cool_down: dict[str, float] = {}
_cool_down_lock = asyncio.Lock()
_cool_down_cleanup_task: asyncio.Task | None = None

# ===================== 任务排队 =====================
_task_queue: asyncio.Queue = asyncio.Queue()  # 排队队列

# ===================== 消息缓存（iOS分享消息get_msg不可用，主动缓存视频卡片）=====================
_msg_cache: dict[int, str] = {}  # message_id → raw_message
_msg_cache_lock = asyncio.Lock()
_MSG_CACHE_MAX_SIZE = 200          # 最多缓存200条，满则顶替最旧的
_is_processing = False                         # 是否正在处理
_processing_lock = asyncio.Lock()              # 保护 _is_processing
_current_bv = ""                               # 当前正在处理的 BV 号（用于排队提示）


async def _cool_down_cleanup_loop():
    """定期清理过期冷却条目，防止内存泄漏"""
    while True:
        await asyncio.sleep(COOL_DOWN_CLEANUP_INTERVAL)
        async with _cool_down_lock:
            now = time.time()
            stale = [
                k for k, t in _cool_down.items()
                if now - t >= COOL_DOWN_TIME
            ]
            for k in stale:
                del _cool_down[k]
            if stale:
                logger.debug(f"🧹 清理冷却条目：{len(stale)} 个")



# ===================== NapCat API 直调（绕过 NoneBot adapter 限制）=====================
NAPCAT_API = "http://127.0.0.1:18081"

async def napcat_call(api: str, **params) -> dict:
    """直接调用 NapCat OneBot HTTP API"""
    session = await get_http_session()
    async with session.post(f"{NAPCAT_API}/{api}", json=params) as resp:
        result = await resp.json()
        if result.get("status") == "failed" or result.get("retcode") not in (0, None):
            logger.error(f"❌ NapCat API {api} 失败: {result}")
        return result

async def napcat_send(event: MessageEvent, message: str):
    """发送消息（绕过 bot.send）"""
    is_group = event.message_type == "group"
    params = {
        "message_type": "group" if is_group else "private",
        "message": message,
    }
    if is_group:
        params["group_id"] = event.group_id
    else:
        params["user_id"] = event.user_id
    return await napcat_call("send_msg", **params)

async def napcat_get_msg(message_id: int) -> dict:
    """获取消息内容（绕过 bot.get_msg）"""
    return await napcat_call("get_msg", message_id=message_id)

# ===================== B站 WBI 签名 =====================
# player/wbi/v2 需要 WBI 签名，key 每天轮换（缓存 24h）
_WBI_MIXIN_KEY_ENC_TAB = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52
]
_wbi_mixin_key: str = ""
_wbi_key_ts: float = 0
_WBI_KEY_TTL = 24 * 3600


async def _fetch_wbi_keys() -> tuple[str, str]:
    """从 B站 nav 接口获取 img_key + sub_key"""
    session = await get_http_session()
    async with session.get("https://api.bilibili.com/x/web-interface/nav") as resp:
        data = await resp.json()
    wbi_img = data.get("data", {}).get("wbi_img", {})
    img_url = wbi_img.get("img_url", "")
    sub_url = wbi_img.get("sub_url", "")
    img_key = img_url.rsplit("/", 1)[-1].split(".")[0]
    sub_key = sub_url.rsplit("/", 1)[-1].split(".")[0]
    return img_key, sub_key


def _get_mixin_key(img_key: str, sub_key: str) -> str:
    """用固定索引表生成混音密钥"""
    raw = img_key + sub_key
    return "".join(raw[i] for i in _WBI_MIXIN_KEY_ENC_TAB)[:32]


async def get_wbi_mixin_key() -> str:
    """获取 WBI mixin_key（带缓存，24h 自动刷新）"""
    global _wbi_mixin_key, _wbi_key_ts
    now = time.time()
    if not _wbi_mixin_key or (now - _wbi_key_ts) > _WBI_KEY_TTL:
        img_key, sub_key = await _fetch_wbi_keys()
        _wbi_mixin_key = _get_mixin_key(img_key, sub_key)
        _wbi_key_ts = now
        logger.info(f"🔑 WBI 密钥已更新: {_wbi_mixin_key[:8]}...")
    return _wbi_mixin_key


def sign_wbi_url(base_url: str, params: dict, mixin_key: str) -> str:
    """对请求参数做 WBI 签名，返回完整 URL（签名和发送用同一个 query string）"""
    params["wts"] = int(time.time())
    # 按 key 排序 → URL 编码 → + 改 %20 → 小写 %xx 改大写 %XX
    q = urllib.parse.urlencode({k: params[k] for k in sorted(params.keys())})
    q = q.replace("+", "%20")
    chars = list(q)
    i = 0
    while i < len(chars):
        if chars[i] == "%" and i + 2 < len(chars):
            chars[i + 1] = chars[i + 1].upper()
            chars[i + 2] = chars[i + 2].upper()
            i += 3
        else:
            i += 1
    q = "".join(chars)
    w_rid = hashlib.md5((q + mixin_key).encode()).hexdigest()
    return f"{base_url}?{q}&w_rid={w_rid}"


# ===================== B站 API 端点 =====================
BVID_INFO_API = "https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
PLAY_URL_API = "https://api.bilibili.com/x/player/playurl"
DANMAKU_API = "https://api.bilibili.com/x/v1/dm/list.so?oid={cid}"

# ===================== B站字幕获取 =====================

async def get_bilibili_subtitle(bvid: str, cid: int) -> tuple[bool, str]:
    """获取 B站视频字幕文本（player/wbi/v2 + WBI 签名）
    返回: (True, 字幕文本) | (False, "no_subtitle"|"download_failed")
    - no_subtitle: 确认无字幕，可降级转写
    - download_failed: 有字幕但下载失败，提示用户重试
    """
    if not BILIBILI_COOKIE:
        return (False, "no_subtitle")

    # 0. 检查本地缓存（30分钟内重复BV号直接复用）
    SUBTITLE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = SUBTITLE_CACHE_DIR / f"{bvid}.txt"
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < SUBTITLE_CACHE_TTL:
            cached = cache_file.read_text(encoding="utf-8")
            logger.info(f"📺 {bvid}: 命中字幕缓存（{age:.0f}秒前）→ {len(cached)} 字符")
            return (True, cached)
        else:
            cache_file.unlink(missing_ok=True)
            logger.info(f"📺 {bvid}: 字幕缓存已过期，重新获取")

    session = await get_http_session()
    found_subtitle = False
    best_text = ""
    best_len = 0

    for full_attempt in range(1, 4):
        try:
            # 1. player/wbi/v2 + WBI 签名获取字幕列表
            mixin_key = await get_wbi_mixin_key()
            url = sign_wbi_url(
                "https://api.bilibili.com/x/player/wbi/v2",
                {"bvid": bvid, "cid": cid},
                mixin_key,
            )
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning(f"📺 {bvid}: wbi/v2 返回 {resp.status} (第{full_attempt}次)")
                    await asyncio.sleep(1)
                    continue
                data = await resp.json()

            subtitles = data.get("data", {}).get("subtitle", {}).get("subtitles", [])
            if not subtitles:
                if full_attempt == 1:
                    return (False, "no_subtitle")
                logger.info(f"📺 {bvid}: 第{full_attempt}次无字幕列表，重试...")
                await asyncio.sleep(1)
                continue

            found_subtitle = True
            # 优先 CC 字幕(type=0)，其次中文 AI 字幕(type=1, lan=ai-zh)，再次任意
            cc = [s for s in subtitles if s.get("type") == 0]
            ai = [s for s in subtitles if s.get("type") == 1]
            zh_ai = [s for s in ai if "zh" in s.get("lan", "").lower()]
            if cc:
                target = cc[0]
            elif zh_ai:
                target = zh_ai[0]
            elif ai:
                target = ai[0]
            else:
                target = subtitles[0]

            sub_url = target.get("subtitle_url", "")
            sub_type = "CC" if target.get("type") == 0 else "AI"

            if not sub_url:
                logger.warning(f"📺 {bvid}: subtitle_url 为空 (第{full_attempt}次)")
                await asyncio.sleep(1)
                continue

            # 2. 下载字幕内容
            if sub_url.startswith("//"):
                sub_url = "https:" + sub_url
            logger.info(
                f"📺 {bvid}: 发现{sub_type}字幕 ({target.get('lan_doc', '?')})"
                f" (第{full_attempt}次)"
            )
            async with session.get(sub_url) as resp2:
                if resp2.status != 200:
                    logger.warning(f"📺 {bvid}: 下载 HTTP {resp2.status}")
                    await asyncio.sleep(1)
                    continue
                sub_data = await resp2.json()

            body = sub_data.get("body", [])
            text = " ".join(item.get("content", "") for item in body)
            logger.info(f"📺 {bvid}: 下载成功 ({len(text)} 字符, {len(body)} 片段)")

            if len(text) > best_len:
                best_text = text
                best_len = len(text)

            if len(text) >= 500:
                cache_file.write_text(text, encoding="utf-8")
                return (True, text)

            logger.info(f"📺 {bvid}: 内容较短 ({len(text)} 字符)，尝试重新获取...")
            await asyncio.sleep(1)

        except Exception as e:
            logger.warning(f"📺 {bvid}: 第{full_attempt}次异常 ({type(e).__name__}: {e})")
            await asyncio.sleep(1)

    # 重试用完
    if best_text:
        logger.info(f"📺 {bvid}: 使用最佳结果 ({best_len} 字符)")
        cache_file.write_text(best_text, encoding="utf-8")
        return (True, best_text)
    if found_subtitle:
        logger.error(f"📺 {bvid}: 有字幕但全部下载失败")
        return (False, "download_failed")
    return (False, "no_subtitle")


# ===================== 核心辅助函数 =====================
def get_raw_message_from_event(event: MessageEvent) -> str:
    """提取原始消息（保留 CQ 码，用于解析引用/@等）"""
    if hasattr(event, "_json"):
        raw_data = event._json
        if isinstance(raw_data, str):
            raw_data = json.loads(raw_data)
        return raw_data.get("raw_message", "") or raw_data.get("message", "")
    return event.model_dump().get("raw_message", "") or event.model_dump().get("message", "")


async def resolve_b23_short_url(short_url: str) -> str:
    """解析 b23.tv 短链接 → 完整 URL"""
    try:
        short_url = short_url.replace("&amp;", "&").replace("\\/", "/").strip()
        if not short_url.startswith(("http://", "https://")):
            short_url = f"https://{short_url}"

        session = await get_http_session()
        async with session.head(short_url, allow_redirects=True) as resp:
            logger.info(f"✅ b23 短链接跳转：{short_url} → {resp.url}")
            return str(resp.url)
    except Exception as e:
        logger.error(f"❌ 解析 b23 短链接失败：{e}")
        return short_url


async def extract_bv_from_bilibili_miniprogram(cq_json_raw: str) -> str:
    """从 QQ 小程序卡片提取 BV 号（兼容 Android [CQ:json,...] 和 iOS [json:data=...] 两种格式）"""
    try:
        # Android 格式：[CQ:json,data={...}]
        json_match = re.search(r"\[CQ:json,data=(.*?)\]", cq_json_raw, re.DOTALL)
        if not json_match:
            # iOS 格式：[json:data={...}]（含换行符，需 DOTALL）
            json_match = re.search(r"\[json:data=(.*)\]", cq_json_raw, re.DOTALL)
        if not json_match:
            return ""

        json_str = json_match.group(1)
        json_str = json_str.replace("&#44;", ",").replace("&amp;", "&")
        json_str = json_str.replace('\\"', '"').replace("\\/", "/")

        data = json.loads(json_str)
        logger.info(f"📱 小程序 JSON: {json_str[:200]}...")

        # Android 小程序格式：meta.detail_1.qqdocurl → b23.tv 短链接
        qqdocurl = data.get("meta", {}).get("detail_1", {}).get("qqdocurl", "")
        if qqdocurl and "b23.tv" in qqdocurl:
            real_url = await resolve_b23_short_url(qqdocurl)
            bv_match = re.search(r"BV([0-9a-zA-Z]{10})", real_url)
            if bv_match:
                bv_code = f"BV{bv_match.group(1)}"
                logger.info(f"✅ 从小程序提取 BV 号：{bv_code}")
                return bv_code

        # iOS 格式或其他格式：直接在 JSON 里搜 URL 或 BV 号
        json_dump = json.dumps(data, ensure_ascii=False)
        bv_match = re.search(r"BV([0-9a-zA-Z]{10})", json_dump)
        if bv_match:
            bv_code = f"BV{bv_match.group(1)}"
            logger.info(f"✅ 从小程序 JSON 提取 BV 号：{bv_code}")
            return bv_code

        # 搜 b23.tv 或 bilibili.com 链接
        url_match = re.search(r"https?://(?:b23\.tv|www\.bilibili\.com)/\S+", json_dump)
        if url_match:
            url = url_match.group(0).rstrip('"\\')
            if "b23.tv" in url:
                real_url = await resolve_b23_short_url(url)
            else:
                real_url = url
            bv_match = re.search(r"BV([0-9a-zA-Z]{10})", real_url)
            if bv_match:
                bv_code = f"BV{bv_match.group(1)}"
                logger.info(f"✅ 从小程序 JSON 链接提取 BV 号：{bv_code}")
                return bv_code

        return ""
    except Exception as e:
        logger.error(f"❌ 解析小程序失败：{e}")
        return ""


def extract_bv_and_page(msg: str) -> tuple[str, int]:
    """从文本提取 BV 号 + 分 P 序号"""
    bv_match = re.search(r"BV([0-9a-zA-Z]{10})", msg)
    bv_code = f"BV{bv_match.group(1)}" if bv_match else ""

    page_match = re.search(r"BV[0-9a-zA-Z]{10}\s*(\d+)", msg)
    page_index = int(page_match.group(1)) if page_match else 1

    return bv_code, page_index


def safe_sender_name(event: MessageEvent) -> str:
    """安全获取发送者昵称"""
    try:
        sender = event.sender
        if sender and hasattr(sender, "nickname"):
            return sender.nickname or "未知用户"
        return "未知用户"
    except Exception:
        return "未知用户"


# ===================== 核心音频函数 =====================
async def get_video_info_by_api(bvid: str) -> dict:
    """获取视频信息（标题、简介、cid、分P）"""
    try:
        session = await get_http_session()
        async with session.get(BVID_INFO_API.format(bvid=bvid)) as resp:
            resp.raise_for_status()
            data = await resp.json()

        if data["code"] != 0:
            return {"success": False, "msg": f"B站接口错误：{data['message']}"}

        info = data["data"]
        title = info.get("title", "无标题")
        desc = (info.get("desc", "") or "").strip() or "无简介"
        cid = info.get("cid", 0)
        pages = info.get("pages", [])
        duration = info.get("duration", 0)  # 视频时长（秒）

        target_cid = cid
        part_title = ""
        if pages and len(pages) > 0:
            first_page = pages[0]
            target_cid = first_page.get("cid", cid)
            part_title = first_page.get("part", "")

        final_title = f"{title} - {part_title}" if part_title else title
        return {
            "success": True,
            "title": final_title,
            "desc": desc,
            "cid": target_cid,
            "pages": pages,
            "duration": duration,  # 视频时长（秒）
        }
    except asyncio.TimeoutError:
        return {"success": False, "msg": "获取视频信息超时"}
    except Exception as e:
        return {"success": False, "msg": f"获取视频信息失败：{str(e)[:80]}"}


async def get_audio_url_by_api(bvid: str, cid: int) -> str:
    """获取 DASH 音频流 URL（fnval=16 强制仅音频）"""
    params = {
        "bvid": bvid,
        "cid": cid,
        "fnval": 16,
        "fnver": 0,
        "fourk": 0,
        "platform": "web",
        "high_quality": 1,
    }
    try:
        session = await get_http_session()
        async with session.get(PLAY_URL_API, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        if data["code"] != 0:
            logger.error(f"【错误】B站音频接口：{data['message']}（cid={cid}）")
            return ""

        dash = data["data"].get("dash", {})
        audio_streams = dash.get("audio", [])
        if not audio_streams:
            logger.warning(f"【警告】无音频流（cid={cid}），可能为版权视频")
            return ""

        audio_streams.sort(key=lambda x: x.get("bandwidth", 0), reverse=True)
        url = audio_streams[0].get("baseUrl", "")
        if url and "?" not in url:
            url += f"?cid={cid}&bvid={bvid}"

        logger.info(f"✅ 获取音频链接成功：{url[:50]}...")
        return url
    except asyncio.TimeoutError:
        logger.error(f"【错误】获取音频地址超时（cid={cid}）")
        return ""
    except Exception as e:
        logger.error(f"【错误】获取音频地址失败：{e}")
        return ""


async def extract_audio(audio_url: str, save_path: str) -> bool:
    """FFmpeg 下载音频流 → 16kHz 单声道 WAV"""
    if not audio_url:
        return False

    header_str = "\r\n".join(f"{k}: {v}" for k, v in HEADERS.items())
    ffmpeg_cmd = [
        "ffmpeg", "-y",
        "-headers", header_str,
        "-i", audio_url,
        "-vn",
        "-acodec", "pcm_s16le",
        "-ar", "16000",
        "-ac", "1",
        "-f", "wav",
        save_path,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=AUDIO_DOWNLOAD_TIMEOUT
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            logger.error("【错误】FFmpeg 下载音频超时")
            return False

        if proc.returncode != 0:
            logger.error(
                f"【错误】FFmpeg 失败：{stderr.decode('utf-8', errors='ignore')[:200]}"
            )
            return False

        size_kb = os.path.getsize(save_path) / 1024 if os.path.exists(save_path) else 0
        if size_kb > 1:
            logger.info(f"✅ 音频提取成功：{size_kb:.1f} KB")
            return True
        else:
            logger.error("【错误】音频文件无效（太小或不存在）")
            return False
    except FileNotFoundError:
        logger.error("【错误】未找到 FFmpeg，请安装并添加到 PATH")
        return False
    except Exception as e:
        logger.error(f"【错误】音频提取异常：{e}")
        return False


async def audio_to_subtitle(audio_path: str, model_name: str = "base") -> tuple[str, float]:
    """whisper.cpp 转写音频 → (字幕文本, 耗时秒数)

    model_name: "tiny" = ggml-tiny.bin（快速）, "base" = ggml-base.bin（精细）
    CPU 推理比 openai-whisper 快 3-5x
    """
    t0 = time.perf_counter()
    model_path = WHISPER_MODEL_PATH if model_name == "base" else WHISPER_FAST_MODEL_PATH
    output_prefix = audio_path.rsplit(".", 1)[0] + f"_{model_name}"

    cmd = [
        WHISPER_CLI,
        "-m", model_path,
        "-f", audio_path,
        "-l", "zh",
        "-otxt",
        "-of", output_prefix,
        "--no-timestamps",
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=WHISPER_TIMEOUT
        )

        txt_file = output_prefix + ".txt"
        if os.path.exists(txt_file):
            with open(txt_file, "r", encoding="utf-8") as f:
                text = f.read().strip()
            try:
                os.remove(txt_file)
            except Exception:
                pass
        else:
            text = ""
            logger.error(f"whisper.cpp 无输出：{stderr.decode('utf-8', errors='ignore')[:200]}")

        elapsed = time.perf_counter() - t0

        if text:
            # 粗略估算音频时长（基于文件大小，16kHz 16bit 单声道 ≈ 32KB/s）
            audio_size_mb = os.path.getsize(audio_path) / (1024 * 1024) if os.path.exists(audio_path) else 0
            duration_min = audio_size_mb / 1.92  # 32KB/s * 60 = 1.92 MB/min
            speedup = (duration_min * 60) / elapsed if elapsed > 0 else 0
            logger.info(
                f"whisper.cpp ({model_name}): {len(text)} chars, "
                f"~{duration_min:.1f}min audio -> {elapsed:.1f}s ({speedup:.1f}x realtime)"
            )

        return text if text else "audio transcribe failed", elapsed

    except asyncio.TimeoutError:
        logger.error(f"whisper.cpp timeout ({WHISPER_TIMEOUT}s)")
        return f"transcribe timeout ({WHISPER_TIMEOUT}s)", 0.0
    except Exception as e:
        logger.error(f"whisper.cpp failed: {e}")
        return f"transcribe failed: {str(e)[:50]}", 0.0


async def get_danmaku_by_api(cid: int) -> str:
    """获取视频弹幕（XML 解析，去重取前 50 条）"""
    try:
        session = await get_http_session()
        async with session.get(DANMAKU_API.format(cid=cid)) as resp:
            text = await resp.text(encoding="utf-8")
        dm_list = re.findall(r"<d[^>]*>(.*?)</d>", text, re.DOTALL)
        dm_list = list(set(dm.strip() for dm in dm_list if dm.strip()))[:50]
        return "\n".join(dm_list) if dm_list else "无弹幕"
    except Exception as e:
        logger.error(f"【错误】获取弹幕失败：{e}")
        return "弹幕获取失败"


async def llm_summarize(
    title: str, desc: str, danmaku: str, subtitle: str
) -> str:
    """DeepSeek API 总结视频内容（OpenAI 兼容格式）"""
    if not DASHSCOPE_API_KEY or not DASHSCOPE_API_KEY.strip():
        return "❌ DeepSeek API Key 未配置！"

    # 硬截断短字段（元数据），字幕不截断
    title = title[:MAX_TITLE_LEN]
    desc = desc[:MAX_DESC_LEN]
    danmaku = danmaku[:MAX_DANMAKU_LEN]

    prompt = (
        "请总结以下B站视频的核心内容，要求：\n"
        "1. 严格基于提供的文本进行总结，不要编造或推测文本中未出现的观点和信息；\n"
        "2. 字数控制在500字以内；\n"
        "3. 突出视频的核心观点/主要内容；\n"
        "4. 如果字幕中存在明显错误或不通顺的地方，请根据上下文合理推断，但不要偏离原文意思；\n"
        "5. 如果字幕内容与视频标题明显不相关，请直接说明不匹配，不要强行总结。\n\n"
        f"【视频标题】：{title}\n"
        f"【视频简介】：{desc}\n"
        f"【弹幕核心】：{danmaku}\n"
        f"【字幕内容】：{subtitle}\n"
        f"\n（注：字幕内容由多个字幕片段拼接而成，无标点符号，请根据语义自行断句理解）\n"
    )

    # 构建 OpenAI 兼容的请求体
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "top_p": 0.9,
        "max_tokens": 1024,
    }

    try:
        session = await get_http_session()
        async with session.post(
            DEEPSEEK_API_URL,
            json=payload,
            headers={"Authorization": f"Bearer {DASHSCOPE_API_KEY}"},
            timeout=aiohttp.ClientTimeout(total=LLM_TIMEOUT),
        ) as resp:
            data = await resp.json()

            # 检查 HTTP 状态码
            if resp.status != 200:
                error_msg = data.get("error", {}).get("message", "") or data.get("message", "") or str(data)[:300]
                logger.error(f"❌ DeepSeek API 返回错误（{resp.status}）：{error_msg}")
                if resp.status == 401:
                    return "❌ DeepSeek API Key 无效！请检查 .env 配置"
                if resp.status == 429:
                    return "❌ DeepSeek API 额度不足或限流！"
                return f"❌ API 错误（{resp.status}）：{error_msg[:100]}"

            # 解析 OpenAI 兼容格式：choices[0].message.content
            choices = data.get("choices", [])
            if choices:
                content = choices[0].get("message", {}).get("content", "")
                if content:
                    logger.info(f"✅ LLM 总结成功（{len(content)} 字符, model={data.get('model', '?')}）")
                    return content.strip()

            # 格式异常：记录详细信息
            logger.error(f"❌ LLM 返回格式异常：status={resp.status}, data={str(data)[:500]}")
            return "❌ 总结生成失败：返回格式异常"

    except asyncio.TimeoutError:
        return "❌ DeepSeek API 调用超时！请稍后重试"
    except UnicodeEncodeError as e:
        logger.error(f"【错误】编码错误：{e}")
        return "❌ 总结编码失败"
    except Exception as e:
        return f"❌ 总结生成失败：{str(e)[:60]}"


# ===================== 核心消息处理器 =====================
async def handle_bilibili_analysis(event: MessageEvent, bot: Bot):
    """整合版消息处理器"""
    global _is_processing, _current_bv
    self_id = str(event.self_id)
    message_type = event.message_type
    is_group = message_type == "group"
    user_id = str(event.user_id)
    group_id = str(event.group_id) if is_group else ""

    logger.info(
        f"\n{'='*50}\n"
        f"🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"📱 {message_type} | 🧑 {user_id} | 👥 {group_id}"
    )

    # ---- 缓存视频分享消息（iOS 的 get_msg 不可用，主动缓存做兜底）----
    raw_for_cache = get_raw_message_from_event(event)
    if '[json:data=' in raw_for_cache or '[CQ:json,data=' in raw_for_cache or 'b23.tv' in raw_for_cache or 'BV' in raw_for_cache:
        async with _msg_cache_lock:
            _msg_cache[event.message_id] = raw_for_cache
            while len(_msg_cache) > _MSG_CACHE_MAX_SIZE:
                del _msg_cache[next(iter(_msg_cache))]

    # ---- 群聊规则：@机器人即可，BV号优先从引用消息中找 ----
    if is_group:
        is_at_me = any(
            seg.type == "at" and seg.data.get("qq") == self_id
            for seg in event.message
        )
        # fallback: 用正则从原始消息解析（兼容 NapCat 的 [at:qq=] 格式）
        if not is_at_me:
            at_re = re.search(r"\[CQ:at,qq=(\d+)\]|\[at:qq=(\d+)\]", raw_for_cache)
            if at_re:
                at_qq = at_re.group(1) or at_re.group(2)
                is_at_me = (at_qq == self_id)
        if not is_at_me:
            logger.info("❌ 未 @机器人，忽略")
            return

    # ---- 冷却机制 ----
    cool_key = group_id if is_group else user_id
    async with _cool_down_lock:
        now = time.time()
        if cool_key in _cool_down and (now - _cool_down[cool_key]) < COOL_DOWN_TIME:
            remain = int(COOL_DOWN_TIME - (now - _cool_down[cool_key]))
            await napcat_send(event, f"⏳ 冷却中！请 {remain} 秒后再试~")
            return
        _cool_down[cool_key] = now

    # ---- 排队机制 ----
    async with _processing_lock:
        if _is_processing:
            user_id_str = str(event.user_id)
            already_queued = any(
                t["user_id"] == user_id_str
                for t in list(_task_queue._queue)
            )
            if already_queued:
                await napcat_send(event, "⏳ 你已经在排队中了，请耐心等待~")
                return
            pos = _task_queue.qsize() + 1
            await _task_queue.put({
                "event": event, "bot": bot, "user_id": user_id_str,
            })
            current_info = f" {_current_bv}" if _current_bv else ""
            await napcat_send(event,
                f"⏳ 排队中！\n"
                f"📹 正在处理：{current_info}\n"
                f"🔢 你排在第 {pos} 位，请耐心等待~"
            )
            return
        _is_processing = True

    try:
        await _do_analyze(event, bot)
    finally:
        async with _processing_lock:
            _is_processing = False
            _current_bv = ""
        if not _task_queue.empty():
            next_task = await _task_queue.get()
            await napcat_send(next_task["event"], "🎉 轮到你了！正在开始处理...")
            asyncio.create_task(handle_bilibili_analysis(
                next_task["event"], next_task["bot"]
            ))


async def _do_analyze(event: MessageEvent, bot: Bot):
    """实际 B站视频处理逻辑
    策略：字幕优先 → 无字幕则 whisper.cpp tiny 转写
    """
    global _current_bv
    self_id = str(event.self_id)
    message_type = event.message_type
    is_group = message_type == "group"
    user_id = str(event.user_id)
    group_id = str(event.group_id) if is_group else ""

    # ---- 解析 BV 号（引用消息优先 → 消息文本兜底）----
    bv_code = ""
    page_index = 1

    if is_group:
        raw_msg = get_raw_message_from_event(event)

        # 先尝试从引用消息中找
        reply_match = re.search(r"\[CQ:reply,id=(\d+)\]|\[reply:id=(\d+)\]", raw_msg)
        reply_id = int(reply_match.group(1) or reply_match.group(2)) if reply_match else 0
        if reply_id:
            # 优先查本地缓存（iOS 分享消息 get_msg 不可用）
            async with _msg_cache_lock:
                cached = _msg_cache.get(reply_id, "")
            if cached:
                quoted_raw = cached
                logger.info(f"📎 从消息缓存中找到引用内容")
            else:
                try:
                    quoted_msg = await napcat_get_msg(message_id=reply_id)
                    data = quoted_msg.get("data", {}) or {}
                    quoted_raw = str(data.get("raw_message", "") or data.get("message", "") or "")
                except Exception as e:
                    quoted_raw = ""
                    logger.warning(f"⚠️ 获取引用消息失败: {e}")
            if quoted_raw:
                bv_code = await extract_bv_from_bilibili_miniprogram(quoted_raw)
                if not bv_code:
                    bv_code, page_index = extract_bv_and_page(quoted_raw)
                if bv_code:
                    logger.info(f"📎 从引用消息中找到: {bv_code}")

        # 引用没找到 → 尝试从事件原始 JSON 提取（涵盖 get_msg 失败的情况）
        if not bv_code:
            event_raw = get_raw_message_from_event(event)
            bv_code = await extract_bv_from_bilibili_miniprogram(event_raw)
            if bv_code:
                logger.info(f"📎 从事件消息中提取: {bv_code}")

        # 还没找到 → 纯文本正则匹配
        if not bv_code:
            bv_code, page_index = extract_bv_and_page(raw_msg)
            if bv_code:
                logger.info(f"💬 从消息文本中找到: {bv_code}")
    else:
        bv_code, page_index = extract_bv_and_page(event.get_plaintext().strip())

    if not bv_code:
        tip = (
            "⚠️ 引用的消息中未找到B站视频信息！"
            if is_group
            else "⚠️ 未找到B站视频信息！请发送B站链接/BV号/小程序~"
        )
        await napcat_send(event, tip)
        return

    _current_bv = bv_code
    tip_prefix = f"@{safe_sender_name(event)} \n" if is_group else ""

    # ---- 检查是否强制转写（消息含"转写"关键词）----
    force_transcribe = False
    if is_group:
        raw_check = get_raw_message_from_event(event)
        # 也检查引用消息
        reply_match = re.search(r"\[CQ:reply,id=(\d+)\]|\[reply:id=(\d+)\]", raw_check)
        if reply_match:
            try:
                qm = await napcat_get_msg(message_id=int(reply_match.group(1) or reply_match.group(2)))
                qdata = qm.get("data", {}) or {}
                qraw = str(qdata.get("raw_message", "") or qdata.get("message", "") or "")
                if "转写" in qraw:
                    force_transcribe = True
            except Exception:
                pass
        if "转写" in raw_check:
            force_transcribe = True
    elif "转写" in event.get_plaintext():
        force_transcribe = True

    if force_transcribe:
        logger.info(f"🔧 {bv_code}: 强制转写模式")
        # 跳过字幕检查，直接跳到转写
        subtitle_text = None
    else:
        # ---- 第1步：获取视频信息 + 检查字幕 ----
        video_info = await get_video_info_by_api(bv_code)
        if not video_info["success"]:
            await napcat_send(event, f"❌ 处理失败：{video_info['msg']}")
            return

        duration = video_info.get("duration", 0)
        subtitle_ok, subtitle_text = await get_bilibili_subtitle(bv_code, video_info["cid"])

        if subtitle_ok:
            # ---- 有字幕：直接总结 ----
            await napcat_send(event, (
                f"{tip_prefix}🎬 正在分析 {bv_code}（第{page_index}分P），预计半分钟内完成..."
            ))
            danmaku_text = await get_danmaku_by_api(video_info["cid"])
            summary = await llm_summarize(
                title=video_info["title"], desc=video_info["desc"],
                danmaku=danmaku_text, subtitle=subtitle_text,
            )
            lines = [
                f"📺 {video_info['title']}",
                "",
                summary,
                "",
                f"—— 字幕+弹幕+简介  ·  {bv_code}",
            ]
            await napcat_send(event, f"{tip_prefix}{chr(10).join(lines)}")
            logger.info(f"✅ 字幕通道完成：{bv_code}")
            return

        if subtitle_text == "download_failed":
            # 有字幕但下载失败 → 提示用户
            await napcat_send(event,
                f"{tip_prefix}❌ {bv_code} 检测到字幕但下载失败，请稍后重试"
            )
            return

        # subtitle_text is None: 确认无字幕，降级转写
        # 计算预估时间
        transcribe_sec = duration / 2.7 if duration > 0 else 0
        total_sec = 10 + transcribe_sec + 20
        est_min = max(1, round(total_sec / 60))
        est_str = f"约 {est_min} 分钟" if est_min <= 1 else f"约 {est_min} 分钟"

        await napcat_send(event, (
            f"{tip_prefix}🎬 {bv_code}（第{page_index}分P）\n"
            f"📺 此视频无字幕，需语音转写，预计 {est_str}..."
        ))

    # ---- 转写路径（无字幕 / 强制转写）----
    if not force_transcribe:
        # 非强制转写时 video_info 已经获取过了；强制转写需要重新获取
        pass
    else:
        video_info = await get_video_info_by_api(bv_code)
        if not video_info["success"]:
            await napcat_send(event, f"❌ 处理失败：{video_info['msg']}")
            return
        await napcat_send(event, (
            f"{tip_prefix}🎬 {bv_code}（第{page_index}分P）\n"
            f"🔧 强制转写模式，预计需要数分钟..."
        ))

    # ---- 第4步：下载音频 + 转写 + 总结 ----
    audio_url = await get_audio_url_by_api(bv_code, video_info["cid"])
    logger.info(f"【调试】BV:{bv_code} CID:{video_info['cid']} audio:{audio_url[:60] if audio_url else '(无)'}...")

    audio_file = SAVE_DIR / f"{bv_code}_page{page_index}_temp.wav"
    has_audio = bool(audio_url and await extract_audio(audio_url, str(audio_file)))

    if not has_audio:
        # 降级：基于文本信息总结
        await napcat_send(event, f"{tip_prefix}⚠️ 音频提取失败，仅基于标题+简介+弹幕总结")
        danmaku_text = await get_danmaku_by_api(video_info["cid"])
        summary_text = await llm_summarize(
            title=video_info["title"], desc=video_info["desc"],
            danmaku=danmaku_text, subtitle="无音频",
        )
        lines = [
            f"📺 {video_info['title']}",
            "",
            summary_text,
            "",
            f"—— 弹幕+简介（无音频）  ·  {bv_code}",
        ]
        await napcat_send(event, f"{tip_prefix}{chr(10).join(lines)}")
        return

    audio_path = str(audio_file)
    danmaku_text = await get_danmaku_by_api(video_info["cid"])

    # whisper.cpp tiny 转写
    sub_text, sub_time = await audio_to_subtitle(audio_path, "tiny")

    if sub_text and not sub_text.startswith("transcribe") and sub_text != "audio transcribe failed":
        summary = await llm_summarize(
            title=video_info["title"], desc=video_info["desc"],
            danmaku=danmaku_text, subtitle=sub_text,
        )
        lines = [
            f"📺 {video_info['title']}",
            "",
            summary,
            "",
            f"—— 语音转写+弹幕+简介  ·  {bv_code}  ·  转写{sub_time:.0f}秒",
        ]
        await napcat_send(event, f"{tip_prefix}{chr(10).join(lines)}")
    else:
        await napcat_send(event, f"{tip_prefix}❌ 语音转写失败，请稍后重试")

    # ---- 延迟清理临时文件 ----
    async def cleanup_later():
        await asyncio.sleep(900)  # 15分钟后删除
        if os.path.exists(audio_path):
            try:
                os.remove(audio_path)
                logger.info(f"🧹 临时文件已清理：{audio_file}")
            except Exception as e:
                logger.error(f"❌ 删除临时文件失败：{e}")

    asyncio.create_task(cleanup_later())


# ===================== 生命周期管理 =====================
@driver.on_startup
async def on_startup():
    """启动时初始化 HTTP 会话 + 启动冷却清理任务"""
    global _cool_down_cleanup_task
    SAVE_DIR.mkdir(exist_ok=True)
    await get_http_session()
    _cool_down_cleanup_task = asyncio.create_task(_cool_down_cleanup_loop())
    logger.success("🚀 B站视频分析机器人已启动")


@driver.on_shutdown
async def on_shutdown():
    """关闭时清理资源"""
    global _cool_down_cleanup_task
    # 取消冷却清理任务
    if _cool_down_cleanup_task:
        _cool_down_cleanup_task.cancel()
        try:
            await _cool_down_cleanup_task
        except asyncio.CancelledError:
            pass

    # 关闭 HTTP 会话
    await close_http_session()

    # 清理临时目录
    import shutil
    if SAVE_DIR.exists():
        shutil.rmtree(SAVE_DIR, ignore_errors=True)
        logger.info("🧹 临时目录已清理")

    logger.success("👋 B站视频分析机器人已下线")


# ===================== 注册处理器 + 启动 =====================
bilibili_analysis_matcher = on_message(rule=Rule(), priority=5, block=True)
bilibili_analysis_matcher.append_handler(handle_bilibili_analysis)

if __name__ == "__main__":
    nonebot.run()
