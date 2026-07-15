# -*- coding: utf-8 -*-
"""
B站视频分析 - 核心工具函数
从 server_bot.py 抽取，不依赖任何机器人框架（NoneBot/AstrBot）
纯 async HTTP + subprocess 调用，可在任何 Python 项目中复用

依赖：aiohttp, urllib3, python-dotenv, dashscope（仅用于旧的 qwen-turbo 分支，可选）
"""

import re
import json
import time
import os
import ssl
import hashlib
import urllib.parse
import asyncio
import logging
from pathlib import Path

import urllib3
import aiohttp
from dotenv import load_dotenv

logger = logging.getLogger("bilibili_utils")

# ===================== 环境配置 =====================
load_dotenv()
DASHSCOPE_API_KEY = os.getenv("DEEPSEEK_API_KEY", os.getenv("DASHSCOPE_API_KEY", ""))
DEEPSEEK_API_URL = os.getenv("DEEPSEEK_API_URL", "https://api.deepseek.com/v1/chat/completions")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")
BILIBILI_COOKIE = os.getenv("BILIBILI_COOKIE", "")

# ===================== 可调参数 =====================
WHISPER_CLI = os.getenv("WHISPER_CLI", "/root/whisper.cpp/build/bin/whisper-cli")
WHISPER_MODEL_PATH = os.getenv("WHISPER_MODEL_PATH", "/root/whisper.cpp/models/ggml-base.bin")
WHISPER_FAST_MODEL_PATH = os.getenv("WHISPER_FAST_MODEL_PATH", "/root/whisper.cpp/models/ggml-tiny.bin")

AUDIO_DOWNLOAD_TIMEOUT = 60
WHISPER_TIMEOUT = 600
LLM_TIMEOUT = 60
HTTP_TIMEOUT = 15

SAVE_DIR = Path(os.getenv("BILIBILI_TEMP_DIR", "./bilibili_temp"))
SUBTITLE_CACHE_DIR = SAVE_DIR / "subtitle_cache"
SUBTITLE_CACHE_TTL = 259200  # 3 天

MAX_TITLE_LEN = 200
MAX_DESC_LEN = 800
MAX_DANMAKU_LEN = 800

# ===================== SSL =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl._create_default_https_context = ssl._create_unverified_context

# ===================== HTTP 请求头 =====================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bilibili.com/",
    "Origin": "https://www.bilibili.com",
    "Cookie": BILIBILI_COOKIE,
}

# ===================== 全局 HTTP 会话 =====================
_http_session: aiohttp.ClientSession | None = None
_http_session_lock = asyncio.Lock()


async def get_http_session() -> aiohttp.ClientSession:
    """获取或创建复用的 HTTP 会话"""
    global _http_session
    if _http_session is None:
        async with _http_session_lock:
            if _http_session is None:
                _http_session = aiohttp.ClientSession(
                    headers=HEADERS,
                    connector=aiohttp.TCPConnector(ssl=False),
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                )
    return _http_session


async def close_http_session():
    """关闭 HTTP 会话"""
    global _http_session
    if _http_session:
        await _http_session.close()
        _http_session = None
        logger.info("HTTP 会话已关闭")


def update_cookie(new_cookie: str):
    """动态更新 B站 Cookie（用于 AstrBot WebUI 配置变更后）"""
    global BILIBILI_COOKIE
    BILIBILI_COOKIE = new_cookie
    HEADERS["Cookie"] = new_cookie


def update_deepseek_config(api_key: str = "", api_url: str = "", model: str = ""):
    """动态更新 DeepSeek 配置"""
    global DASHSCOPE_API_KEY, DEEPSEEK_API_URL, DEEPSEEK_MODEL
    if api_key:
        DASHSCOPE_API_KEY = api_key
    if api_url:
        DEEPSEEK_API_URL = api_url
    if model:
        DEEPSEEK_MODEL = model


# ===================== B站 API 端点 =====================
BVID_INFO_API = "https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
PLAY_URL_API = "https://api.bilibili.com/x/player/playurl"
DANMAKU_API = "https://api.bilibili.com/x/v1/dm/list.so?oid={cid}"

# ===================== WBI 签名 =====================
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
        logger.info(f"WBI 密钥已更新: {_wbi_mixin_key[:8]}...")
    return _wbi_mixin_key


def sign_wbi_url(base_url: str, params: dict, mixin_key: str) -> str:
    """对请求参数做 WBI 签名，返回完整 URL"""
    params["wts"] = int(time.time())
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


# ===================== B站 视频信息 / 音频 =====================

async def get_video_info_by_api(bvid: str) -> dict:
    """获取视频信息（标题、简介、cid、分P、时长）"""
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
        duration = info.get("duration", 0)

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
            "duration": duration,
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
            logger.error(f"B站音频接口错误：{data['message']}（cid={cid}）")
            return ""

        dash = data["data"].get("dash", {})
        audio_streams = dash.get("audio", [])
        if not audio_streams:
            logger.warning(f"无音频流（cid={cid}），可能为版权视频")
            return ""

        audio_streams.sort(key=lambda x: x.get("bandwidth", 0), reverse=True)
        url = audio_streams[0].get("baseUrl", "")
        if url and "?" not in url:
            url += f"?cid={cid}&bvid={bvid}"

        logger.info(f"获取音频链接成功：{url[:50]}...")
        return url
    except asyncio.TimeoutError:
        logger.error(f"获取音频地址超时（cid={cid}）")
        return ""
    except Exception as e:
        logger.error(f"获取音频地址失败：{e}")
        return ""


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
        logger.error(f"获取弹幕失败：{e}")
        return "弹幕获取失败"


# ===================== B站字幕获取 =====================

async def get_bilibili_subtitle(bvid: str, cid: int) -> tuple[bool, str]:
    """获取 B站视频字幕文本（player/wbi/v2 + WBI 签名）

    返回: (True, 字幕文本) | (False, "no_subtitle"|"download_failed")
    - no_subtitle: 确认无字幕，可降级转写
    - download_failed: 有字幕但下载失败，提示用户重试
    """
    if not BILIBILI_COOKIE:
        return (False, "no_subtitle")

    # 检查本地缓存
    SUBTITLE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = SUBTITLE_CACHE_DIR / f"{bvid}.txt"
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < SUBTITLE_CACHE_TTL:
            cached = cache_file.read_text(encoding="utf-8")
            logger.info(f"{bvid}: 命中字幕缓存（{age:.0f}秒前）→ {len(cached)} 字符")
            return (True, cached)
        else:
            cache_file.unlink(missing_ok=True)
            logger.info(f"{bvid}: 字幕缓存已过期，重新获取")

    session = await get_http_session()
    found_subtitle = False
    best_text = ""
    best_len = 0

    for full_attempt in range(1, 4):
        try:
            mixin_key = await get_wbi_mixin_key()
            url = sign_wbi_url(
                "https://api.bilibili.com/x/player/wbi/v2",
                {"bvid": bvid, "cid": cid},
                mixin_key,
            )
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning(f"{bvid}: wbi/v2 返回 {resp.status} (第{full_attempt}次)")
                    await asyncio.sleep(1)
                    continue
                data = await resp.json()

            subtitles = data.get("data", {}).get("subtitle", {}).get("subtitles", [])
            if not subtitles:
                if full_attempt == 1:
                    return (False, "no_subtitle")
                logger.info(f"{bvid}: 第{full_attempt}次无字幕列表，重试...")
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
                logger.warning(f"{bvid}: subtitle_url 为空 (第{full_attempt}次)")
                await asyncio.sleep(1)
                continue

            if sub_url.startswith("//"):
                sub_url = "https:" + sub_url
            logger.info(
                f"{bvid}: 发现{sub_type}字幕 ({target.get('lan_doc', '?')})"
                f" (第{full_attempt}次)"
            )
            async with session.get(sub_url) as resp2:
                if resp2.status != 200:
                    logger.warning(f"{bvid}: 下载 HTTP {resp2.status}")
                    await asyncio.sleep(1)
                    continue
                sub_data = await resp2.json()

            body = sub_data.get("body", [])
            text = " ".join(item.get("content", "") for item in body)
            logger.info(f"{bvid}: 下载成功 ({len(text)} 字符, {len(body)} 片段)")

            if len(text) > best_len:
                best_text = text
                best_len = len(text)

            if len(text) >= 500:
                cache_file.write_text(text, encoding="utf-8")
                return (True, text)

            logger.info(f"{bvid}: 内容较短 ({len(text)} 字符)，尝试重新获取...")
            await asyncio.sleep(1)

        except Exception as e:
            logger.warning(f"{bvid}: 第{full_attempt}次异常 ({type(e).__name__}: {e})")
            await asyncio.sleep(1)

    if best_text:
        logger.info(f"{bvid}: 使用最佳结果 ({best_len} 字符)")
        cache_file.write_text(best_text, encoding="utf-8")
        return (True, best_text)
    if found_subtitle:
        logger.error(f"{bvid}: 有字幕但全部下载失败")
        return (False, "download_failed")
    return (False, "no_subtitle")


# ===================== FFmpeg 音频提取 =====================

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
            logger.error("FFmpeg 下载音频超时")
            return False

        if proc.returncode != 0:
            logger.error(
                f"FFmpeg 失败：{stderr.decode('utf-8', errors='ignore')[:200]}"
            )
            return False

        size_kb = os.path.getsize(save_path) / 1024 if os.path.exists(save_path) else 0
        if size_kb > 1:
            logger.info(f"音频提取成功：{size_kb:.1f} KB")
            return True
        else:
            logger.error("音频文件无效（太小或不存在）")
            return False
    except FileNotFoundError:
        logger.error("未找到 FFmpeg，请安装并添加到 PATH")
        return False
    except Exception as e:
        logger.error(f"音频提取异常：{e}")
        return False


# ===================== whisper.cpp 转写 =====================

async def audio_to_subtitle(audio_path: str, model_name: str = "tiny") -> tuple[str, float]:
    """whisper.cpp 转写音频 → (字幕文本, 耗时秒数)

    model_name: "tiny" = ggml-tiny.bin（快速）, "base" = ggml-base.bin（精细）
    CPU 推理，不消耗 API token
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
            audio_size_mb = os.path.getsize(audio_path) / (1024 * 1024) if os.path.exists(audio_path) else 0
            duration_min = audio_size_mb / 1.92
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


# ===================== LLM 总结（DeepSeek API）=====================

async def llm_summarize(title: str, desc: str, danmaku: str, subtitle: str) -> str:
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

            if resp.status != 200:
                error_msg = data.get("error", {}).get("message", "") or data.get("message", "") or str(data)[:300]
                logger.error(f"DeepSeek API 返回错误（{resp.status}）：{error_msg}")
                if resp.status == 401:
                    return "❌ DeepSeek API Key 无效！请检查配置"
                if resp.status == 429:
                    return "❌ DeepSeek API 额度不足或限流！"
                return f"❌ API 错误（{resp.status}）：{error_msg[:100]}"

            choices = data.get("choices", [])
            if choices:
                content = choices[0].get("message", {}).get("content", "")
                if content:
                    logger.info(f"LLM 总结成功（{len(content)} 字符, model={data.get('model', '?')}）")
                    return content.strip()

            logger.error(f"LLM 返回格式异常：status={resp.status}, data={str(data)[:500]}")
            return "❌ 总结生成失败：返回格式异常"

    except asyncio.TimeoutError:
        return "❌ DeepSeek API 调用超时！请稍后重试"
    except UnicodeEncodeError as e:
        logger.error(f"编码错误：{e}")
        return "❌ 总结编码失败"
    except Exception as e:
        return f"❌ 总结生成失败：{str(e)[:60]}"


# ===================== 短链接解析 =====================

async def resolve_b23_short_url(short_url: str) -> str:
    """解析 b23.tv 短链接 → 完整 URL"""
    try:
        short_url = short_url.replace("&amp;", "&").replace("\\/", "/").strip()
        if not short_url.startswith(("http://", "https://")):
            short_url = f"https://{short_url}"

        session = await get_http_session()
        async with session.head(short_url, allow_redirects=True) as resp:
            logger.info(f"b23 短链接跳转：{short_url} → {resp.url}")
            return str(resp.url)
    except Exception as e:
        logger.error(f"解析 b23 短链接失败：{e}")
        return short_url


# ===================== BV 号提取 =====================

def extract_bv_and_page(msg: str) -> tuple[str, int]:
    """从纯文本提取 BV 号 + 分 P 序号"""
    bv_match = re.search(r"BV([0-9a-zA-Z]{10})", msg)
    bv_code = f"BV{bv_match.group(1)}" if bv_match else ""

    page_match = re.search(r"BV[0-9a-zA-Z]{10}\s*(\d+)", msg)
    page_index = int(page_match.group(1)) if page_match else 1

    return bv_code, page_index


async def extract_bv_from_qq_miniprogram(cq_json_raw: str) -> str:
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
        logger.info(f"QQ 小程序 JSON: {json_str[:200]}...")

        # Android 小程序格式：meta.detail_1.qqdocurl → b23.tv 短链接
        qqdocurl = data.get("meta", {}).get("detail_1", {}).get("qqdocurl", "")
        if qqdocurl and "b23.tv" in qqdocurl:
            real_url = await resolve_b23_short_url(qqdocurl)
            bv_match = re.search(r"BV([0-9a-zA-Z]{10})", real_url)
            if bv_match:
                bv_code = f"BV{bv_match.group(1)}"
                logger.info(f"从小程序提取 BV 号：{bv_code}")
                return bv_code

        # iOS 格式或其他格式：直接在 JSON 里搜 URL 或 BV 号
        json_dump = json.dumps(data, ensure_ascii=False)
        bv_match = re.search(r"BV([0-9a-zA-Z]{10})", json_dump)
        if bv_match:
            bv_code = f"BV{bv_match.group(1)}"
            logger.info(f"从小程序 JSON 提取 BV 号：{bv_code}")
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
                logger.info(f"从小程序 JSON 链接提取 BV 号：{bv_code}")
                return bv_code

        return ""
    except Exception as e:
        logger.error(f"解析 QQ 小程序失败：{e}")
        return ""


async def extract_bv_from_wechat_appmsg(raw_text: str) -> str:
    """从微信小程序卡片/XML 提取 BV 号

    微信 B站小程序分享可能以多种格式出现：
    1. XML appmsg 格式（Gewechat 可能透传原始 XML）
    2. 纯文本 URL（用户直接发链接）
    3. AstrBot 解析后的结构化数据

    策略：在原始文本中搜索 BV 号 / b23.tv 短链接
    """
    try:
        # 先尝试直接搜 BV 号（最快）
        bv_match = re.search(r"BV([0-9a-zA-Z]{10})", raw_text)
        if bv_match:
            bv_code = f"BV{bv_match.group(1)}"
            logger.info(f"从微信消息提取 BV 号：{bv_code}")
            return bv_code

        # 搜索 b23.tv 短链接
        short_match = re.search(r"https?://b23\.tv/\S+", raw_text)
        if short_match:
            short_url = short_match.group(0).rstrip('"\'\\')
            real_url = await resolve_b23_short_url(short_url)
            bv_match = re.search(r"BV([0-9a-zA-Z]{10})", real_url)
            if bv_match:
                bv_code = f"BV{bv_match.group(1)}"
                logger.info(f"从微信 b23.tv 提取 BV 号：{bv_code}")
                return bv_code

        # 搜索 bilibili.com 完整链接
        full_match = re.search(r"https?://(?:www\.)?bilibili\.com/video/BV([0-9a-zA-Z]{10})", raw_text)
        if full_match:
            bv_code = f"BV{full_match.group(1)}"
            logger.info(f"从微信 bilibili 链接提取 BV 号：{bv_code}")
            return bv_code

        # 尝试解析 XML appmsg（微信小程序卡片）
        # <url> 或 <weappinfo><path> 中可能包含 b23.tv 或 bvid
        url_match = re.search(r"<url>(.*?)</url>", raw_text, re.DOTALL)
        if url_match:
            url_content = url_match.group(1)
            if "b23.tv" in url_content:
                real_url = await resolve_b23_short_url(url_content.strip())
                bv_match = re.search(r"BV([0-9a-zA-Z]{10})", real_url)
                if bv_match:
                    return f"BV{bv_match.group(1)}"

        # 搜索 weappinfo/path 中的 bvid 参数
        path_match = re.search(r"bvid=(BV[0-9a-zA-Z]{10})", raw_text)
        if path_match:
            return path_match.group(1)

        return ""
    except Exception as e:
        logger.error(f"解析微信小程序失败：{e}")
        return ""


async def extract_bv_from_any_text(raw_text: str, platform: str = "") -> str:
    """统一的 BV 号提取入口，自动尝试所有格式

    Args:
        raw_text: 原始消息文本（可含 CQ 码 / XML / 普通文本）
        platform: 平台标识（"qq" / "gewechat" / "" 自动检测）

    Returns: BV 号字符串，未找到返回空字符串
    """
    if not raw_text or not raw_text.strip():
        return ""

    # 1. QQ 小程序卡片格式
    if platform == "qq" or "CQ:json" in raw_text or "[json:data=" in raw_text:
        bv = await extract_bv_from_qq_miniprogram(raw_text)
        if bv:
            return bv

    # 2. 微信小程序格式（含 XML appmsg 或 b23.tv）
    if platform == "gewechat" or "<appmsg" in raw_text or "b23.tv" in raw_text:
        bv = await extract_bv_from_wechat_appmsg(raw_text)
        if bv:
            return bv

    # 3. 纯文本 BV 号
    bv, _ = extract_bv_and_page(raw_text)
    if bv:
        return bv

    # 4. 兜底：尝试从任意文本中匹配 b23.tv
    short_match = re.search(r"https?://b23\.tv/\S+", raw_text)
    if short_match:
        short_url = short_match.group(0).rstrip('"\'\\')
        real_url = await resolve_b23_short_url(short_url)
        bv_match = re.search(r"BV([0-9a-zA-Z]{10})", real_url)
        if bv_match:
            return f"BV{bv_match.group(1)}"

    return ""


# ===================== 视频时长估算 =====================

def estimate_transcribe_time(duration_seconds: int) -> str:
    """根据视频时长估算转写耗时，返回人类可读的预估"""
    if duration_seconds <= 0:
        return "约 1 分钟"
    transcribe_sec = duration_seconds / 2.7
    total_sec = 10 + transcribe_sec + 20
    est_min = max(1, round(total_sec / 60))
    if est_min <= 1:
        return "约 1 分钟"
    else:
        return f"约 {est_min} 分钟"
