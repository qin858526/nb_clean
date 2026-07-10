# -*- coding: utf-8 -*-
"""
B站视频分析QQ机器人（optimized）
流程：B站链接/BV号 → API获取视频信息+音频流 → FFmpeg转WAV
    → faster-whisper语音转文字(+VAD静音过滤) → 弹幕采集
    → 通义千问(qwen-turbo)总结 → 回复QQ群/私聊

触发规则：
  群聊 — 引用B站消息 + @机器人
  私聊 — 直接发链接/BV号

性能：faster-whisper (CTranslate2 + int8量化 + VAD) 比 openai-whisper 快 3-4 倍
      10 分钟视频 base 模型 CPU 处理约 30-60 秒（原 2-4 分钟）
"""

import re
import json
import time
import os
import ssl
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
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")
NAPCAT_WS_URL = os.getenv("NAPCAT_WS_URL", "ws://127.0.0.1:6099")

# ===================== 可调参数 =====================
WHISPER_MODEL_SIZE = "base"          # tiny/base/small-medium/large-v3
WHISPER_COMPUTE_TYPE = "int8"        # int8(CPU最快)/int8_float16/float16(GPU)/default
WHISPER_BEAM_SIZE = 3                # beam search 宽度（1=最快, 3=平衡, 5=最准）
WHISPER_VAD = True                   # 启用 Silero VAD 跳过静音段
VAD_THRESHOLD = 0.5                  # VAD 灵敏度（0-1，越高越激进过滤静音）
VAD_MIN_SPEECH_MS = 250              # 最短语音段（毫秒）
VAD_MIN_SILENCE_MS = 400             # 最短静音间隔（毫秒）

COOL_DOWN_TIME = 6                   # 群聊冷却秒数
COOL_DOWN_CLEANUP_INTERVAL = 300     # 冷却字典清理间隔（秒）
LLM_TIMEOUT = 60                     # LLM 调用超时（秒）
AUDIO_DOWNLOAD_TIMEOUT = 60          # FFmpeg 下载超时（秒）
HTTP_TIMEOUT = 15                    # HTTP 请求超时（秒）

SAVE_DIR = Path("./bilibili_temp")
BILIBILI_COOKIE = ""                 # 可选，提升版权视频成功率

# 输入长度硬限制（防超长 prompt 炸 token）
MAX_TITLE_LEN = 200
MAX_DESC_LEN = 500
MAX_DANMAKU_LEN = 500
MAX_SUBTITLE_LEN = 1500
MAX_PROMPT_LEN = 4000               # 最终 prompt 总长度上限

# ===================== SSL 与日志 =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl._create_default_https_context = ssl._create_unverified_context

# ===================== NoneBot 初始化 =====================
nonebot.init(driver="~websockets", env_file=None)
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


# ===================== faster-whisper（延迟加载）=====================
_whisper_model = None
_whisper_lock = asyncio.Lock()


async def load_whisper_model():
    """延迟加载 faster-whisper 模型（CTranslate2 + int8 量化）"""
    global _whisper_model
    if _whisper_model is not None:
        return _whisper_model

    async with _whisper_lock:
        if _whisper_model is not None:
            return _whisper_model
        try:
            from faster_whisper import WhisperModel
            logger.info(
                f"📥 加载 faster-whisper 模型（{WHISPER_MODEL_SIZE}, "
                f"{WHISPER_COMPUTE_TYPE}）..."
            )
            loop = asyncio.get_event_loop()
            _whisper_model = await loop.run_in_executor(
                None,
                lambda: WhisperModel(
                    WHISPER_MODEL_SIZE,
                    device="cpu",
                    compute_type=WHISPER_COMPUTE_TYPE,
                    cpu_threads=0,          # 自动检测核心数
                    num_workers=1,          # 单 worker 避免额外开销
                ),
            )
            logger.success(
                f"✅ faster-whisper 模型加载完成（{WHISPER_MODEL_SIZE}/{WHISPER_COMPUTE_TYPE}）"
            )
        except ImportError:
            logger.error("❌ 未安装 faster-whisper：pip install faster-whisper")
            raise
        except Exception as e:
            logger.error(f"❌ faster-whisper 初始化失败：{e}")
            raise
    return _whisper_model


# ===================== 通义千问 =====================
try:
    from dashscope import Generation
except ImportError:
    logger.error("❌ 未安装 dashscope：pip install dashscope")
    raise

# ===================== 冷却字典（TTL + 自动清理）=====================
_cool_down: dict[str, float] = {}
_cool_down_lock = asyncio.Lock()
_cool_down_cleanup_task: asyncio.Task | None = None


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


# ===================== B站 API 端点 =====================
BVID_INFO_API = "https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
PLAY_URL_API = "https://api.bilibili.com/x/player/playurl"
DANMAKU_API = "https://api.bilibili.com/x/v1/dm/list.so?oid={cid}"

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
    """从 QQ 小程序卡片（[CQ:json,...]）提取 BV 号"""
    try:
        json_match = re.search(r"\[CQ:json,data=(.*?)\]", cq_json_raw)
        if not json_match:
            return ""

        json_str = json_match.group(1)
        json_str = json_str.replace("&#44;", ",").replace("&amp;", "&")
        json_str = json_str.replace('\\"', '"').replace("\\/", "/")

        data = json.loads(json_str)
        qqdocurl = data.get("meta", {}).get("detail_1", {}).get("qqdocurl", "")
        if not qqdocurl or "b23.tv" not in qqdocurl:
            return ""

        real_url = await resolve_b23_short_url(qqdocurl)
        bv_match = re.search(r"BV([0-9a-zA-Z]{10})", real_url)
        if bv_match:
            bv_code = f"BV{bv_match.group(1)}"
            logger.info(f"✅ 从小程序提取 BV 号：{bv_code}")
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


async def audio_to_subtitle(audio_path: str) -> tuple[str, float]:
    """faster-whisper 转写音频 → (字幕文本, 耗时秒数)

    核心优化：
    - CTranslate2 推理引擎 (比 openai-whisper 快 3-4x)
    - int8 量化
    - Silero VAD 自动跳过静音段
    """
    t0 = time.perf_counter()
    try:
        model = await load_whisper_model()

        vad_params = None
        if WHISPER_VAD:
            vad_params = {
                "threshold": VAD_THRESHOLD,
                "min_speech_duration_ms": VAD_MIN_SPEECH_MS,
                "min_silence_duration_ms": VAD_MIN_SILENCE_MS,
            }

        loop = asyncio.get_event_loop()
        segments, info = await loop.run_in_executor(
            None,
            lambda: model.transcribe(
                audio_path,
                language="zh",
                beam_size=WHISPER_BEAM_SIZE,
                vad_filter=WHISPER_VAD,
                vad_parameters=vad_params if WHISPER_VAD else None,
                condition_on_previous_text=False,  # 避免幻觉串联
                no_speech_threshold=0.6,
            ),
        )

        # 收集转写结果
        lines = []
        seg_count = 0
        for seg in segments:
            text = seg.text.strip()
            if text:
                lines.append(text)
            seg_count += 1

        elapsed = time.perf_counter() - t0
        duration_min = info.duration / 60 if info.duration else 0
        speedup = info.duration / elapsed if elapsed > 0 and info.duration else 0

        logger.info(
            f"✅ 转写完成：{len(lines)} 段 / {seg_count} 个语音段, "
            f"音频 {duration_min:.1f} 分钟 → 处理 {elapsed:.1f} 秒 "
            f"({speedup:.1f}x 实时)"
        )

        result = "\n".join(lines) if lines else "音频转写无有效内容"
        return result, elapsed
    except Exception as e:
        logger.error(f"【错误】音频转写失败：{e}")
        return f"转写失败：{str(e)[:50]}", 0.0


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
    """通义千问 (qwen-turbo) 总结视频内容"""
    if not DASHSCOPE_API_KEY or not DASHSCOPE_API_KEY.strip():
        return "❌ 通义千问 API Key 未配置！"

    # 硬截断输入，防止超长 prompt
    title = title[:MAX_TITLE_LEN]
    desc = desc[:MAX_DESC_LEN]
    danmaku = danmaku[:MAX_DANMAKU_LEN]
    subtitle = subtitle[:MAX_SUBTITLE_LEN]

    prompt = (
        "请总结以下B站视频的核心内容，要求：\n"
        "1. 字数控制在500字以内，尽量包括所有内容；\n"
        "2. 突出视频的核心观点/主要内容；\n"
        "3. 结合弹幕反馈（如有）。\n\n"
        f"【视频标题】：{title}\n"
        f"【视频简介】：{desc}\n"
        f"【弹幕核心】：{danmaku}\n"
        f"【音频字幕】：{subtitle}\n"
    )

    # 兜底截断
    if len(prompt) > MAX_PROMPT_LEN:
        overflow = len(prompt) - MAX_PROMPT_LEN
        prompt = prompt[:MAX_PROMPT_LEN]
        logger.warning(f"⚠️ Prompt 过长，截断 {overflow} 字符")

    try:
        loop = asyncio.get_event_loop()
        response = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: Generation.call(
                    model="qwen-turbo",
                    api_key=DASHSCOPE_API_KEY,
                    prompt=prompt,
                    result_format="text",
                    temperature=0.3,
                    top_p=0.8,
                ),
            ),
            timeout=LLM_TIMEOUT,
        )

        if hasattr(response, "output") and hasattr(response.output, "text"):
            return response.output.text.strip()
        else:
            return "❌ 总结生成失败：返回格式异常"
    except asyncio.TimeoutError:
        return "❌ 通义千问调用超时！请稍后重试"
    except UnicodeEncodeError as e:
        logger.error(f"【错误】编码错误：{e}")
        return "❌ 总结编码失败"
    except Exception as e:
        msg = str(e).lower()
        if "api key" in msg:
            return "❌ 通义千问 API Key 无效！"
        elif "quota" in msg:
            return "❌ 通义千问额度不足！"
        else:
            return f"❌ 总结生成失败：{str(e)[:60]}"


# ===================== 核心消息处理器 =====================
async def handle_bilibili_analysis(event: MessageEvent, bot: Bot):
    """整合版消息处理器"""
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

    # ---- 群聊规则：必须引用 + @ ----
    if is_group:
        is_at_me = any(
            seg.type == "at" and seg.data.get("qq") == self_id
            for seg in event.message
        )
        has_quote = hasattr(event, "reply_id") and event.reply_id

        logger.info(f"🔍 群聊规则：@机器人={is_at_me} | 引用消息={has_quote}")

        if not is_at_me:
            logger.info("❌ 未 @机器人，忽略")
            return
        if not has_quote:
            await bot.send(
                event=event,
                message="⚠️ 群聊使用规则：请先「引用B站分享消息」，再@我，我会分析被引用的视频~",
            )
            return

    # ---- 冷却机制 ----
    cool_key = group_id if is_group else user_id
    async with _cool_down_lock:
        now = time.time()
        if cool_key in _cool_down and (now - _cool_down[cool_key]) < COOL_DOWN_TIME:
            remain = int(COOL_DOWN_TIME - (now - _cool_down[cool_key]))
            await bot.send(event=event, message=f"⏳ 冷却中！请 {remain} 秒后再试~")
            return
        _cool_down[cool_key] = now

    # ---- 解析 BV 号 ----
    bv_code = ""
    page_index = 1

    if is_group:
        try:
            quoted_msg = await bot.get_msg(message_id=event.reply_id)
            quoted_raw = quoted_msg.get("raw_message", "")
            bv_code = await extract_bv_from_bilibili_miniprogram(quoted_raw)
            if not bv_code:
                bv_code, page_index = extract_bv_and_page(quoted_raw)
        except Exception as e:
            logger.error(f"❌ 获取引用消息失败：{e}")
            await bot.send(event=event, message=f"❌ 获取引用消息失败：{str(e)[:50]}")
            return
    else:
        bv_code, page_index = extract_bv_and_page(event.get_plaintext().strip())

    if not bv_code:
        tip = (
            "⚠️ 引用的消息中未找到B站视频信息！"
            if is_group
            else "⚠️ 未找到B站视频信息！请发送B站链接/BV号/小程序~"
        )
        await bot.send(event=event, message=tip)
        return

    # ---- 处理中提示 ----
    prompt = (
        f"⏳ 正在分析 {bv_code}（第{page_index}分P）...\n"
        f"💡 音频处理约需 30-90 秒，请耐心等待"
    )
    if is_group:
        prompt = f"@{safe_sender_name(event)} \n{prompt}"
    await bot.send(event=event, message=prompt)

    # ---- 核心流程 ----
    # 1. 视频信息
    video_info = await get_video_info_by_api(bv_code)
    if not video_info["success"]:
        await bot.send(event=event, message=f"❌ 处理失败：{video_info['msg']}")
        return

    # 2. 音频 URL
    audio_url = await get_audio_url_by_api(bv_code, video_info["cid"])
    logger.info(f"【调试】BV:{bv_code} CID:{video_info['cid']} audio:{audio_url[:60] if audio_url else '(无)'}...")

    # 3. 音频提取 + 转写
    audio_file = SAVE_DIR / f"{bv_code}_page{page_index}_temp.wav"
    subtitle_text = "无音频字幕（无法获取音频流/版权限制）"
    transcription_time = 0.0
    if audio_url:
        if await extract_audio(audio_url, str(audio_file)):
            subtitle_text, transcription_time = await audio_to_subtitle(str(audio_file))
        else:
            await bot.send(event=event, message="⚠️ 音频提取失败，仅基于标题+简介+弹幕总结")

    # 4. 弹幕
    danmaku_text = await get_danmaku_by_api(video_info["cid"])

    # 5. LLM 总结
    summary_text = await llm_summarize(
        title=video_info["title"],
        desc=video_info["desc"],
        danmaku=danmaku_text,
        subtitle=subtitle_text,
    )

    # 6. 拼接回复
    lines = [
        f"✅ B站视频总结（{bv_code} - 第{page_index}分P）：",
        f"📌 视频标题：{video_info['title']}",
        f"📝 核心总结：",
        summary_text,
        "",
        f"💡 总结维度：{( '音频字幕 + ' if audio_url else '' )}弹幕 + 视频简介",
    ]
    if transcription_time > 0:
        lines.append(f"⏱️ 语音转写耗时：{transcription_time:.0f} 秒")
    final_reply = "\n".join(lines)

    if is_group:
        final_reply = f"@{safe_sender_name(event)} \n{final_reply}"
    await bot.send(event=event, message=Message(final_reply))

    # 7. 清理临时文件
    if audio_file.exists():
        try:
            audio_file.unlink()
            logger.info(f"✅ 临时文件已删除：{audio_file}")
        except Exception as e:
            logger.error(f"❌ 删除临时文件失败：{e}")


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
    nonebot.run(host="0.0.0.0", port=6099)
