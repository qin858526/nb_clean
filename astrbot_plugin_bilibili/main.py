# -*- coding: utf-8 -*-
"""
AstrBot 插件：B站视频分析
支持 QQ（NapCat）和微信（Gewechat）双平台

触发规则：
  群聊 — 引用B站消息 + @机器人
  私聊 — 直接发链接/BV号/小程序

流程：B站链接/BV号 → 优先B站AI字幕 → 无字幕则whisper.cpp转写 → DeepSeek总结 → 回复
"""

import re
import json
import time
import os
import asyncio
import inspect
from pathlib import Path

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.api import logger
from astrbot.core.message.components import At, Reply, Plain
from astrbot.core.message.message_event_result import MessageEventResult

from .bilibili_utils import (
    extract_bv_from_any_text,
    extract_bv_and_page,
    get_video_info_by_api,
    get_bilibili_subtitle,
    get_audio_url_by_api,
    get_danmaku_by_api,
    extract_audio,
    audio_to_subtitle,
    llm_summarize,
    estimate_transcribe_time,
    SAVE_DIR,
    update_cookie,
    update_deepseek_config,
)

# ===================== 可调参数 =====================
COOL_DOWN_TIME = 6                    # 群聊冷却秒数
COOL_DOWN_CLEANUP_INTERVAL = 300      # 冷却清理间隔（秒）
MSG_CACHE_MAX_SIZE = 200              # 消息缓存上限

# ===================== 冷却 & 排队 & 缓存（模块级全局，所有实例共享）=====================
_cool_down: dict[str, float] = {}
_cool_down_lock = asyncio.Lock()

_is_processing = False
_processing_lock = asyncio.Lock()
_current_bv = ""

_task_queue: asyncio.Queue = asyncio.Queue()

_msg_cache: dict[str, str] = {}       # message_id → 原始文本
_msg_cache_lock = asyncio.Lock()


def _get_platform(event: AstrMessageEvent) -> str:
    """获取平台标识：'qq' / 'gewechat' / 'unknown'"""
    name = event.get_platform_name().lower()
    if any(k in name for k in ("aiocqhttp", "napcat", "onebot")):
        return "qq"
    if any(k in name for k in ("gewe", "wechat", "weixin")):
        return "gewechat"
    return name


def _get_raw_text(event: AstrMessageEvent) -> str:
    """尽可能获取原始消息文本（含 CQ 码 / XML），用于小程序解析"""
    raw = event.message_obj.raw_message
    if raw is None:
        return ""
    # QQ / OneBot：raw_message 通常有 message 或 raw_message 字段
    if hasattr(raw, "raw_message"):
        return str(raw.raw_message) or ""
    if hasattr(raw, "message"):
        if isinstance(raw.message, str):
            return raw.message
        if isinstance(raw.message, list):
            return json.dumps(raw.message, ensure_ascii=False)
    # WeChat / Gewechat：可能直接是字符串或 dict
    if isinstance(raw, str):
        return raw
    if isinstance(raw, dict):
        return json.dumps(raw, ensure_ascii=False)
    try:
        return str(raw)
    except Exception:
        return ""


class BiliAnalyzer(Star):
    """B站视频分析插件"""

    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context, config=config)
        self._cleanup_task: asyncio.Task | None = None

        # 读取插件配置
        if config:
            if config.get("bilibili_cookie"):
                update_cookie(config["bilibili_cookie"])
            if config.get("deepseek_api_key"):
                update_deepseek_config(
                    api_key=config["deepseek_api_key"],
                    api_url=config.get("deepseek_api_url", ""),
                    model=config.get("deepseek_model", ""),
                )

    # ==================== 消息发送 ====================

    async def _send(self, event: AstrMessageEvent, message: str):
        """统一的发消息方法"""
        try:
            result = event.send(MessageEventResult().message(message))
            if inspect.isawaitable(result):
                await result
            else:
                async for _ in result:
                    pass
        except Exception as e:
            logger.error(f"发送消息失败：{e}")

    # ==================== 私聊处理器 ====================

    @filter.regex(r"BV[0-9a-zA-Z]{10}|b23\.tv|bilibili\.com/(?:video|bangumi)")
    async def on_private_bilibili(self, event: AstrMessageEvent):
        """私聊：收到B站链接/BV号直接触发"""
        if event.message_obj.group_id != "":
            return

        event.stop_event()  # 阻止 LLM Agent 同时响应
        user_id = event.get_sender_id()
        logger.info(f"私聊触发 | 用户: {user_id} | 内容: {event.message_str[:100]}")

        await self._start_or_queue(event)

    # ==================== 群聊处理器 ====================

    @filter.platform_adapter_type(filter.PlatformAdapterType.ALL)
    async def on_group_message(self, event: AstrMessageEvent):
        """群聊：检测 @机器人 后触发（内部判断是否为群消息+@机器人）"""
        msg = event.message_obj

        # 仅处理群消息
        if not msg.group_id:
            return

        # 静默缓存 B站相关消息（不管有没有@，先存，便于后续引用查询）
        raw_text = _get_raw_text(event)
        raw_to_cache = raw_text or event.message_str
        if any(k in raw_to_cache for k in ("[json:data=", "[CQ:json,data=", "b23.tv", "BV")):
            async with _msg_cache_lock:
                _msg_cache[str(msg.message_id)] = raw_to_cache
                while len(_msg_cache) > MSG_CACHE_MAX_SIZE:
                    del _msg_cache[next(iter(_msg_cache))]

        bot_id = msg.self_id

        # 检查是否 @了机器人（优先检查 At 消息组件）
        is_at_me = any(
            isinstance(c, At) and str(c.qq) == bot_id
            for c in msg.message
        )
        # fallback：正则匹配 [CQ:at,...] / [at:...] 格式
        if not is_at_me:
            at_re = re.search(r"\[CQ:at,qq=(\d+)\]|\[at:qq=(\d+)\]", raw_text)
            if at_re:
                is_at_me = (at_re.group(1) or at_re.group(2)) == bot_id

        if not is_at_me:
            return  # 未 @机器人，不响应（B站相关消息已缓存）

        event.stop_event()  # 阻止 LLM Agent 同时响应
        logger.info(f"群聊触发 | 群: {msg.group_id} | 用户: {event.get_sender_id()}")

        await self._start_or_queue(event)

    # ==================== 排队 & 冷却 ====================

    async def _start_or_queue(self, event: AstrMessageEvent):
        """统一入口：冷却检查 → 排队 → 处理"""
        global _is_processing

        is_group = event.message_obj.group_id != ""
        cool_key = event.message_obj.group_id if is_group else event.get_sender_id()

        # 冷却检查
        async with _cool_down_lock:
            now = time.time()
            if cool_key in _cool_down and (now - _cool_down[cool_key]) < COOL_DOWN_TIME:
                remain = int(COOL_DOWN_TIME - (now - _cool_down[cool_key]))
                await self._send(event, f"⏳ 冷却中！请 {remain} 秒后再试~")
                return
            _cool_down[cool_key] = now

        # 排队检查
        async with _processing_lock:
            if _is_processing:
                user_id = event.get_sender_id()
                already_queued = any(
                    t.get("event") and t["event"].get_sender_id() == user_id
                    for t in list(_task_queue._queue)
                )
                if already_queued:
                    await self._send(event, "⏳ 你已经在排队中了，请耐心等待~")
                    return
                pos = _task_queue.qsize() + 1
                await _task_queue.put({"event": event})
                current_info = f" {_current_bv}" if _current_bv else ""
                await self._send(event,
                    f"⏳ 排队中！\n"
                    f"📹 正在处理：{current_info}\n"
                    f"🔢 你排在第 {pos} 位，请耐心等待~"
                )
                return
            _is_processing = True

        try:
            await self._do_analyze(event)
        except Exception as e:
            logger.error(f"分析异常：{e}")
            await self._send(event, f"❌ 处理失败：{str(e)[:100]}")
        finally:
            await self._process_next()

    async def _process_next(self):
        """处理完当前任务后，调度队列中的下一个"""
        global _is_processing, _current_bv
        async with _processing_lock:
            if not _task_queue.empty():
                next_task = await _task_queue.get()
                next_event = next_task["event"]
                await self._send(next_event, "🎉 轮到你了！正在开始处理...")
                # 异步启动，不等待（让 handler 返回）
                asyncio.create_task(self._run_queued(next_event))
            else:
                _is_processing = False
                _current_bv = ""

    async def _run_queued(self, event: AstrMessageEvent):
        """处理队列中的任务"""
        global _is_processing
        async with _processing_lock:
            _is_processing = True
        try:
            await self._do_analyze(event)
        except Exception as e:
            logger.error(f"排队任务异常：{e}")
            await self._send(event, f"❌ 处理失败：{str(e)[:100]}")
        finally:
            await self._process_next()

    # ==================== 核心分析流程 ====================

    async def _do_analyze(self, event: AstrMessageEvent):
        """B站视频分析主流程（和 server_bot.py 的 _do_analyze 逻辑一致）"""
        global _current_bv

        platform = _get_platform(event)
        is_group = event.message_obj.group_id != ""
        sender_name = event.get_sender_name()
        tip_prefix = f"@{sender_name} \n" if is_group else ""

        # ---- Step 1: 解析 BV 号 ----
        bv_code = ""
        page_index = 1

        if is_group:
            raw_text = _get_raw_text(event)

            # 从引用消息中提取
            reply_msg_text = ""
            for comp in event.message_obj.message:
                if isinstance(comp, Reply):
                    reply_id = str(comp.id)
                    async with _msg_cache_lock:
                        reply_msg_text = _msg_cache.get(reply_id, "")
                    if reply_msg_text:
                        logger.info(f"从消息缓存找到引用内容 (id={reply_id})")
                    break

            if reply_msg_text:
                bv_code = await extract_bv_from_any_text(reply_msg_text, platform)
                if bv_code:
                    logger.info(f"从引用消息提取 BV: {bv_code}")

            if not bv_code and raw_text:
                bv_code = await extract_bv_from_any_text(raw_text, platform)
                if bv_code:
                    logger.info(f"从事件消息提取 BV: {bv_code}")

            if not bv_code:
                bv_code, page_index = extract_bv_and_page(raw_text or event.message_str)
                if bv_code:
                    logger.info(f"从纯文本提取 BV: {bv_code}")
        else:
            raw_text = _get_raw_text(event) or event.message_str
            bv_code = await extract_bv_from_any_text(raw_text, platform)
            if not bv_code:
                bv_code, page_index = extract_bv_and_page(event.message_str)

        if not bv_code:
            tip = (
                "⚠️ 引用的消息中未找到B站视频信息！"
                if is_group
                else "⚠️ 未找到B站视频信息！请发送B站链接/BV号/小程序~"
            )
            await self._send(event, f"{tip_prefix}{tip}")
            return

        _current_bv = bv_code

        # ---- Step 2: 检查强制转写 ----
        force_transcribe = False
        check_text = _get_raw_text(event) or event.message_str
        if is_group:
            for comp in event.message_obj.message:
                if isinstance(comp, Reply):
                    async with _msg_cache_lock:
                        cached = _msg_cache.get(str(comp.id), "")
                    if "转写" in cached:
                        force_transcribe = True
                    break
        if "转写" in check_text:
            force_transcribe = True

        if force_transcribe:
            logger.info(f"{bv_code}: 强制转写模式")
            subtitle_text = None
        else:
            # ---- Step 3: 获取视频信息 + 尝试 B站字幕 ----
            video_info = await get_video_info_by_api(bv_code)
            if not video_info["success"]:
                await self._send(event, f"{tip_prefix}❌ 处理失败：{video_info['msg']}")
                return

            duration = video_info.get("duration", 0)
            subtitle_ok, subtitle_text = await get_bilibili_subtitle(bv_code, video_info["cid"])

            if subtitle_ok:
                # 有字幕：直接总结
                await self._send(event,
                    f"{tip_prefix}🎬 正在分析 {bv_code}（第{page_index}分P），预计半分钟内完成..."
                )
                danmaku_text = await get_danmaku_by_api(video_info["cid"])
                summary = await llm_summarize(
                    title=video_info["title"],
                    desc=video_info["desc"],
                    danmaku=danmaku_text,
                    subtitle=subtitle_text,
                )
                lines = [
                    f"📺 {video_info['title']}",
                    "",
                    summary,
                    "",
                    f"—— 字幕+弹幕+简介  ·  {bv_code}",
                ]
                await self._send(event, f"{tip_prefix}{chr(10).join(lines)}")
                logger.info(f"字幕通道完成：{bv_code}")
                return

            if subtitle_text == "download_failed":
                await self._send(event,
                    f"{tip_prefix}❌ {bv_code} 检测到字幕但下载失败，请稍后重试"
                )
                return

            # 确认无字幕，降级转写
            est_str = estimate_transcribe_time(duration)
            await self._send(event,
                f"{tip_prefix}🎬 {bv_code}（第{page_index}分P）\n"
                f"📺 此视频无字幕，需语音转写，预计 {est_str}..."
            )

        # ---- Step 4: 转写路径（无字幕 / 强制转写）----
        if force_transcribe:
            video_info = await get_video_info_by_api(bv_code)
            if not video_info["success"]:
                await self._send(event, f"{tip_prefix}❌ 处理失败：{video_info['msg']}")
                return
            await self._send(event,
                f"{tip_prefix}🎬 {bv_code}（第{page_index}分P）\n"
                f"🔧 强制转写模式，预计需要数分钟..."
            )

        audio_url = await get_audio_url_by_api(bv_code, video_info["cid"])
        logger.info(
            f"BV:{bv_code} CID:{video_info['cid']} "
            f"audio:{audio_url[:60] if audio_url else '(无)'}..."
        )

        SAVE_DIR.mkdir(exist_ok=True)
        audio_file = SAVE_DIR / f"{bv_code}_page{page_index}_temp.wav"
        has_audio = bool(audio_url and await extract_audio(audio_url, str(audio_file)))

        if not has_audio:
            await self._send(event, f"{tip_prefix}⚠️ 音频提取失败，仅基于标题+简介+弹幕总结")
            danmaku_text = await get_danmaku_by_api(video_info["cid"])
            summary = await llm_summarize(
                title=video_info["title"],
                desc=video_info["desc"],
                danmaku=danmaku_text,
                subtitle="无音频",
            )
            lines = [
                f"📺 {video_info['title']}",
                "",
                summary,
                "",
                f"—— 弹幕+简介（无音频）  ·  {bv_code}",
            ]
            await self._send(event, f"{tip_prefix}{chr(10).join(lines)}")
            return

        audio_path = str(audio_file)
        danmaku_text = await get_danmaku_by_api(video_info["cid"])

        sub_text, sub_time = await audio_to_subtitle(audio_path, "tiny")

        if sub_text and not sub_text.startswith("transcribe") and sub_text != "audio transcribe failed":
            summary = await llm_summarize(
                title=video_info["title"],
                desc=video_info["desc"],
                danmaku=danmaku_text,
                subtitle=sub_text,
            )
            lines = [
                f"📺 {video_info['title']}",
                "",
                summary,
                "",
                f"—— 语音转写+弹幕+简介  ·  {bv_code}  ·  转写{sub_time:.0f}秒",
            ]
            await self._send(event, f"{tip_prefix}{chr(10).join(lines)}")
        else:
            await self._send(event, f"{tip_prefix}❌ 语音转写失败，请稍后重试")

        # 延迟清理
        async def _cleanup(path: str):
            await asyncio.sleep(900)
            if os.path.exists(path):
                try:
                    os.remove(path)
                    logger.info(f"临时文件已清理：{path}")
                except Exception as e:
                    logger.error(f"删除临时文件失败：{e}")

        asyncio.create_task(_cleanup(audio_path))

    # ==================== 生命周期 ====================

    async def _cool_down_cleanup_loop(self):
        """定期清理过期冷却条目"""
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

    async def terminate(self):
        """插件卸载"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
        logger.info("B站视频分析插件已卸载")
