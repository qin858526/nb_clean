# -*- coding: utf-8 -*-
"""
B站视频分析机器人（终极整合版）
核心：旧代码稳定音频获取 + 新代码严格群聊规则
1. 群聊：仅「引用B站消息+@机器人」触发，无误操作
2. 音频：旧代码FFmpeg直连音频URL，稳定获取无版权限制视频音频
3. 兼容：修复编码/SSL/冷却/小程序解析等所有问题
"""

import re
import json
import time
import os
import sys
import ssl
import asyncio
import requests
import urllib3
import subprocess
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr

# ===================== 基础配置（必改）=====================
# 从 .env 读取阿里云百炼API Key（必填）
from dotenv import load_dotenv
load_dotenv()
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")
# Whisper模型（tiny最快/base平衡/small最准，2C4G建议base）
WHISPER_MODEL = "base"
# 临时文件目录
SAVE_DIR = Path("./bilibili_temp")
SAVE_DIR.mkdir(exist_ok=True)
# 群聊冷却时间（秒）
COOL_DOWN_TIME = 60
# B站Cookie（可选，提升版权视频成功率）
BILIBILI_COOKIE = ""
# ==========================================================

# 禁用SSL警告 + 强制UTF-8编码
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl._create_default_https_context = ssl._create_unverified_context
try:
    reload(sys)
    sys.setdefaultencoding('utf-8')
except Exception as e:
    pass

# ===================== NoneBot初始化 =====================
import nonebot
from nonebot import on_message, get_bot, logger
from nonebot.adapters.onebot.v11 import Adapter as OneBotV11Adapter
from nonebot.adapters.onebot.v11 import Bot, MessageEvent, Message
from nonebot.rule import Rule

# NoneBot核心配置
nonebot.init(driver="nonebot.drivers.fastapi")
driver = nonebot.get_driver()
driver.register_adapter(OneBotV11Adapter)
driver.config.onebot_access_token = ""

# ===================== B站API配置（旧代码稳定版）=====================
BVID_INFO_API = "https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
PLAY_URL_API = "https://api.bilibili.com/x/player/playurl"
DANMAKU_API = "https://api.bilibili.com/x/v1/dm/list.so?oid={cid}"

# 请求头（旧代码稳定版，模拟浏览器）
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.bilibili.com/",
    "Origin": "https://www.bilibili.com",
    "Cookie": BILIBILI_COOKIE
}

# ===================== Whisper初始化（旧代码稳定版）=====================
try:
    import whisper
    whisper_model = whisper.load_model(
        WHISPER_MODEL,
        device="cpu" # 强制CPU（云服务器无GPU）
    )
    logger.success("Whisper模型初始化成功")
except ImportError:
    logger.error(" 未安装whisper：pip install openai-whisper")
    raise
except Exception as e:
    logger.error(f" Whisper初始化失败：{str(e)}")
    raise

# ===================== 通义千问导入 =====================
try:
    from dashscope import Generation
except ImportError:
    logger.error(" 未安装dashscope：pip install dashscope")
    raise

# ===================== 全局冷却字典 =====================
cool_down = {}

# ===================== 核心辅助函数（新代码+旧代码整合）=====================
def get_raw_message_from_event(event: MessageEvent) -> str:
    """提取原始消息（保留CQ码）"""
    if hasattr(event, "_json"):
        raw_data = event._json
        if isinstance(raw_data, str):
            raw_data = json.loads(raw_data)
        return raw_data.get("raw_message", "") or raw_data.get("message", "")

    event_dict = event.model_dump()
    return event_dict.get("raw_message", "") or event_dict.get("message", "")

def parse_cq_codes(full_msg_str: str, self_id: str) -> tuple[str, bool]:
    """解析CQ码：引用ID + @机器人状态"""
    reply_id = ""
    is_at_me = False

    # 匹配引用消息
    reply_match = re.search(r"\[CQ:reply,id=(\d+)\]|\[reply:id=(\d+)\]", full_msg_str)
    if reply_match:
        reply_id = reply_match.group(1) or reply_match.group(2)

    # 匹配@机器人
    at_match = re.search(r"\[CQ:at,qq=(\d+)\]|\[at:qq=(\d+)\]", full_msg_str)
    if at_match:
        at_qq = at_match.group(1) or at_match.group(2)
        if at_qq == self_id:
            is_at_me = True

    return reply_id, is_at_me

def resolve_b23_short_url(short_url: str) -> str:
    """解析b23短链接（新代码）"""
    try:
        short_url = short_url.replace("&amp;", "&").replace("\\/", "/").strip()
        if not short_url.startswith(("http://", "https://")):
            short_url = f"https://{short_url}"

        resp = requests.head(
            short_url,
            headers=HEADERS,
            allow_redirects=True,
            timeout=10,
            verify=False
        )
        logger.info(f"✅ b23短链接跳转：{short_url} → {resp.url}")
        return resp.url
    except Exception as e:
        logger.error(f" 解析b23短链接失败：{str(e)}")
        return short_url

def extract_bv_from_bilibili_miniprogram(cq_json_raw: str) -> str:
    """解析B站小程序获取BV号（新代码）"""
    try:
        json_match = re.search(r"\[CQ:json,data=(.*?)\]", cq_json_raw)
        if not json_match:
            return ""

        json_data_str = json_match.group(1)
        json_data_str = json_data_str.replace("&#44;", ",").replace("&amp;", "&")
        json_data_str = json_data_str.replace('\\"', '"').replace("\\/", "/")

        json_data = json.loads(json_data_str)
        qqdocurl = json_data.get("meta", {}).get("detail_1", {}).get("qqdocurl", "")
        if not qqdocurl or "b23.tv" not in qqdocurl:
            return ""

        real_url = resolve_b23_short_url(qqdocurl)
        bv_match = re.search(r"BV([0-9a-zA-Z]{10})", real_url)
        if bv_match:
            bv_code = f"BV{bv_match.group(1)}"
            logger.info(f" 从小程序提取BV号：{bv_code}")
            return bv_code

        return ""
    except Exception as e:
        logger.error(f" 解析小程序失败：{str(e)}")
        return ""

def extract_bv_and_page(msg: str) -> tuple[str, int]:
    """提取BV号+分P（旧代码）"""
    bv_pattern = r"BV([0-9a-zA-Z]{10})"
    bv_match = re.search(bv_pattern, msg)
    bv_code = f"BV{bv_match.group(1)}" if bv_match else ""

    page_pattern = r"BV[0-9a-zA-Z]{10}\s*(\d+)"
    page_match = re.search(page_pattern, msg)
    page_index = int(page_match.group(1)) if page_match else 1

    return bv_code, page_index

# ===================== 核心音频函数（旧代码稳定版，关键！）=====================
def get_video_info_by_api(bvid: str) -> dict:
    """获取视频信息（旧代码稳定版）"""
    try:
        resp = requests.get(BVID_INFO_API.format(bvid=bvid), headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data["code"] != 0:
            return {"success": False, "msg": f"B站接口错误：{data['message']}"}

        info = data["data"]
        title = info.get("title", "无标题")
        desc = info.get("desc", "").strip() or "无简介"
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
            "pages": pages
        }
    except requests.exceptions.Timeout:
        return {"success": False, "msg": "获取视频信息超时（B站接口慢）"}
    except requests.exceptions.HTTPError as e:
        return {"success": False, "msg": f"B站接口HTTP错误：{str(e)}"}
    except Exception as e:
        return {"success": False, "msg": f"获取视频信息失败：{str(e)[:80]}"}

def get_audio_url_by_api(bvid: str, cid: int) -> str:
    """【旧代码稳定版】获取音频链接（核心！能正常获取音频）"""
    params = {
        "bvid": bvid,
        "cid": cid,
        "fnval": 16,    # 强制仅音频流
        "fnver": 0,
        "fourk": 0,
        "platform": "web",
        "high_quality": 1
    }
    try:
        resp = requests.get(PLAY_URL_API, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if data["code"] != 0:
            logger.error(f"【错误】B站音频接口返回：{data['message']}（cid={cid}）")
            return ""

        dash_data = data["data"].get("dash", {})
        audio_streams = dash_data.get("audio", [])

        if not audio_streams:
            logger.warning(f"【警告】无音频流（cid={cid}），可能是版权视频")
            return ""

        # 选最高码率音频
        audio_streams.sort(key=lambda x: x.get("bandwidth", 0), reverse=True)
        best_audio = audio_streams[0]
        audio_url = best_audio.get("baseUrl", "")

        # 补充签名
        if audio_url and "?" not in audio_url:
            audio_url += f"?cid={cid}&bvid={bvid}"

        logger.info(f"✅ 成功获取音频链接：{audio_url[:50]}...")
        return audio_url
    except requests.exceptions.Timeout:
        logger.error(f"【错误】获取音频地址超时（cid={cid}）")
        return ""
    except Exception as e:
        logger.error(f"【错误】获取音频地址失败：{str(e)}")
        return ""

def extract_audio(audio_url: str, save_path: str) -> bool:
    """【旧代码稳定版】FFmpeg提取音频（核心！）"""
    if not audio_url:
        return False

    ffmpeg_cmd = [
        "ffmpeg",
        "-y",
        "-headers", "\r\n".join([f"{k}: {v}" for k, v in HEADERS.items()]),
        "-i", audio_url,
        "-vn",
        "-acodec", "pcm_s16le",
        "-ar", "16000",
        "-ac", "1",
        "-f", "wav",
        save_path
    ]

    try:
        result = subprocess.run(
            ffmpeg_cmd,
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode != 0:
            logger.error(f"【错误】FFmpeg执行失败：{result.stderr[:200]}")
            return False

        if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
            logger.info(f"✅ 音频提取成功，大小：{os.path.getsize(save_path)/1024:.1f}KB")
            return True
        else:
            logger.error(f"【错误】音频文件无效（大小为0）")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"【错误】FFmpeg下载音频超时")
        return False
    except FileNotFoundError:
        logger.error(f"【错误】未找到FFmpeg（请安装并添加到PATH）")
        return False
    except Exception as e:
        logger.error(f"【错误】音频提取异常：{str(e)}")
        return False

def audio_to_subtitle(audio_path: str) -> str:
    """【旧代码稳定版】Whisper转写音频"""
    try:
        result = whisper_model.transcribe(
            audio_path,
            language="zh",
            verbose=False,
            fp16=False,
            beam_size=3
        )
        subtitle_lines = [seg["text"].strip() for seg in result["segments"] if seg["text"].strip()]
        return "\n".join(subtitle_lines) if subtitle_lines else "音频转写无有效内容"
    except Exception as e:
        logger.error(f"【错误】音频转写失败：{str(e)}")
        return f"转写失败：{str(e)[:50]}"

def get_danmaku_by_api(cid: int) -> str:
    """获取弹幕（旧代码）"""
    try:
        resp = requests.get(DANMAKU_API.format(cid=cid), headers=HEADERS, timeout=10)
        resp.encoding = "utf-8"
        dm_list = re.findall(r"<d[^>]*>(.*?)</d>", resp.text, re.DOTALL)
        dm_list = list(set([dm.strip() for dm in dm_list if dm.strip()]))[:50]
        return "\n".join(dm_list) if dm_list else "无弹幕"
    except Exception as e:
        logger.error(f"【错误】获取弹幕失败：{str(e)}")
        return "弹幕获取失败"

def llm_summarize(title: str, desc: str, danmaku: str, subtitle: str) -> str:
    """通义千问总结（旧代码+编码修复）"""
    if not DASHSCOPE_API_KEY or DASHSCOPE_API_KEY.strip() == "":
        return "❌ 通义千问API Key未配置！"

    prompt = f"""
请总结以下B站视频的核心内容，要求：
1. 字数控制在500字以内，尽量包括所有内容；
2. 突出视频的核心观点/主要内容；
3. 结合弹幕反馈（如有）。

【视频标题】：{title}
【视频简介】：{desc[:500]}
【弹幕核心】：{danmaku[:500]}
【音频字幕】：{subtitle[:1000]}
"""

    try:
        response = Generation.call(
            model="qwen-turbo",
            api_key=DASHSCOPE_API_KEY,
            prompt=prompt,
            result_format="text",
            temperature=0.3,
            top_p=0.8
        )
        if hasattr(response, "output") and hasattr(response.output, "text"):
            summary = response.output.text.strip()
            summary = summary.encode("utf-8").decode("utf-8")  # 编码修复
            return summary
        else:
            return "❌ 总结生成失败：返回格式异常"
    except UnicodeEncodeError as e:
        logger.error(f"【错误】编码错误：{str(e)}")
        return "❌ 总结编码失败：中文内容无法正常编码"
    except Exception as e:
        error_msg = str(e).lower()
        if "api key" in error_msg:
            return "❌ 通义千问API Key无效！"
        elif "timeout" in error_msg:
            return "❌ 通义千问调用超时！"
        elif "quota" in error_msg:
            return "❌ 通义千问额度不足！"
        else:
            return f"❌ 总结生成失败：{error_msg[:60]}"

# ===================== 核心消息处理器（新代码规则 + 旧代码音频）=====================
async def handle_bilibili_analysis(event: MessageEvent, bot: Bot):
    """整合版核心处理器"""
    self_id = str(event.self_id)
    message_type = event.message_type
    is_group = (message_type == "group")
    user_id = str(event.user_id)
    group_id = str(event.group_id) if is_group else ""
    bv_code = ""
    page_index = 1

    # 调试日志
    logger.info(f"\n===== 新消息触发 =====\n🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📱 {message_type} | 🧑 {user_id} | 👥 {group_id}")

    # ---------------------- 严格群聊规则（新代码）----------------------
    if is_group:
        # 验证@机器人
        is_at_me_segment = any(seg.type == "at" and seg.data.get("qq") == self_id for seg in event.get_message())
        raw_protocol_msg = get_raw_message_from_event(event)
        at_match = re.search(r"\[CQ:at,qq=(\d+)\]|\[at:qq=(\d+)\]", raw_protocol_msg)
        is_at_me_cq = (at_match and (at_match.group(1) == self_id or at_match.group(2) == self_id)) if at_match else False
        is_at_me = is_at_me_segment or is_at_me_cq

        # 验证引用消息
        has_reply_id = hasattr(event, "reply_id") and event.reply_id
        reply_match = re.search(r"\[CQ:reply,id=(\d+)\]|\[reply:id=(\d+)\]", raw_protocol_msg)
        has_reply_cq = bool(reply_match)
        has_quote = has_reply_id or has_reply_cq

        logger.info(f"🔍 群聊规则验证：@机器人={is_at_me} | 引用消息={has_quote}")

        # 严格规则判断
        if not is_at_me:
            logger.info("❌ 未@机器人，忽略消息")
            return
        if not has_quote:
            await bot.send(event=event, message="⚠️ 群聊���用规则：请先「引用B站分享消息」，再@我，我会分析被引用的视频~")
            return

    # ---------------------- 冷却机制 ----------------------
    cool_down_key = group_id if is_group else user_id
    now = time.time()
    if cool_down.get(cool_down_key) and (now - cool_down[cool_down_key]) < COOL_DOWN_TIME:
        remain = int(COOL_DOWN_TIME - (now - cool_down[cool_down_key]))
        await bot.send(event=event, message=f"⏳ 冷却中！请{remain}秒后再试~")
        return
    cool_down[cool_down_key] = now

    # ---------------------- 解析BV号 ----------------------
    raw_protocol_msg = get_raw_message_from_event(event)
    raw_text = event.get_plaintext().strip()

    if is_group:
        # 群聊：解析被引用的消息
        quoted_msg_id = event.reply_id if hasattr(event, "reply_id") and event.reply_id else (reply_match.group(1) or reply_match.group(2))
        try:
            quoted_msg = await bot.get_msg(message_id=quoted_msg_id)
            quoted_raw_msg = quoted_msg.get("raw_message", "")
            # 优先解析小程序
            bv_code = extract_bv_from_bilibili_miniprogram(quoted_raw_msg)
            # 兜底：从引用消息文本提取
            if not bv_code:
                bv_code, page_index = extract_bv_and_page(quoted_raw_msg)
        except Exception as e:
            logger.error(f"❌ 获取引用消息失败：{str(e)}")
            await bot.send(event=event, message=f"❌ 获取引用消息失败：{str(e)[:50]}")
            return
    else:
        # 私聊：直接解析文本
        bv_code, page_index = extract_bv_and_page(raw_text)

    # 无BV号提示
    if not bv_code:
        tip = "⚠️ 未找到B站视频信息！请发送B站链接/BV号/小程序~" if not is_group else "⚠️ 引用的消息中未找到B站视频信息！"
        await bot.send(event=event, message=tip)
        return

    # 发送处理中提示
    prompt = f"⏳ 正在分析 {bv_code}（第{page_index}分P）...\n💡 音频处理需要1-2分钟，请耐心等待"
    if is_group:
        prompt = f"@{event.sender.nickname} \n{prompt}"
    await bot.send(event=event, message=prompt)

    # ---------------------- 核心分析流程（旧代码稳定版）----------------------
    # 1. 获取视频信息
    video_info = get_video_info_by_api(bv_code)
    if not video_info["success"]:
        await bot.send(event=event, message=f"❌ 处理失败：{video_info['msg']}")
        return

    # 2. 获取音频链接（旧代码核心，能正常获取）
    audio_url = get_audio_url_by_api(bv_code, video_info["cid"])
    logger.info(f"【调试】BV:{bv_code} CID:{video_info['cid']} 音频URL:{audio_url[:60]}...")

    # 3. 提取音频并转写
    audio_file = SAVE_DIR / f"{bv_code}_page{page_index}_temp.wav"
    subtitle_text = "无音频字幕（无法获取音频流/版权限制）"
    if audio_url:
        # await bot.send(event=event, message="🔗 正在下载并提取音频...")
        if extract_audio(audio_url, str(audio_file)):
            # await bot.send(event=event, message="🔤 正在转写音频为字幕...")
            subtitle_text = audio_to_subtitle(str(audio_file))
        else:
            await bot.send(event=event, message="⚠️ 音频提取失败，仅基于标题+简介+弹幕总结")

    # 4. 获取弹幕
    danmaku_text = get_danmaku_by_api(video_info["cid"])

    # 5. 生成总结
    # await bot.send(event=event, message="🤖 正在生成视频总结...")
    summary_text = llm_summarize(
        title=video_info["title"],
        desc=video_info["desc"],
        danmaku=danmaku_text,
        subtitle=subtitle_text
    )

    # 6. 拼接最终回复
    final_reply = f"""
✅ B站视频总结（{bv_code} - 第{page_index}分P）：
📌 视频标题：{video_info['title']}
📝 核心总结：
{summary_text}

💡 总结维度：{('音频字幕 + ' if audio_url else '')}弹幕 + 视频简介
    """.strip()

    if is_group:
        final_reply = f"@{event.sender.nickname} \n{final_reply}"
    await bot.send(event=event, message=Message(final_reply))

    # 7. 清理临时文件
    if os.path.exists(audio_file):
        try:
            os.remove(audio_file)
            logger.info(f"✅ 临时音频文件已删除：{audio_file}")
        except Exception as e:
            logger.error(f"❌ 删除临时文件失败：{str(e)}")

# ===================== 注册处理器 + 启动 =====================
# 注册消息处理器
bilibili_analysis_matcher = on_message(rule=Rule(), priority=5, block=False)
bilibili_analysis_matcher.append_handler(handle_bilibili_analysis)

# 启动NoneBot
if __name__ == "__main__":
    nonebot.run(host="127.0.0.1", port=8082)
