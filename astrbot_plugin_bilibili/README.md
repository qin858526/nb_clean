# B站视频分析 - AstrBot 插件

支持 QQ（NapCat）+ 微信（Gewechat）双平台。

## 架构

```
QQ   ←→ NapCat (OneBot) ←→                    ←→ DeepSeek API
微信 ←→ Gewechat (Docker) ←→  AstrBot (pip)  ←→ B站 API
                                             ←→ whisper.cpp
```

- **Gewechat**：Docker 部署（微信协议端，必须 Docker）
- **AstrBot**：宿主机 pip 安装（方便调用 whisper.cpp，避免 Docker 二进制兼容问题）
- **NapCat**：保持现有配置，只需改端口指向 AstrBot

---

## 部署步骤

### 1. 启动 Gewechat

```bash
cd /root/bilibot-astrbot
docker compose up -d
```

### 2. 安装 AstrBot

```bash
pip install astrbot
```

### 3. 配置环境变量

把 `.env.example` 复制为 `.env` 并填入实际值：

```bash
cp astrbot_plugin_bilibili/.env.example .env
vim .env
```

### 4. 安装插件

```bash
# 方式A：软链接（推荐，方便更新代码）
ln -s /root/bilibot-astrbot/astrbot_plugin_bilibili \
      /root/.astrbot/plugins/astrbot_plugin_bilibili

# 方式B：直接复制
cp -r astrbot_plugin_bilibili /root/.astrbot/plugins/
```

### 5. 修改 NapCat 配置

让 NapCat 连 AstrBot 而非 NoneBot2。在 NapCat 的 OneBot 配置中，将 WS 地址从：
```
ws://127.0.0.1:18082    # NoneBot2（旧）
```
改为：
```
ws://127.0.0.1:6199      # AstrBot OneBot 适配器
```

### 6. 启动 AstrBot

```bash
astrbot
```

首次启动会生成默认配置，然后打开 WebUI 继续配置。

### 7. 配置 WebUI

浏览器打开 `http://服务器IP:6185`，默认账号密码 `astrbot / astrbot`。

**消息平台** → 添加适配器：

| 平台 | 适配器类型 | 关键配置 |
|:----:|:---------|:--------|
| QQ | aiocqhttp | 等待 NapCat 连上来即可（第5步配置的 WS） |
| 微信 | GEWECHAT | base_url=`http://127.0.0.1:2531`, port=`11451` |

**微信扫码**：适配器日志中会显示二维码链接，用微信小号扫码登录。

---

## 目录结构

```
/root/bilibot-astrbot/
├── docker-compose.yml
├── .env
├── astrbot_plugin_bilibili/     # 插件代码
│   ├── main.py                  # AstrBot 插件入口
│   ├── bilibili_utils.py        # 核心工具函数
│   └── _conf_schema.json        # WebUI 可配置项
├── gewe/temp/                   # Gewechat 临时文件
└── bilibili_temp/               # 音频临时文件
    └── subtitle_cache/          # B站字幕缓存（3天 TTL）
```

---

## 从 NoneBot2 迁移 checklist

- [ ] 部署 Gewechat（`docker compose up -d`）
- [ ] 安装 AstrBot（`pip install astrbot`）
- [ ] 安装插件（软链接或复制）
- [ ] 修改 NapCat WS 地址：`18082` → `6199`
- [ ] 停掉旧 bot：`pkill -f server_bot.py`
- [ ] 启动 AstrBot：`astrbot`
- [ ] WebUI 配置 QQ + 微信适配器
- [ ] 微信扫码登录
- [ ] 测试 QQ 群聊 + 私聊
- [ ] 测试微信私聊 + 群聊
