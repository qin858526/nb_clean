#!/bin/bash
# B站视频总结机器人一键部署脚本（Ubuntu 22.04 / Debian 12）
# 适用：阿里云轻量应用服务器 / ECS 经济型实例
# 前提：已安装 NapCat 或 LLOneBot（请先完成 QQ NT 客户端 + NapCat 的部署）

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}========== 第一步：安装系统依赖 ==========${NC}"
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv ffmpeg git

echo -e "${YELLOW}========== 第二步：创建项目目录 ==========${NC}"
PROJECT_DIR="/root/bilibot"
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"

echo -e "${YELLOW}========== 第三步：Python 虚拟环境 + 依赖 ==========${NC}"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo -e "${GREEN}依赖安装完成！${NC}"
echo ""
echo -e "${YELLOW}========== 第四步：上传项目代码 ==========${NC}"
echo -e "${GREEN}请将以下文件通过 WinSCP / Xftp / scp 上传到 ${PROJECT_DIR}/：${NC}"
echo -e "  - bot.py"
echo -e "  - requirements.txt"
echo -e "  - .env（已填写 API Key 和 NapCat WS 地址）"
echo -e "${RED}上传完成后按任意键继续...${NC}"
read -n 1

echo ""
echo -e "${YELLOW}========== 第五步：写入 systemd 服务 ==========${NC}"
SERVICE_FILE="/etc/systemd/system/bilibot.service"
sudo tee "$SERVICE_FILE" > /dev/null << 'EOF'
[Unit]
Description=B站视频分析QQ机器人
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/bilibot
ExecStart=/root/bilibot/venv/bin/python /root/bilibot/bot.py
Restart=on-failure
RestartSec=10
Environment="PYTHONUNBUFFERED=1"

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable bilibot
sudo systemctl start bilibot

echo -e "${GREEN}========== 部署完成！==========${NC}"
echo ""
echo -e "常用命令："
echo -e "  查看日志：  ${GREEN}journalctl -u bilibot -f${NC}"
echo -e "  重启服务：  ${GREEN}systemctl restart bilibot${NC}"
echo -e "  停止服务：  ${GREEN}systemctl stop bilibot${NC}"
echo -e "  服务状态：  ${GREEN}systemctl status bilibot${NC}"
echo ""
echo -e "${YELLOW}⚠️ 确保 NapCat 的 WebSocket 服务端地址与 .env 中 NAPCAT_WS_URL 一致${NC}"
echo -e "  本地 NapCat 默认：ws://127.0.0.1:6099"
echo -e "  远程 NapCat 填写：ws://<远程IP>:6099"
