#!/bin/bash
# B站视频总结机器人一键部署脚本（阿里云Ubuntu 22.04）
# 作者：豆包编程助手
# 适用：阿里云轻量应用服务器/ECS经济型实例

# 颜色输出（方便看日志）
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 第一步：更新系统+安装核心依赖
echo -e "${YELLOW}========== 第一步：安装系统依赖 ==========${NC}"
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv ffmpeg git screen wget unzip

# 第二步：创建项目目录+下载go-cqhttp
echo -e "${YELLOW}========== 第二步：部署go-cqhttp（QQ机器人协议） ==========${NC}"
mkdir -p /root/bilibot /root/gocq
cd /root/gocq

# 下载go-cqhttp（Linux 64位）
wget -O go-cqhttp.tar.gz https://github.com/Mrs4s/go-cqhttp/releases/download/v1.2.0/go-cqhttp_linux_amd64.tar.gz
tar -zxvf go-cqhttp.tar.gz
rm -f go-cqhttp.tar.gz

# 生成默认配置文件（反向WS）
echo -e "0\n" | ./go-cqhttp > /dev/null 2>&1

# 修改go-cqhttp配置（反向WS对接NoneBot）
sed -i 's/^  - url: .*/  - url: ws:\/\/127.0.0.1:8082\/onebot\/v11\/ws/' config.yml
sed -i 's/^  reconnect-interval: .*/  reconnect-interval: 3000/' config.yml

echo -e "${GREEN}go-cqhttp配置完成！后续需要手动扫码登录QQ${NC}"

# 第三步：部署NoneBot项目
echo -e "${YELLOW}========== 第三步：部署NoneBot项目 ==========${NC}"
cd /root/bilibot

# 创建Python虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装项目依赖
pip install --upgrade pip
pip install nonebot2 nonebot-adapter-onebot openai-whisper requests ffmpeg-python uvicorn dashscope urllib3

# 提示：上传项目代码
echo -e "${YELLOW}========== 第四步：手动上传项目代码 ==========${NC}"
echo -e "${GREEN}请将本地的bot.py等项目文件，通过WinSCP/Xftp上传到 /root/bilibot 目录下${NC}"
echo -e "${RED}上传完成后，按任意键继续...${NC}"
read -n 1

# 第五步：后台运行服务
echo -e "${YELLOW}========== 第五步：后台运行服务 ==========${NC}"
# 运行go-cqhttp（后台会话）
screen -dmS gocq bash -c "cd /root/gocq && ./go-cqhttp"
echo -e "${GREEN}go-cqhttp已后台运行！请执行 screen -r gocq 扫码登录QQ${NC}"

# 运行NoneBot（后台会话）
screen -dmS bilibot bash -c "cd /root/bilibot && source venv/bin/activate && python bot.py"
echo -e "${GREEN}NoneBot已后台运行！请执行 screen -r bilibot 查看运行日志${NC}"

# 完成提示
echo -e "${GREEN}========== 部署完成！==========${NC}"
echo -e "1. 登录go-cqhttp：screen -r gocq（扫码登录QQ后按Ctrl+A+D后台）"
echo -e "2. 查看机器人日志：screen -r bilibot（按Ctrl+A+D后台）"
echo -e "3. 测试：在QQ群里引用B站视频+@机器人，看是否响应"
echo -e "4. 重启服务：screen -S gocq -X quit（停止go-cqhttp）；screen -S bilibot -X quit（停止NoneBot）"