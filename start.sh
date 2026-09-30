#!/bin/bash
# archive-node 启动脚本：首次运行自动生成 config.json，编辑 sources 后重启生效
cd "$(dirname "$0")"

# 依赖安装（首次）
python3 -m pip install --break-system-packages -q -r requirements.txt 2>/dev/null \
  || python3 -m pip install -q -r requirements.txt

# 启动（后台运行见 README；前台调试直接 ./start.sh）
exec python3 server.py
