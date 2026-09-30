# archive-node 归档机

独立部署的文件归档节点：接收一台或多台 dola-pool 来源机的生成产物归档（视频/图片），支持回源拉取。多对多：一台来源机可挂多个归档机，一台归档机可接多个来源机，凭「地址 + apikey」识别。

## 部署（归档机上执行）

```bash
# 1. 把 archive-node 目录放到归档机（拷贝或 git clone）

# 2. 编辑 config.json，登记来源机（每来源机一条：名字 + 独立 key + 配额MB）
#    key 自行生成随机串即可，例如：
#    {"listen_port": 8100, "sources": [
#        {"name": "A机", "key": "arch-xxxxxxxx-a", "quota_mb": 2048},
#        {"name": "B机", "key": "arch-yyyyyyyy-b", "quota_mb": 2048}
#    ]}

# 3. 启动
chmod +x start.sh
./start.sh
```

后台常驻：

```bash
setsid nohup python3 server.py > archive.log 2>&1 < /dev/null &
```

## 与来源机对接

1. 归档机确认在跑：`curl http://127.0.0.1:8100/health`
2. 把归档机的**对外访问地址**（沙箱环境为平台预览域名，如 `https://8100-xxxx.monkeycode-ai.live`）+ 该来源机对应的 **key** 发给来源机
3. 来源机（dola-pool）面板 → 设置 → 归档节点：填地址+key+名称，测试联通 → 启用

## 协议（供排障）

| 端点 | 说明 |
|------|------|
| `GET /health` | 探活，返回版本与来源机名列表（无鉴权） |
| `POST /archive/push?source=&fname=&sha256=&size=&mtime=&task_id=` | 推送归档，body 为文件原始字节，头 `X-Archive-Key` |
| `GET /archive/file/{source}/{fname}` | 回源拉取，头 `X-Archive-Key` |
| `GET /archive/list/{source}` | 归档清单 |
| `GET /archive/stats` | 各来源机占用统计 |

## 特性

- sha256 + size 双校验，一致才算归档成功；重复推送幂等
- 每来源机独立配额（quota_mb），超限拒绝（507）
- 清单记 SQLite（archive.db），文件落 `data/{来源机名}/`
- 不开删除接口（归档机数据只进不出，清理手工处理）
