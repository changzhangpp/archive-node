# archive-node 归档机

独立轻量部署的文件归档节点（非 dola-pool，无浏览器/账号/调度等重功能）：接收一台或多台 dola-pool 来源机的生成产物归档（视频/图片），支持回源拉取。自带管理台（资源管理 / 文件管理 / 来源机关联配置）。多对多：一台来源机可挂多个归档机，一台归档机可接多个来源机，凭「地址 + apikey」识别。

## 部署（归档机上执行）

```bash
# 1. 拉代码（仓库私有，需 GitHub PAT）
git clone -b archive-node --depth 1 https://github.com/changzhangpp/dola-poolmanage.git archive-node
cd archive-node

# 2. 启动（首次自动生成 config.json，日志打印随机 admin_key）
chmod +x start.sh && ./start.sh
# 后台常驻：
setsid nohup python3 server.py > archive.log 2>&1 < /dev/null &

# 3. 打开管理台 http://127.0.0.1:8100/admin （外网用平台预览地址 + /admin）
#    用日志里的 admin_key 登录，在「来源机关联配置」里增删改来源机（名称/key/配额），保存即写回 config.json
```

依赖只有 fastapi + uvicorn（start.sh 自动装）。

## 管理台功能

| 区块 | 说明 |
|------|------|
| 资源管理 | 磁盘总量/剩余、归档总量、各来源机占用与配额使用条、最近推送时间 |
| 文件管理 | 按来源机浏览（分页）、下载、删除单个、清空某来源机（删除有确认） |
| 来源机关联配置 | 增删改来源机（名称/key/配额），保存写回 config.json 立即生效；可改 admin_key |

## 与来源机对接

1. 管理台「来源机关联配置」添加来源机（如 A机 / arch-xxx / 配额）
2. 把归档机的**对外访问地址**（沙箱环境为平台预览域名）+ 该来源机的 **key** 发给对方
3. 来源机（dola-pool）面板 → 设置 → 归档节点：填地址+key，测试联通 → 启用

## 协议（供排障）

| 端点 | 鉴权 | 说明 |
|------|------|------|
| `GET /health` | 无 | 探活，返回版本与来源机名列表 |
| `GET /admin` | 页面内输 key | 管理台 |
| `POST /archive/push?fname=&sha256=&size=[&mtime=&task_id=]` | X-Archive-Key | 推送归档，body 为文件原始字节 |
| `GET /archive/whoami` | X-Archive-Key | 按 key 返回登记名 |
| `GET /archive/file/{source}/{fname}` | X-Archive-Key | 回源拉取 |
| `GET /archive/list/{source}` / `/archive/stats` | X-Archive-Key | 清单 / 占用 |

## 特性

- sha256 + size 双校验，一致才算归档成功；重复推送幂等
- 每来源机独立配额（quota_mb），超限拒绝（507）
- 清单记 SQLite（archive.db），文件落 `data/{来源机名}/`
- 管理台删除功能（单文件/清空来源机）带确认；删除后来源机回源失效
