# chunk-sender（归档分片发送组件）

把 archive-node 的「分片上传」能力抽成独立组件，可单独部署在任意来源机（A/B/其他系统）上，
把本机要归档的内容切成分片后推送到归档机。零第三方依赖，Python 3.8+。

- 发送：`send` / `send-dir`（分片上传）
- 取回：`list` / `stats` / `fetch` / `fetch-all`（回源下载、校验、续传）
- 作为 **CLI**：`archive-send send|fetch ...`
- 作为 **库**：`from chunk_sender import ArchiveSender, SenderConfig`
- 作为 **HTTP 微服务**：`chunk_sender_service.py`，供非 Python 系统调用
- 作为 **容器**：自带 `Dockerfile`

## 集成方式（其他系统如何接入）

本组件既可**独立部署**，也可**嵌入现有系统**。其他系统要往归档机推送数据时，按对接方技术栈选择：

| 方式 | 适用场景 | 入口 | 说明 |
|---|---|---|---|
| 直接对接协议 | 任意语言/系统 | `POST /archive/push_chunk`、`GET /archive/push_status`、`POST /archive/push_commit` | 不依赖本组件，按协议自行实现切片与提交 |
| Python 库 | Python 系统内嵌 | `ArchiveSender(...).send(path, task_id=..., on_progress=...)` | 进程内调用，支持进度回调 |
| HTTP 微服务 | 非 Python 系统、跨进程/跨机器 | 独立启动 `chunk_sender_service.py`，`POST /send` | 请求体为文件字节，返回归档结果 |
| CLI / Docker | 定时任务、脚本、容器化 | `archive-send send ./file` 或运行容器 | 适合批处理与调度集成 |

接入前提：每台来源机需在归档机 `config.json` 登记一个来源机名与 key。归档机据此做**来源隔离**（key 只能访问本来源机数据，跨来源 403）与**独立配额**（`quota_mb`，0 为不限）。key 通过 `X-Archive-Key` 请求头传递，仅放环境变量，勿写入代码或仓库。

### 回调与结果获取

当前组件为**同步语义**，没有内置的异步回调/webhook：

- **CLI**：执行结束返回退出码，`--json` 输出结果 JSON（`fname`、`sha256`、`size`、`chunks_total`、`chunks_uploaded`、`chunks_skipped`、`dedup`、`usage_bytes`）。
- **Python 库**：`send()` 返回结果 dict；`on_progress(done, total, nbytes)` 提供分片进度回调。
- **HTTP 微服务**：`POST /send` 在请求内完成上传并同步返回归档机的 commit 结果。

因此「归档完成后通知业务系统」这类回调，需要调用方在拿到同步结果后自行处理（例如在业务侧发起通知）。若需要由归档机在归档成功后**主动回调指定 URL**（含成功/失败状态、重试与签名校验），目前未内置，可作为后续扩展需求提出。

## 分片逻辑说明

归档机侧协议共 3 个接口，发送端负责"切片、补片、提交"：

```mermaid
graph LR
    A["来源机文件"] --> B["计算 size 与 sha256"]
    B --> C["按 chunk_size 切片"]
    C --> D["push_status 查询已收片"]
    D --> E["仅补传缺失片 push_chunk"]
    E --> F["push_commit 拼装并整体校验"]
    F --> G["归档机落盘并记账"]
```

| 步骤 | 接口 | 说明 |
|---|---|---|
| 1. 指纹 | 本地 | 流式计算文件 `size` 与整体 `sha256` |
| 2. 规划 | 本地 | `total = ceil(size / chunk_size)`；空文件按 1 个空片 |
| 3. 查缺 | `GET /archive/push_status?upload_id&fname` | 返回已收到分片的 `seq` 列表 |
| 4. 补片 | `POST /archive/push_chunk?fname&upload_id&seq` | 请求体为原始分片字节，每片 ≤128KB |
| 5. 提交 | `POST /archive/push_commit?fname&upload_id&size&sha256&mtime&task_id` | 服务端按 seq 拼装，整体 sha256 校验后落盘记账 |

关键约定：

- 请求头 `X-Archive-Key: <归档 key>`，归档机据此识别来源机（`/archive/whoami` 可自检）。
- `upload_id`：16 字节随机 hex（32 字符），同一次上传的所有分片共用；跨进程续传时复用同一值。
- `seq`：从 0 开始连续编号，服务端落为 `{seq:06d}.part`，commit 按序拼装。
- 分片大小：默认 **48KB**（适配平台预览代理约 64KB 的 body 限制），可调范围 1KB–128KB。
- `fname`：仅文件名（服务端会取 basename），不得以 `.` 开头；同名按幂等覆盖。
- `mtime`：源文件修改时间（秒级 epoch），归档机落盘后回填，便于保留原始时间。
- 空文件：上传 1 个 0 字节分片，commit `size=0`，服务端可正常拼装记账。

### 取回逻辑

| 步骤 | 接口 | 说明 |
|---|---|---|
| 1. 清单 | `GET /archive/list/{source}` | 取回前读取 `size`/`sha256`/`mtime` 作为校验基准 |
| 2. 下载 | `GET /archive/file/{source}/{fname}` | 流式回源；支持 `Range` 时用 `.part` 断点续传 |
| 3. 校验 | 本地 | 按清单校验 `size` 与整体 `sha256` |
| 4. 落位 | 本地 | 校验通过后 `.part` 原子改名；按清单回填 `mtime` |
| 5. 统计 | `GET /archive/stats` | 查看各来源机占用 |

- **来源隔离**：key 只能访问其登记来源机的文件，跨来源返回 403。
- **安全覆盖**：目标已存在且校验一致时跳过；不一致需显式 `--overwrite`。
- **续传**：下载中断留下的 `<dest>.part` 在下次 `fetch` 时自动续传，完成后整体复校。
- **回源留痕**：归档机对每次取回记 `audit_log`（action=fetch）。
- **在线预览**：归档机提供 `GET /archive/preview/{source}/{fname}`（inline、支持 Range），管理台「文件管理」内置预览；程序侧可用来源机 key 访问，浏览器请用管理台或短期 token。
- **分片合并预览**：上传中途（未 commit）也可在管理台「文件管理」下方「未完成分片」中合并预览；归档机按序合并到 `data/_preview/`，24h 未再预览自动清理，commit 后立即清理。

## 快速开始

```bash
# 1) 配置（key 只放环境变量，勿写入仓库）
export ARCHIVE_URL="https://8100-572cc49902f2ee46.monkeycode-ai.live"
export ARCHIVE_KEY="<归档 key>"

# 2) 自检：确认 key 对应哪个来源机
python3 chunk_sender.py whoami

# 3) 预演：只看分片计划，不传输
python3 chunk_sender.py send ./big.mp4 --dry-run

# 4) 正式发送（默认断点续传）
python3 chunk_sender.py send ./big.mp4 --task-id job-20261021

# 5) 批量发送目录
python3 chunk_sender.py send-dir ./exports --pattern "*.mp4"

# 6) 取回：先看清单/统计，再下载（默认校验 sha256，支持 .part 续传）
python3 chunk_sender.py list
python3 chunk_sender.py stats
python3 chunk_sender.py fetch big.mp4 --dest ./restore/
python3 chunk_sender.py fetch-all --dest-dir ./restore/
```

也可以 `python3 -m chunk_sender ...`（把本目录加入 `PYTHONPATH`），或安装后使用 `archive-send`：

```bash
pip install .
archive-send send ./big.mp4
```

## 配置项

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `ARCHIVE_URL` | 无（必填） | 归档机地址，不带 `/archive` 后缀 |
| `ARCHIVE_KEY` | 无（必填） | 归档 key |
| `ARCHIVE_CHUNK_SIZE` | `49152` | 分片字节数（1024–131072） |
| `ARCHIVE_TIMEOUT` | `30` | 单请求超时（秒） |
| `ARCHIVE_RETRIES` | `4` | 5xx/429/网络错误重试次数 |
| `ARCHIVE_BACKOFF` | `0.8` | 重试退避基数（指数增长，上限 10s） |
| `ARCHIVE_VERIFY_TLS` | `1` | `0` 时跳过证书校验（不建议） |
| `SERVICE_HOST` | `0.0.0.0` | HTTP 服务监听地址 |
| `SERVICE_PORT` | `8089` | HTTP 服务端口 |
| `SERVICE_TOKEN` | 空 | 服务鉴权令牌；为空则无鉴权（仅内网） |
| `SERVICE_MAX_MB` | `0` | 服务接收 body 上限（MB），0 为不限 |

示例见 `config.example.env`。

## HTTP 微服务（供非 Python 系统调用）

```bash
export ARCHIVE_URL=... ARCHIVE_KEY=... SERVICE_TOKEN=<本服务令牌>
python3 chunk_sender_service.py
```

```bash
# 健康检查
curl -s http://127.0.0.1:8089/health

# 发送文件：body 直接是文件字节
curl -s -X POST \
  -H "X-Service-Token: <本服务令牌>" \
  --data-binary @./big.mp4 \
  "http://127.0.0.1:8089/send?fname=big.mp4&task_id=job-20261021"
```

可选查询参数：`chunk_size`、`upload_id`（续传）、`resume=0`、`mtime`。
服务端返回归档机的 commit 结果（`ok/sha256/size/dedup/usage_bytes` 等）。

取回相关接口（同样需鉴权）：

```bash
# 清单 / 统计
curl -s -H "X-Service-Token: <token>" http://127.0.0.1:8089/list
curl -s -H "X-Service-Token: <token>" http://127.0.0.1:8089/stats

# 回源下载（响应体即文件字节）
curl -s -H "X-Service-Token: <token>" -OJ \
  "http://127.0.0.1:8089/fetch?fname=big.mp4"
```

## 容器部署

```bash
docker build -t chunk-sender .
docker run --rm -p 8089:8089 \
  -e ARCHIVE_URL=... -e ARCHIVE_KEY=... -e SERVICE_TOKEN=... \
  chunk-sender
```

## 断点续传与失败恢复

- 发送：单次运行内先 `push_status`，只补缺失片；网络中断后重跑即可继续。
- 跨进程/跨机器：显式传同一个 `--upload-id`（需与首次相同且 `fname` 一致），组件会查缺补片。
  未传时每次生成新 id，等价于全新上传。
- 分片与提交均幂等：`push_chunk` 同 seq 覆盖，`push_commit` 按 (source,fname) 幂等 upsert。
- 归档机对超过 24h 未 commit 的暂存分片会自动清理，超期后重跑会从头补片。
- 取回：下载中断后保留 `<dest>.part`，重跑 `fetch` 自动 `Range` 续传，完成后整体 sha256 复校。
- 5xx/429/网络错误自动重试；4xx（鉴权失败、参数错误、配额不足）直接失败并返回原因。

## 安全

- key 仅通过环境变量传入，禁止写入代码或提交仓库；日志不打印 key。
- `ARCHIVE_KEY` 是**来源机 key**（由归档机管理台为每台来源机分配），不是管理台 `admin_key`。
- 生产使用 HTTPS；仅在受信任内网才考虑 `ARCHIVE_VERIFY_TLS=0`。
- HTTP 微服务务必设置 `SERVICE_TOKEN`，并限制在内网/白名单访问。
- 取回同样受来源隔离约束：key 只能读本来源机文件；跨来源请求由归档机返回 403。

## 兼容性

- 对接 archive-node 的 `/archive/push_chunk`、`/archive/push_status`、`/archive/push_commit`。
- 服务端单分片上限 128KB；若链路有代理 body 限制（如 64KB），请保持默认 48KB。
- 归档结果与直传 `/archive/push` 的账务语义一致（sha256、size、mtime、task_id、幂等 dedup）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试使用本地 mock 归档机，覆盖：整体往返、断点续传跳片、5xx 重试、空文件、dry-run、
非法文件名与分片大小边界；取回侧覆盖清单、往返下载、已存在跳过、Range 续传、
sha256 校验失败与 overwrite 保护。
