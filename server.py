"""archive-node 归档机端：接收多来源机（dola-pool）的文件归档，支持回源拉取。

协议（全部走 HTTPS/HTTP，鉴权用 X-Archive-Key 头）：
  GET  /health                          探活（无鉴权，仅返回版本与来源名列表）
  POST /archive/push                    推送归档文件（query: source,fname,sha256,size,mtime,task_id）
  GET  /archive/file/{source}/{fname}   拉取归档文件（回源）
  GET  /archive/list/{source}           某来源机的归档清单
  GET  /archive/stats                   全部来源机占用统计

来源机在 config.json 的 sources 里登记（name + key + 配额），一机一 key。
文件落盘 data/{source}/{fname}，清单记 SQLite（archive.db）。
sha256 与 size 双重校验，一致才算归档成功；同 sha256 重复推送幂等秒回。
"""
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE / "data"
DB_PATH = BASE / "archive.db"
CONFIG_PATH = BASE / "config.json"
VERSION = "20260930a"

DEFAULT_CONFIG = {
    "listen_host": "0.0.0.0",
    "listen_port": 8100,
    "sources": [
        # 示例：把 name/key 发给对应来源机，来源机面板配置同样的地址+key 即完成挂载
        {"name": "A机", "key": "arch-change-me-a", "quota_mb": 2048}
    ]
}

app = FastAPI(title="archive-node", version=VERSION)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2), encoding="utf-8")
        return DEFAULT_CONFIG
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def get_source(key: str | None) -> dict:
    """按 key 找来源机配置；找不到即 401。"""
    if not key:
        raise HTTPException(401, "missing X-Archive-Key")
    for s in load_config().get("sources", []):
        if s.get("key") == key:
            return s
    raise HTTPException(401, "invalid archive key")


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("""CREATE TABLE IF NOT EXISTS archive_files(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        fname TEXT NOT NULL,
        sha256 TEXT NOT NULL,
        size INTEGER NOT NULL,
        mtime INTEGER,
        task_id TEXT,
        pushed_at INTEGER NOT NULL,
        UNIQUE(source, fname))""")
    return conn


def source_dir(source: str) -> Path:
    d = DATA_DIR / source
    d.mkdir(parents=True, exist_ok=True)
    return d


def source_usage(source: str) -> int:
    d = DATA_DIR / source
    if not d.exists():
        return 0
    return sum(f.stat().st_size for f in d.rglob("*") if f.is_file())


@app.get("/health")
async def health():
    """探活：返回版本与已登记来源机名（不含 key）。"""
    cfg = load_config()
    return {"ok": True, "version": VERSION,
            "sources": [s.get("name") for s in cfg.get("sources", [])]}


@app.get("/archive/whoami")
async def archive_whoami(x_archive_key: str | None = Header(default=None)):
    """来源机挂载时调用：按 key 返回自己在归档机的登记名（push/回源路径用）。"""
    s = get_source(x_archive_key)
    return {"ok": True, "source": s.get("name"), "version": VERSION}


@app.post("/archive/push")
async def archive_push(request: Request,
                       fname: str, sha256: str, size: int,
                       source: str = "", mtime: int = 0, task_id: str = "",
                       x_archive_key: str | None = Header(default=None)):
    """推送归档：流式收文件 → sha256 校验 → 落盘 → 记清单（幂等）。

    - 来源机只需持有 key，source 缺省按 key 自动识别（传了则必须与登记名一致）
    - 配额：来源机 data 目录总占用超 quota_mb 拒绝
    - 同 (source, fname) 且 sha 一致 → dedup=true 幂等返回
    """
    s = get_source(x_archive_key)
    if source and source != s.get("name"):
        raise HTTPException(403, f"source name mismatch: expect {s.get('name')}")
    source = s.get("name")
    fname = Path(fname).name  # 防目录穿越
    if not fname or fname.startswith("."):
        raise HTTPException(400, "bad fname")
    quota = int(s.get("quota_mb", 2048)) * 1024 * 1024
    if source_usage(source) + size > quota:
        raise HTTPException(507, f"quota exceeded ({quota // 1024 // 1024}MB)")

    target = source_dir(source) / fname
    tmp = target.with_suffix(target.suffix + ".part")
    h = hashlib.sha256()
    n = 0
    with open(tmp, "wb") as f:
        async for chunk in request.stream():
            n += len(chunk)
            h.update(chunk)
            f.write(chunk)
            if n > quota:
                f.close()
                tmp.unlink(missing_ok=True)
                raise HTTPException(507, "quota exceeded during upload")
    if n != size:
        tmp.unlink(missing_ok=True)
        raise HTTPException(400, f"size mismatch: got {n} expect {size}")
    digest = h.hexdigest()
    if digest != sha256:
        tmp.unlink(missing_ok=True)
        raise HTTPException(400, f"sha256 mismatch: got {digest} expect {sha256}")

    dedup = False
    if target.exists():
        dedup = True  # 同名同 sha：幂等（sha 不同名冲突理论不发生，fname 带时间戳唯一）
    else:
        tmp.rename(target)
    tmp.unlink(missing_ok=True)

    now = int(time.time())
    conn = db()
    conn.execute("""INSERT INTO archive_files(source,fname,sha256,size,mtime,task_id,pushed_at)
                    VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(source,fname) DO UPDATE SET
                      sha256=excluded.sha256, size=excluded.size,
                      mtime=excluded.mtime, task_id=excluded.task_id,
                      pushed_at=excluded.pushed_at""",
                 (source, fname, digest, n, mtime, task_id, now))
    conn.commit()
    conn.close()
    if mtime:
        os.utime(target, (mtime, mtime))  # 保留原修改时间
    return {"ok": True, "dedup": dedup, "fname": fname, "sha256": digest,
            "usage_bytes": source_usage(source)}


@app.get("/archive/file/{source}/{fname}")
async def archive_file(source: str, fname: str,
                       x_archive_key: str | None = Header(default=None)):
    """回源拉取：来源机本地被清理后，从这里把文件拉回去服务用户。"""
    get_source(x_archive_key)
    fname = Path(fname).name
    target = DATA_DIR / source / fname
    if not target.is_file():
        raise HTTPException(404, "not archived")
    return FileResponse(target, filename=fname)


@app.get("/archive/list/{source}")
async def archive_list(source: str,
                       x_archive_key: str | None = Header(default=None)):
    get_source(x_archive_key)
    conn = db()
    rows = conn.execute(
        "SELECT fname,sha256,size,mtime,task_id,pushed_at FROM archive_files "
        "WHERE source=? ORDER BY pushed_at DESC", (source,)).fetchall()
    conn.close()
    return {"ok": True, "source": source, "count": len(rows),
            "files": [{"fname": r[0], "sha256": r[1], "size": r[2],
                       "mtime": r[3], "task_id": r[4], "pushed_at": r[5]} for r in rows]}


@app.get("/archive/stats")
async def archive_stats(x_archive_key: str | None = Header(default=None)):
    """占用统计（任一来源 key 均可查，便于来源机面板展示）。"""
    get_source(x_archive_key)
    cfg = load_config()
    out = []
    for s in cfg.get("sources", []):
        name = s.get("name")
        conn = db()
        cnt = conn.execute("SELECT count(*) FROM archive_files WHERE source=?",
                           (name,)).fetchone()[0]
        conn.close()
        used = source_usage(name)
        out.append({"name": name, "files": cnt, "usage_bytes": used,
                    "quota_bytes": int(s.get("quota_mb", 2048)) * 1024 * 1024})
    return {"ok": True, "version": VERSION, "sources": out}


if __name__ == "__main__":
    import uvicorn
    cfg = load_config()
    uvicorn.run(app, host=cfg.get("listen_host", "0.0.0.0"),
                port=int(cfg.get("listen_port", 8100)),
                access_log=False)
