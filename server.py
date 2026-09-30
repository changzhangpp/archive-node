"""archive-node 归档机：接收多来源机（dola-pool）文件归档 + 自带管理台。

对外归档协议（X-Archive-Key 鉴权，来源机用）：
  GET  /health                          探活（无鉴权）
  GET  /archive/whoami                  按 key 返回登记名
  POST /archive/push                    推送归档（query: fname,sha256,size[,mtime,task_id,source]）
  GET  /archive/file/{source}/{fname}   回源拉取
  GET  /archive/list/{source}           清单
  GET  /archive/stats                   占用统计

管理台（X-Admin-Key 鉴权，管理员用）：
  GET  /admin                           管理页面（资源管理/文件管理/来源机配置）
  GET  /admin/api/overview              磁盘与各来源机占用统计
  GET  /admin/api/files/{source}        文件清单（分页）
  GET  /admin/api/file/{source}/{fname}/dl   下载文件
  POST /admin/api/file/{source}/{fname}/delete  删除文件（磁盘+清单）
  POST /admin/api/sources               保存来源机配置（写回 config.json）

config.json：listen_port / admin_key / sources[{name,key,quota_mb}]（quota_mb=0 表示不限制，用满磁盘）。
首次启动自动生成含随机 admin_key 的 config.json 并在日志打印。
"""
import asyncio
import csv
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import sqlite3
import time
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE / "data"
DB_PATH = BASE / "archive.db"
CONFIG_PATH = BASE / "config.json"
WEB_DIR = BASE / "web"
VERSION = "20260930f"
START_TS = time.time()

DEFAULT_CONFIG = {
    "listen_host": "0.0.0.0",
    "listen_port": 8100,
    "admin_key": "archadmin-" + secrets.token_hex(8),
    "sources": [
        # 示例：把 name/key 发给对应来源机，来源机面板配置同样的地址+key 即完成挂载
        {"name": "A机", "key": "arch-change-me-a", "quota_mb": 0}
    ]
}

app = FastAPI(title="archive-node", version=VERSION)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2),
                               encoding="utf-8")
        print(f"[archive-node] 已生成默认配置，管理员 key：{DEFAULT_CONFIG['admin_key']}",
              flush=True)
        return json.loads(json.dumps(DEFAULT_CONFIG))
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def get_source(key: str | None) -> dict:
    if not key:
        raise HTTPException(401, "missing X-Archive-Key")
    for s in load_config().get("sources", []):
        if s.get("key") == key:
            return s
    raise HTTPException(401, "invalid archive key")


def admin_auth(key: str | None) -> None:
    if not key or key != load_config().get("admin_key"):
        raise HTTPException(401, "invalid admin key")


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


def audit_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("""CREATE TABLE IF NOT EXISTS audit_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        actor_type TEXT NOT NULL,
        actor TEXT NOT NULL,
        action TEXT NOT NULL,
        target TEXT NOT NULL,
        detail TEXT DEFAULT '')""")
    return conn


def audit(actor_type: str, actor: str, action: str, target: str, detail: str = ""):
    """写操作审计日志（失败不影响业务）；每次写入顺带清理超过保留期的旧记录。"""
    try:
        days = int(load_config().get("audit_retention_days", 30) or 30)
        now = int(time.time())
        conn = audit_db()
        conn.execute(
            "INSERT INTO audit_log(ts,actor_type,actor,action,target,detail) VALUES(?,?,?,?,?,?)",
            (now, actor_type, actor, action, target, detail))
        conn.execute("DELETE FROM audit_log WHERE ts < ?", (now - days * 86400,))
        conn.commit()
        conn.close()
    except Exception:
        pass


def source_usage(source: str) -> int:
    d = DATA_DIR / source
    if not d.exists():
        return 0
    return sum(f.stat().st_size for f in d.rglob("*") if f.is_file())


# ==================== 归档协议（来源机） ====================

@app.get("/health")
async def health():
    cfg = load_config()
    return {"ok": True, "version": VERSION,
            "sources": [s.get("name") for s in cfg.get("sources", [])]}


@app.get("/archive/whoami")
async def archive_whoami(x_archive_key: str | None = Header(default=None)):
    s = get_source(x_archive_key)
    return {"ok": True, "source": s.get("name"), "version": VERSION}


@app.post("/archive/push")
async def archive_push(request: Request,
                       fname: str, sha256: str, size: int,
                       source: str = "", mtime: int = 0, task_id: str = "",
                       x_archive_key: str | None = Header(default=None)):
    """推送归档：流式收文件 → sha256 校验 → 落盘 → 记清单（幂等）。"""
    s = get_source(x_archive_key)
    if source and source != s.get("name"):
        raise HTTPException(403, f"source name mismatch: expect {s.get('name')}")
    source = s.get("name")
    fname = Path(fname).name
    if not fname or fname.startswith("."):
        raise HTTPException(400, "bad fname")
    quota_mb = int(s.get("quota_mb", 0) or 0)
    quota = quota_mb * 1024 * 1024
    if quota > 0 and source_usage(source) + size > quota:
        raise HTTPException(507, f"quota exceeded ({quota_mb}MB)")

    target = source_dir(source) / fname
    tmp = target.with_suffix(target.suffix + ".part")
    h = hashlib.sha256()
    n = 0
    with open(tmp, "wb") as f:
        async for chunk in request.stream():
            n += len(chunk)
            h.update(chunk)
            f.write(chunk)
            if quota > 0 and n > quota:
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

    dedup = target.exists()
    if not dedup:
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
        os.utime(target, (mtime, mtime))
    audit("source", source, "overwrite" if dedup else "push", fname,
          f"size={n}; sha256={digest[:16]}")
    return {"ok": True, "dedup": dedup, "fname": fname, "sha256": digest,
            "usage_bytes": source_usage(source)}


# -------------------- 分片上传（大文件专用：平台预览代理限制上传 body ~64KB） --------------------
# 协议：POST /archive/push_chunk (fname,upload_id,seq,size,sha256) 逐片上传，每片 <64KB
#       GET  /archive/push_status?upload_id=X 查已收片序号（断点续传）
#       POST /archive/push_commit (fname,upload_id,size,sha256[,mtime,task_id])
#            按 seq 拼装 → 整体 sha256 校验 → 落盘记账（账务语义与 push 完全一致）
# 分片暂存 data/_tmp/{source}/{upload_id}/{seq:06d}.part，commit 后即清；
# 未 commit 的残留由 push_chunk 顺带清理（超 24h 视为废弃）。

_UPLOAD_ID_RE = re.compile(r"^[0-9a-f]{8,64}$")
CHUNK_LOCKS: dict = {}          # upload_id -> asyncio.Lock，commit 串行化
CHUNK_EXPIRE = 24 * 3600


def _chunk_tmp_dir(source: str, upload_id: str, create: bool = False) -> Path:
    if not _UPLOAD_ID_RE.match(upload_id or ""):
        raise HTTPException(400, "bad upload_id")
    d = DATA_DIR / "_tmp" / source / upload_id
    if create:
        d.mkdir(parents=True, exist_ok=True)
    return d


def _cleanup_stale_uploads(source: str) -> None:
    """清理该来源机下超期未 commit 的分片暂存（防上传中断垃圾堆积）。"""
    base = DATA_DIR / "_tmp" / source
    if not base.is_dir():
        return
    now = time.time()
    for d in base.iterdir():
        if not d.is_dir():
            continue
        try:
            if now - d.stat().st_mtime > CHUNK_EXPIRE:
                shutil.rmtree(d, ignore_errors=True)
        except OSError:
            pass


@app.post("/archive/push_chunk")
async def archive_push_chunk(request: Request,
                             fname: str, upload_id: str, seq: int,
                             x_archive_key: str | None = Header(default=None)):
    """收单个分片（≤64KB，规避代理上传限制）。片数据完整性由 commit 整体校验兜底。"""
    s = get_source(x_archive_key)
    source = s.get("name")
    fname = Path(fname).name
    if not fname or fname.startswith("."):
        raise HTTPException(400, "bad fname")
    if seq < 0 or seq > 10_000_000:
        raise HTTPException(400, "bad seq")
    d = _chunk_tmp_dir(source, upload_id, create=True)
    _cleanup_stale_uploads(source)
    part = d / f"{seq:06d}.part"
    tmp = part.with_suffix(".part.tmp")
    n = 0
    with open(tmp, "wb") as f:
        async for chunk in request.stream():
            n += len(chunk)
            if n > 128 * 1024:
                f.close()
                tmp.unlink(missing_ok=True)
                raise HTTPException(400, "chunk too large")
            f.write(chunk)
    tmp.rename(part)
    return {"ok": True, "seq": seq, "bytes": n}


@app.get("/archive/push_status")
async def archive_push_status(upload_id: str, fname: str,
                              x_archive_key: str | None = Header(default=None)):
    """查询某次分片上传已收到的片序号（断点续传：只补缺失片）。"""
    s = get_source(x_archive_key)
    fname = Path(fname).name
    try:
        d = _chunk_tmp_dir(s.get("name"), upload_id)
    except HTTPException:
        raise
    got = sorted(int(p.stem) for p in d.glob("*.part")) if d.is_dir() else []
    return {"ok": True, "upload_id": upload_id, "received": got}


@app.post("/archive/push_commit")
async def archive_push_commit(fname: str, upload_id: str, size: int, sha256: str,
                              mtime: int = 0, task_id: str = "",
                              x_archive_key: str | None = Header(default=None)):
    """拼装分片 → 整体校验 → 落盘记账。账务语义与 /archive/push 一致（幂等、dedup）。"""
    s = get_source(x_archive_key)
    source = s.get("name")
    fname = Path(fname).name
    if not fname or fname.startswith("."):
        raise HTTPException(400, "bad fname")
    d = _chunk_tmp_dir(source, upload_id)
    lock = CHUNK_LOCKS.setdefault(upload_id, asyncio.Lock())
    async with lock:
        parts = sorted(d.glob("*.part")) if d.is_dir() else []
        if not parts:
            raise HTTPException(400, "no chunks uploaded")
        quota_mb = int(s.get("quota_mb", 0) or 0)
        quota = quota_mb * 1024 * 1024
        if quota > 0 and source_usage(source) + size > quota:
            raise HTTPException(507, f"quota exceeded ({quota_mb}MB)")
        target = source_dir(source) / fname
        tmp = target.with_suffix(target.suffix + ".part")
        h = hashlib.sha256()
        n = 0
        with open(tmp, "wb") as f:
            for p in parts:
                data = p.read_bytes()
                n += len(data)
                if n > size:
                    tmp.unlink(missing_ok=True)
                    raise HTTPException(400, f"size mismatch: got {n} expect {size}")
                h.update(data)
                f.write(data)
        if n != size:
            tmp.unlink(missing_ok=True)
            raise HTTPException(400, f"size mismatch: got {n} expect {size}")
        digest = h.hexdigest()
        if digest != sha256:
            tmp.unlink(missing_ok=True)
            raise HTTPException(400, f"sha256 mismatch: got {digest} expect {sha256}")
        dedup = target.exists()
        if dedup:
            tmp.unlink(missing_ok=True)   # 已有同位置文件：只更新账目，不动磁盘
        else:
            tmp.rename(target)
        shutil.rmtree(d, ignore_errors=True)
        CHUNK_LOCKS.pop(upload_id, None)
    now = int(time.time())
    conn = db()
    conn.execute("""INSERT INTO archive_files(source,fname,sha256,size,mtime,task_id,pushed_at)
                    VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(source,fname) DO UPDATE SET
                      sha256=excluded.sha256, size=excluded.size,
                      mtime=excluded.mtime, task_id=excluded.task_id,
                      pushed_at=excluded.pushed_at""",
                 (source, fname, sha256, size, mtime, task_id, now))
    conn.commit()
    conn.close()
    if mtime:
        os.utime(target, (mtime, mtime))
    audit("source", source, "overwrite" if dedup else "push", fname,
          f"size={size}; sha256={sha256[:16]}; chunks={len(parts)}")
    print(f"[archive-node] 分片归档 {source}/{fname} ({size}B, {len(parts)}片, dedup={dedup})",
          flush=True)
    return {"ok": True, "dedup": dedup, "fname": fname, "sha256": sha256,
            "usage_bytes": source_usage(source)}


@app.get("/archive/file/{source}/{fname}")
async def archive_file(source: str, fname: str,
                       x_archive_key: str | None = Header(default=None)):
    s = get_source(x_archive_key)
    if source != s.get("name"):
        raise HTTPException(403, "source mismatch")
    fname = Path(fname).name
    source = Path(source).name
    target = DATA_DIR / source / fname
    if not target.is_file():
        raise HTTPException(404, "not archived")
    audit("source", s.get("name", ""), "fetch", f"{source}/{fname}")
    return FileResponse(target, filename=fname)


@app.get("/archive/list/{source}")
async def archive_list(source: str,
                       x_archive_key: str | None = Header(default=None)):
    s = get_source(x_archive_key)
    if source != s.get("name"):
        raise HTTPException(403, "source mismatch")
    source = Path(source).name
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
        qm = int(s.get("quota_mb", 0) or 0)
        out.append({"name": name, "files": cnt, "usage_bytes": used,
                    "quota_bytes": qm * 1024 * 1024 if qm > 0
                    else shutil.disk_usage(BASE).total})
    return {"ok": True, "version": VERSION, "sources": out}


# ==================== 管理台 ====================

@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    return ADMIN_HTML.replace("<body>", "<body>" + sys_nav("admin"), 1)


@app.get("/admin/api/overview")
async def admin_overview(x_admin_key: str | None = Header(default=None)):
    admin_auth(x_admin_key)
    du = shutil.disk_usage(BASE)
    cfg = load_config()
    conn = db()
    sources = []
    for s in cfg.get("sources", []):
        name = s.get("name")
        row = conn.execute(
            "SELECT count(*), coalesce(sum(size),0), coalesce(max(pushed_at),0) "
            "FROM archive_files WHERE source=?", (name,)).fetchone()
        sources.append({"name": name, "quota_mb": int(s.get("quota_mb", 0) or 0),
                        "files": row[0], "usage_bytes": row[1],
                        "last_push": row[2],
                        "disk_usage_bytes": source_usage(name)})
    conn.close()
    total_files = sum(x["files"] for x in sources)
    return {"ok": True, "version": VERSION, "uptime_s": int(time.time() - START_TS),
            "disk": {"total": du.total, "used": du.used, "free": du.free},
            "archive_bytes": sum(x["usage_bytes"] for x in sources),
            "total_files": total_files, "sources": sources}


@app.get("/admin/api/files/{source}")
async def admin_files(source: str, page: int = 1, page_size: int = 50,
                      x_admin_key: str | None = Header(default=None)):
    admin_auth(x_admin_key)
    page = max(1, page)
    page_size = min(max(10, page_size), 200)
    conn = db()
    total = conn.execute("SELECT count(*) FROM archive_files WHERE source=?",
                         (source,)).fetchone()[0]
    rows = conn.execute(
        "SELECT fname,sha256,size,mtime,task_id,pushed_at FROM archive_files "
        "WHERE source=? ORDER BY pushed_at DESC LIMIT ? OFFSET ?",
        (source, page_size, (page - 1) * page_size)).fetchall()
    conn.close()
    return {"ok": True, "source": source, "total": total, "page": page,
            "page_size": page_size,
            "files": [{"fname": r[0], "sha256": r[1], "size": r[2], "mtime": r[3],
                       "task_id": r[4], "pushed_at": r[5]} for r in rows]}


@app.get("/admin/api/file/{source}/{fname}/dl")
async def admin_file_dl(source: str, fname: str,
                        x_admin_key: str | None = Header(default=None)):
    admin_auth(x_admin_key)
    target = DATA_DIR / Path(source).name / Path(fname).name
    if not target.is_file():
        raise HTTPException(404, "not found")
    audit("admin", "admin", "download", f"{source}/{fname}")
    return FileResponse(target, filename=target.name)


async def _delete_one(source: str, fname: str) -> bool:
    target = DATA_DIR / Path(source).name / Path(fname).name
    existed = target.is_file()
    target.unlink(missing_ok=True)
    conn = db()
    conn.execute("DELETE FROM archive_files WHERE source=? AND fname=?", (source, fname))
    conn.commit()
    conn.close()
    return existed


@app.post("/admin/api/file/{source}/{fname}/delete")
async def admin_file_delete(source: str, fname: str,
                            x_admin_key: str | None = Header(default=None)):
    """删除单个归档文件（磁盘+清单）。注意：来源机回源将 404。"""
    admin_auth(x_admin_key)
    existed = await _delete_one(source, fname)
    audit("admin", "admin", "delete", f"{source}/{fname}", f"existed={existed}")
    return {"ok": True, "existed": existed}


@app.post("/admin/api/source/{source}/clear")
async def admin_source_clear(source: str,
                             x_admin_key: str | None = Header(default=None)):
    """清空某来源机的全部归档文件。"""
    admin_auth(x_admin_key)
    d = DATA_DIR / Path(source).name
    n = 0
    if d.exists():
        for f in d.rglob("*"):
            if f.is_file():
                f.unlink(missing_ok=True)
                n += 1
        try:
            d.rmdir()
        except OSError:
            pass
    conn = db()
    conn.execute("DELETE FROM archive_files WHERE source=?", (source,))
    conn.commit()
    conn.close()
    audit("admin", "admin", "clear", source, f"deleted={n}")
    return {"ok": True, "deleted": n}


@app.get("/admin/api/sources")
async def admin_sources_get(x_admin_key: str | None = Header(default=None)):
    admin_auth(x_admin_key)
    cfg = load_config()
    return {"ok": True, "admin_key": cfg.get("admin_key", ""),
            "sources": cfg.get("sources", [])}


@app.post("/admin/api/sources")
async def admin_sources_save(request: Request,
                             x_admin_key: str | None = Header(default=None)):
    """保存来源机关联配置（整表替换，写回 config.json 立即生效）。

    body: {"sources":[{"name","key","quota_mb"}], "admin_key": 可选改管理 key}（quota_mb=0 不限）
    """
    admin_auth(x_admin_key)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "bad json body")
    cfg = load_config()
    sources = []
    for i, s in enumerate(body.get("sources", [])):
        name = (s.get("name") or "").strip()
        key = (s.get("key") or "").strip()
        if not name or not key:
            continue
        qm = int(s.get("quota_mb") or 0)
        sources.append({"name": name, "key": key, "quota_mb": max(0, qm)})
    cfg["sources"] = sources
    nk = (body.get("admin_key") or "").strip()
    if nk:
        cfg["admin_key"] = nk
    save_config(cfg)
    audit("admin", "admin", "sources_save", "",
          f"count={len(sources)}" + ("; admin_key_changed" if nk else ""))
    return {"ok": True, "count": len(sources)}


# -------------------- 操作审计日志（查询/导出/清空） --------------------

AUDIT_ACTIONS = ("push", "overwrite", "fetch", "download", "delete", "clear",
                 "upload", "save", "mkdir", "touch", "rename", "move", "copy",
                 "unarchive", "kill", "sources_save", "audit_clear")


def _audit_where(actor: str, action: str, q: str):
    where, args = [], []
    if actor:
        where.append("actor=?")
        args.append(actor)
    if action:
        where.append("action=?")
        args.append(action)
    if q:
        where.append("(target LIKE ? OR detail LIKE ? OR actor LIKE ?)")
        args += [f"%{q}%"] * 3
    return (" WHERE " + " AND ".join(where)) if where else "", args


@app.get("/admin/api/audit")
async def admin_audit(page: int = 1, page_size: int = 50, actor: str = "",
                      action: str = "", q: str = "",
                      x_admin_key: str | None = Header(default=None)):
    """查询操作审计日志（倒序分页，支持 actor/action/关键词过滤）。"""
    admin_auth(x_admin_key)
    page = max(1, page)
    page_size = min(max(10, page_size), 200)
    w, args = _audit_where(actor, action, q)
    cfg = load_config()
    days = int(cfg.get("audit_retention_days", 30) or 30)
    conn = audit_db()
    total = conn.execute("SELECT count(*) FROM audit_log" + w, args).fetchone()[0]
    rows = conn.execute(
        "SELECT ts,actor_type,actor,action,target,detail FROM audit_log" + w +
        " ORDER BY id DESC LIMIT ? OFFSET ?",
        args + [page_size, (page - 1) * page_size]).fetchall()
    conn.close()
    return {"ok": True, "retention_days": days, "total": total, "page": page,
            "page_size": page_size,
            "logs": [{"ts": r[0], "actor_type": r[1], "actor": r[2],
                      "action": r[3], "target": r[4], "detail": r[5]}
                     for r in rows]}


@app.get("/admin/api/audit/export")
async def admin_audit_export(actor: str = "", action: str = "", q: str = "",
                             x_admin_key: str | None = Header(default=None)):
    """按当前过滤条件导出审计日志 CSV。"""
    admin_auth(x_admin_key)
    w, args = _audit_where(actor, action, q)
    conn = audit_db()
    rows = conn.execute(
        "SELECT ts,actor_type,actor,action,target,detail FROM audit_log" + w +
        " ORDER BY id DESC", args).fetchall()
    conn.close()
    buf = io.StringIO()
    wtr = csv.writer(buf)
    wtr.writerow(["time", "actor_type", "actor", "action", "target", "detail"])
    for r in rows:
        wtr.writerow([time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r[0])),
                      r[1], r[2], r[3], r[4], r[5]])
    return Response(buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=audit-log.csv"})


@app.post("/admin/api/audit/clear")
async def admin_audit_clear(x_admin_key: str | None = Header(default=None)):
    """清空审计日志（清空动作本身会补记一条留痕）。"""
    admin_auth(x_admin_key)
    conn = audit_db()
    n = conn.execute("SELECT count(*) FROM audit_log").fetchone()[0]
    conn.execute("DELETE FROM audit_log")
    conn.commit()
    conn.close()
    audit("admin", "admin", "audit_clear", "", f"cleared={n}")
    return {"ok": True, "cleared": n}


# ==================== 管理页面 ====================

ADMIN_HTML = """<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>archive-node 管理台</title>
<style>
:root{--bg:#101418;--card:#171d23;--line:#232b33;--tx:#d8dee5;--dim:#7a8794;
--ac:#2f81f7;--ok:#00b875;--warn:#e5b567;--bad:#e5484d}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--tx);
font:14px/1.5 -apple-system,"PingFang SC","Microsoft YaHei",sans-serif}
.wrap{max-width:1080px;margin:0 auto;padding:16px}
.bar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.bar h1{font-size:17px;margin:0}
.dot{width:9px;height:9px;border-radius:50%;background:var(--ok);display:inline-block}
.tag{font-size:11px;color:var(--dim)}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;
padding:14px;margin-bottom:14px}
.card h2{font-size:14px;margin:0 0 10px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}
th{color:var(--dim);font-weight:500;font-size:12px}
input,select{background:#0d1116;border:1px solid var(--line);border-radius:6px;
color:var(--tx);padding:6px 9px;font-size:13px}
input:focus{outline:none;border-color:var(--ac)}
.btn{background:#1c242d;border:1px solid var(--line);border-radius:6px;color:var(--tx);
padding:6px 12px;cursor:pointer;font-size:13px}
.btn:hover{border-color:var(--ac)}
.btn-p{background:var(--ac);border-color:var(--ac);color:#fff}
.btn-danger{color:var(--bad)}
.qbar{height:7px;background:#0d1116;border-radius:4px;overflow:hidden;min-width:90px}
.qbar i{display:block;height:100%;background:var(--ok)}
.qbar i.hot{background:var(--warn)}.qbar i.full{background:var(--bad)}
.tiny{font-size:12px;color:var(--dim)}
.nw{white-space:nowrap}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px}
.pager{display:flex;gap:8px;align-items:center;margin-top:10px}
.mono{font-family:ui-monospace,Consolas,monospace;font-size:12px}
#loginMask{position:fixed;inset:0;background:rgba(8,10,13,.96);display:flex;
align-items:center;justify-content:center;z-index:9}
#loginMask .card{width:min(360px,92vw)}
.msg{font-size:12px;margin-left:8px}
.nav{display:flex;gap:6px;margin-bottom:14px;border-bottom:1px solid var(--line)}
.navbtn{background:transparent;border:none;border-bottom:2px solid transparent;color:var(--dim);
padding:8px 14px;cursor:pointer;font-size:14px}
.navbtn:hover{color:var(--tx)}
.navbtn.active{color:var(--tx);border-bottom-color:var(--ac)}
.view{display:none}
.view.active{display:block}
@media(max-width:640px){.wrap{padding:10px}table{font-size:12px}
th,td{padding:5px 5px}.hide-m{display:none}}
</style></head><body>
<div id="loginMask" style="display:none">
  <div class="card"><h2>archive-node 管理台</h2>
    <div class="tiny" style="margin-bottom:10px">输入管理员 key（config.json 的 admin_key）</div>
    <div class="row"><input id="loginKey" type="password" style="flex:1" placeholder="admin_key">
    <button class="btn btn-p" onclick="doLogin()">进入</button></div>
    <div id="loginMsg" class="tiny" style="color:var(--bad)"></div></div>
</div>
<div class="wrap">
  <div class="bar"><span class="dot"></span><h1>archive-node 管理台</h1>
    <span class="tag" id="verTag"></span>
    <span class="tag" id="upTag"></span>
    <span style="margin-left:auto" class="row">
      <input id="akInput" style="width:180px" placeholder="admin_key">
      <button class="btn" onclick="saveKey()">保存 key</button></span></div>

  <div class="nav" id="nav">
    <button class="navbtn active" data-view="overview" onclick="showView('overview')">资源管理</button>
    <button class="navbtn" data-view="files" onclick="showView('files')">文件管理</button>
    <button class="navbtn" data-view="sources" onclick="showView('sources')">来源机配置</button>
    <button class="navbtn" data-view="audit" onclick="showView('audit')">日志</button>
  </div>

  <div class="view active" id="view-overview">
  <div class="card"><h2>资源管理</h2><div id="ovBox" class="tiny">加载中…</div></div>
  </div>

  <div class="view" id="view-sources">
  <div class="card"><h2>来源机关联配置</h2>
    <div class="tiny" style="margin-bottom:8px">每行 = 一台来源机（dola-pool）。名称与 key 发给对方填入其面板「归档节点」即完成挂载；配额为该机可占用空间上限，填 0 表示不限制（用满磁盘）。</div>
    <div id="srcRows"></div>
    <div class="row"><button class="btn" onclick="addSrc()">＋ 添加来源机</button>
    <button class="btn btn-p" onclick="saveSrc()">保存配置</button>
    <span id="srcMsg" class="msg tiny"></span></div></div>
  </div>

  <div class="view" id="view-files">
  <div class="card"><h2>文件管理</h2>
    <div class="row">
      <select id="srcSel" onchange="loadFiles(1)"></select>
      <button class="btn btn-danger" onclick="clearSource()">清空该来源机归档</button>
      <span id="fileMsg" class="msg tiny"></span></div>
    <div id="fileBox"></div>
    <div class="pager" id="pager"></div></div>
  </div>

  <div class="view" id="view-audit">
  <div class="card"><h2>日志 <span class="tiny">（保留 <span id="audDays">30</span> 天）</span></h2>
    <div class="row">
      <select id="audActor" onchange="loadAudit(1)"><option value="">全部操作者</option></select>
      <select id="audAction" onchange="loadAudit(1)">
        <option value="">全部动作</option>
        <option value="push">归档新增(push)</option><option value="overwrite">覆盖归档(overwrite)</option>
        <option value="fetch">回源拉取(fetch)</option><option value="download">下载(download)</option>
        <option value="delete">删除(delete)</option><option value="clear">清空(clear)</option>
        <option value="upload">上传(upload)</option><option value="save">编辑保存(save)</option>
        <option value="mkdir">新建目录(mkdir)</option><option value="touch">新建文件(touch)</option>
        <option value="rename">重命名(rename)</option><option value="move">移动(move)</option>
        <option value="copy">复制(copy)</option><option value="unarchive">解压(unarchive)</option>
        <option value="kill">终止进程(kill)</option><option value="sources_save">来源机配置修改</option>
        <option value="audit_clear">清空日志</option>
      </select>
      <input id="audQ" style="flex:1;min-width:140px" placeholder="关键词（对象/详情/操作者）">
      <button class="btn btn-p" onclick="loadAudit(1)">查询</button>
      <button class="btn" onclick="q('#audQ').value='';q('#audActor').value='';q('#audAction').value='';loadAudit(1)">重置</button>
      <a class="btn" style="text-decoration:none" id="audExport" href="/admin/api/audit/export" download>导出 CSV</a>
      <button class="btn btn-danger" onclick="clearAudit()">清空日志</button>
    </div>
    <div id="audBox"></div>
    <div class="pager" id="audPager"></div></div>
  </div>
</div>
<script>
let AK=localStorage.getItem('arch_ak')||'';
const q=s=>document.querySelector(s);
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const fmtB=n=>n>=1073741824?(n/1073741824).toFixed(2)+' GB':n>=1048576?(n/1048576).toFixed(1)+' MB':(n/1024).toFixed(1)+' KB';
const fmtT=ts=>ts?new Date(ts*1000).toLocaleString():'—';
async function api(path,opt={}){
  const b=(opt.body&&typeof opt.body==='object')?JSON.stringify(opt.body):opt.body;
  const r=await fetch(path,{...opt,body:b,headers:{'X-Admin-Key':AK,'Content-Type':'application/json',...(opt.headers||{})}});
  if(r.status===401){q('#loginMask').style.display='flex';throw new Error('unauthorized');}
  if(!r.ok)throw new Error((await r.json().catch(()=>({}))).detail||('HTTP '+r.status));
  return r.json();
}
async function doLogin(){
  AK=q('#loginKey').value.trim();
  try{await api('/admin/api/overview');localStorage.setItem('arch_ak',AK);
    q('#loginMask').style.display='none';boot();}
  catch(e){q('#loginMsg').textContent='key 无效';}
}
function saveKey(){AK=q('#akInput').value.trim();localStorage.setItem('arch_ak',AK);
  q('#akInput').value='';boot();}
function bar(used,quota){const p=Math.min(100,quota?used/quota*100:0);
  const cls=p>=90?'full':p>=70?'hot':'';
  return `<div style="display:flex;gap:6px;align-items:center"><div class="qbar" style="flex:1"><i class="${cls}" style="width:${p}%"></i></div><span class="tiny">${p.toFixed(0)}%</span></div>`;}
async function loadOverview(){
  const o=await api('/admin/api/overview');
  q('#verTag').textContent='v'+o.version;
  const up=o.uptime_s,uh=Math.floor(up/3600),um=Math.floor(up%3600/60);
  q('#upTag').textContent=`已运行 ${uh}时${um}分`;
  const d=o.disk,dp=(d.used/d.total*100).toFixed(1);
  q('#ovBox').innerHTML=`
    <div class="row" style="gap:16px">
      <span>磁盘 <b>${fmtB(d.used)}</b> / ${fmtB(d.total)}（已用 ${dp}%，剩 ${fmtB(d.free)}）</span>
      <span>归档总量 <b>${fmtB(o.archive_bytes)}</b> · ${o.total_files} 个文件</span></div>
    <table><tr><th>来源机</th><th>文件数</th><th>占用/配额</th><th style="width:140px">配额使用</th><th class="hide-m">最近推送</th></tr>
    ${o.sources.map(s=>`<tr><td><b>${esc(s.name)}</b></td><td>${s.files}</td>
      <td>${fmtB(s.usage_bytes)} / ${s.quota_mb?s.quota_mb+' MB':'不限制'}</td>
      <td>${bar(s.usage_bytes,s.quota_mb?s.quota_mb*1048576:o.disk.total)}</td>
      <td class="hide-m">${fmtT(s.last_push)}</td></tr>`).join('')||'<tr><td colspan="5" class="tiny">尚未登记来源机</td></tr>'}
    </table>`;
}
let SRC=[];
async function loadSources(){
  const r=await api('/admin/api/sources');SRC=r.sources||[];
  q('#srcRows').innerHTML=SRC.length?SRC.map((s,i)=>`
    <div class="row src-row">
      <input class="s-name" style="width:130px" placeholder="名称（如 A机）" value="${esc(s.name)}">
      <input class="s-key mono" style="flex:1;min-width:180px" placeholder="key" value="${esc(s.key)}">
      <input class="s-quota" style="width:110px" placeholder="配额MB(0=不限)" value="${s.quota_mb??0}">
      <button class="btn btn-danger" onclick="this.parentElement.remove()">删除</button></div>`).join('')
    :'<div class="tiny" style="margin-bottom:8px">尚未登记来源机</div>';
  const opts=SRC.map(s=>`<option>${esc(s.name)}</option>`).join('');
  q('#srcSel').innerHTML=opts||'<option value="">（无来源机）</option>';
  q('#audActor').innerHTML='<option value="">全部操作者</option><option value="admin">admin（管理台）</option>'
    +SRC.map(s=>`<option>${esc(s.name)}</option>`).join('');
  q('#akInput').placeholder='修改 admin_key';
}
function addSrc(){const div=document.createElement('div');div.className='row src-row';
  div.innerHTML=`<input class="s-name" style="width:130px" placeholder="名称（如 A机）">
    <input class="s-key mono" style="flex:1;min-width:180px" placeholder="key">
    <input class="s-quota" style="width:110px" placeholder="配额MB(0=不限)" value="0">
    <button class="btn btn-danger" onclick="this.parentElement.remove()">删除</button>`;
  q('#srcRows').appendChild(div);}
async function saveSrc(){
  const sources=[...q('#srcRows').querySelectorAll('.src-row')].map(r=>({
    name:r.querySelector('.s-name').value.trim(),
    key:r.querySelector('.s-key').value.trim(),
    quota_mb:(n=>Number.isFinite(n)&&n>0?n:0)(parseInt(r.querySelector('.s-quota').value))
  })).filter(s=>s.name&&s.key);
  await api('/admin/api/sources',{method:'POST',body:{sources,admin_key:''}});
  const m=q('#srcMsg');m.textContent='已保存（写回 config.json，立即生效）';
  setTimeout(()=>m.textContent='',3000);
  await loadSources();await loadOverview();}
let curPage=1;
async function loadFiles(p){
  curPage=p||1;const src=q('#srcSel').value;
  if(!src){q('#fileBox').innerHTML='<div class="tiny">无来源机</div>';q('#pager').innerHTML='';return;}
  const r=await api(`/admin/api/files/${encodeURIComponent(src)}?page=${curPage}&page_size=50`);
  q('#fileBox').innerHTML=`<table><tr><th>文件名</th><th>大小</th><th class="hide-m">归档时间</th><th class="hide-m">任务</th><th>操作</th></tr>
    ${r.files.map(f=>`<tr><td class="mono" title="${esc(f.sha256)}">${esc(f.fname)}</td>
      <td>${fmtB(f.size)}</td><td class="hide-m">${fmtT(f.pushed_at)}</td>
      <td class="hide-m tiny">${esc(f.task_id||'')}</td>
      <td><a class="btn" style="text-decoration:none" href="/admin/api/file/${encodeURIComponent(src)}/${encodeURIComponent(f.fname)}/dl" download>下载</a>
      <button class="btn btn-danger" onclick="delFile('${esc(src)}','${esc(f.fname)}')">删除</button></td></tr>`).join('')
    ||'<tr><td colspan="5" class="tiny">暂无归档文件</td></tr>'}</table>`;
  const pages=Math.max(1,Math.ceil(r.total/r.page_size));
  q('#pager').innerHTML=`<span class="tiny">共 ${r.total} 个 · 第 ${r.page}/${pages} 页</span>
    ${r.page>1?`<button class="btn" onclick="loadFiles(${r.page-1})">上一页</button>`:''}
    ${r.page<pages?`<button class="btn" onclick="loadFiles(${r.page+1})">下一页</button>`:''}`;
}
async function delFile(src,fname){
  if(!confirm('确认删除 '+fname+'？（来源机回源将失效）'))return;
  await api(`/admin/api/file/${encodeURIComponent(src)}/${encodeURIComponent(fname)}/delete`,{method:'POST'});
  loadFiles(curPage);loadOverview();}
async function clearSource(){
  const src=q('#srcSel').value;if(!src)return;
  if(!confirm('确认清空来源机「'+src+'」的全部归档文件？'))return;
  const r=await api(`/admin/api/source/${encodeURIComponent(src)}/clear`,{method:'POST'});
  const m=q('#fileMsg');m.textContent=`已删除 ${r.deleted} 个文件`;
  setTimeout(()=>m.textContent='',4000);loadFiles(1);loadOverview();}
function showView(name){
  document.querySelectorAll('.view').forEach(v=>v.classList.toggle('active',v.id==='view-'+name));
  document.querySelectorAll('.navbtn').forEach(b=>b.classList.toggle('active',b.dataset.view===name));
  if(name==='files')loadFiles(curPage).catch(()=>{});
  if(name==='audit')loadAudit(audPage).catch(()=>{});
}
const AUD_ACT={push:'归档新增',overwrite:'覆盖归档',fetch:'回源拉取',download:'下载',delete:'删除文件',
  clear:'清空来源机',upload:'上传文件',save:'编辑保存',mkdir:'新建目录',touch:'新建文件',rename:'重命名',
  move:'移动',copy:'复制',unarchive:'解压',kill:'终止进程',sources_save:'来源机配置修改',audit_clear:'清空日志'};
let audPage=1;
function audQs(extra){const a=encodeURIComponent(q('#audActor').value),ac=encodeURIComponent(q('#audAction').value),
  kw=encodeURIComponent(q('#audQ').value.trim());return `${extra?'page='+audPage+'&':''}actor=${a}&action=${ac}&q=${kw}`;}
async function loadAudit(p){
  audPage=p||audPage||1;
  q('#audExport').href='/admin/api/audit/export?'+audQs(false);
  const r=await api('/admin/api/audit?'+audQs(true));
  q('#audDays').textContent=r.retention_days??30;
  const pages=Math.max(1,Math.ceil(r.total/r.page_size));
  q('#audBox').innerHTML=`<table><tr><th class="hide-m">时间</th><th>操作者</th><th>动作</th><th>对象</th><th class="hide-m">详情</th></tr>
    ${r.logs.map(x=>`<tr><td class="hide-m">${fmtT(x.ts)}</td>
      <td class="nw"><b>${x.actor_type==='admin'?'归档机':esc(x.actor)}</b></td>
      <td class="nw">${AUD_ACT[x.action]||esc(x.action)}</td>
      <td class="mono">${x.target?esc(x.target).split('; ').join('<br>'):'—'}</td>
      <td class="tiny hide-m">${esc(x.detail)||'—'}</td></tr>`).join('')
    ||'<tr><td colspan="5" class="tiny">暂无日志</td></tr>'}</table>`;
  q('#audPager').innerHTML=`<span class="tiny">共 ${r.total} 条 · 第 ${r.page}/${pages} 页</span>
    ${r.page>1?`<button class="btn" onclick="loadAudit(${r.page-1})">上一页</button>`:''}
    ${r.page<pages?`<button class="btn" onclick="loadAudit(${r.page+1})">下一页</button>`:''}`;
}
async function clearAudit(){
  if(!confirm('确认清空全部操作日志？（本条清空动作本身会留痕）'))return;
  await api('/admin/api/audit/clear',{method:'POST'});
  loadAudit(1);
}
function boot(){loadOverview().catch(()=>{});loadSources().then(()=>loadFiles(1)).catch(()=>{});}
q('#audQ').addEventListener('keydown',e=>{if(e.key==='Enter')loadAudit(1);});
if(AK){api('/admin/api/overview').then(()=>boot()).catch(()=>{});
  q('#akInput').addEventListener('change',saveKey);}
else q('#loginMask').style.display='flex';
</script></body></html>"""


# ==================== 顶部系统菜单 + 服务器文件管理/资源监控 ====================

SYS_NAV_CSS = """<style>
.sysnav{display:flex;gap:4px;padding:8px 16px;background:#0d1116;border-bottom:1px solid #2a3342;
flex-wrap:wrap}
.sysnav a{color:#8a94a6;text-decoration:none;padding:6px 14px;border-radius:6px;
font:14px/1.5 -apple-system,"PingFang SC","Microsoft YaHei",sans-serif}
.sysnav a:hover{color:#d7dde6;background:#1c2330}
.sysnav a.on{color:#4a9eff;background:#1c2330}
</style>"""


def sys_nav(active: str) -> str:
    items = [("admin", "归档机管理"), ("fm", "文件管理"), ("mon", "资源管理")]
    links = ""
    for k, name in items:
        cls = ' class="on"' if k == active else ""
        links += f'<a href="/{k}"{cls}>{name}</a>'
    return SYS_NAV_CSS + f'<div class="sysnav">{links}</div>'


def _page_with_nav(filename: str, active: str) -> str:
    html = (WEB_DIR / filename).read_text(encoding="utf-8")
    nav = sys_nav(active)
    return html.replace("<body>", "<body>" + nav, 1) if "<body>" in html else nav + html


@app.get("/fm", response_class=HTMLResponse)
async def fm_page():
    return _page_with_nav("fm.html", "fm")


@app.get("/mon", response_class=HTMLResponse)
async def mon_page():
    return _page_with_nav("mon.html", "mon")


from tools_api import router as tools_router  # noqa: E402

app.include_router(tools_router)


if __name__ == "__main__":
    import uvicorn
    cfg = load_config()
    uvicorn.run(app, host=cfg.get("listen_host", "0.0.0.0"),
                port=int(cfg.get("listen_port", 8100)),
                access_log=False)
