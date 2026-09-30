"""内置于 archive-node 的文件管理器 + 资源监控子路由（与管理台共用 admin_key）。

页面：/fm、/mon（由 server.py 注入顶部系统菜单后提供）。
API：/api/list|read|save|mkdir|touch|delete|rename|copy|move|upload|download|
     preview|unarchive|search（文件管理），/api/stats|kill|disk/*（资源监控）。
鉴权：请求头 X-Admin-Key（或 ?key= 查询参数，供下载直链）须等于 config.json 的 admin_key。
跨平台：全部基于 psutil / os.path；Windows 下 load 用 psutil.getloadavg 模拟值、
根目录列为盘符列表，CPU 型号回退 PROCESSOR_IDENTIFIER。
"""
import json
import mimetypes
import os
import platform
import shutil
import socket
import stat
import tarfile
import tempfile
import threading
import time
import zipfile
from collections import deque
from pathlib import Path

import psutil
from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAX_EDIT = 5 * 1024 * 1024
HIST_MAX = 900

router = APIRouter()

hist = deque(maxlen=HIST_MAX)
hist_lock = threading.Lock()
_proc_cache: dict = {}


class PathBody(BaseModel):
    path: str


class SaveBody(BaseModel):
    path: str
    content: str
    encoding: str = "utf-8"


class DeleteBody(BaseModel):
    paths: list[str]


class RenameBody(BaseModel):
    path: str
    new_name: str


class TransferBody(BaseModel):
    srcs: list[str]
    dest: str


class UnarchiveBody(BaseModel):
    path: str
    dest: str = ""


class SearchBody(BaseModel):
    dir: str
    q: str
    limit: int = 300


class KillBody(BaseModel):
    pid: int


def _admin_auth(request: Request):
    """与归档机管理台同源认证：X-Admin-Key 请求头或 ?key= 查询参数（下载直链用）。"""
    from server import load_config
    admin_key = load_config().get("admin_key", "")
    key = request.headers.get("x-admin-key") or request.query_params.get("key")
    if not admin_key:
        return
    if key != admin_key:
        raise HTTPException(401, "管理密钥错误")


def _abs(p: str) -> str:
    if not p or not p.strip():
        raise HTTPException(400, "路径为空")
    return os.path.realpath(os.path.abspath(p))


def _check_exists(p: str):
    if not os.path.exists(p):
        raise HTTPException(404, f"不存在: {p}")


def _fail(msg: str):
    raise HTTPException(400, msg)


@router.get("/api/list")
async def api_list(request: Request, path: str = "/"):
    _admin_auth(request)
    p = _abs(path)
    _check_exists(p)
    if not os.path.isdir(p):
        _fail(f"不是目录: {p}")
    entries = []
    try:
        names = os.listdir(p)
    except PermissionError:
        raise HTTPException(403, f"权限不足: {p}")
    for name in names:
        full = os.path.join(p, name)
        try:
            st = os.lstat(full)
            entries.append({
                "name": name,
                "is_dir": os.path.isdir(full) and not os.path.islink(full),
                "is_link": os.path.islink(full),
                "size": 0 if os.path.isdir(full) else st.st_size,
                "mtime": int(st.st_mtime),
                "mode": stat.filemode(st.st_mode),
            })
        except (PermissionError, OSError):
            entries.append({"name": name, "is_dir": False, "is_link": False,
                            "size": 0, "mtime": 0, "mode": "???", "error": True})
    entries.sort(key=lambda e: (not e["is_dir"], e["name"].lower()))
    return {"path": p, "entries": entries}


@router.get("/api/read")
async def api_read(request: Request, path: str):
    _admin_auth(request)
    p = _abs(path)
    _check_exists(p)
    if os.path.isdir(p):
        _fail("这是目录")
    st = os.stat(p)
    if st.st_size > MAX_EDIT:
        return {"path": p, "binary": True, "too_big": True, "size": st.st_size}
    with open(p, "rb") as f:
        data = f.read(MAX_EDIT + 1)
    if b"\0" in data[:8192]:
        return {"path": p, "binary": True, "size": st.st_size}
    for enc in ("utf-8", "gbk", "latin-1"):
        try:
            text = data.decode(enc)
            return {"path": p, "binary": False, "content": text,
                    "encoding": enc, "size": st.st_size, "mtime": int(st.st_mtime)}
        except (UnicodeDecodeError, ValueError):
            continue
    return {"path": p, "binary": True, "size": st.st_size}


@router.post("/api/save")
async def api_save(request: Request, body: SaveBody):
    _admin_auth(request)
    p = _abs(body.path)
    _check_exists(os.path.dirname(p))
    if body.encoding not in ("utf-8", "gbk", "latin-1"):
        _fail("不支持的编码")
    tmp = p + ".fbtmp"
    with open(tmp, "wb") as f:
        f.write(body.content.encode(body.encoding, errors="replace"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)
    return {"ok": True, "path": p, "size": os.path.getsize(p)}


@router.post("/api/mkdir")
async def api_mkdir(request: Request, body: PathBody):
    _admin_auth(request)
    p = _abs(body.path)
    if os.path.exists(p):
        _fail("已存在")
    os.makedirs(p)
    return {"ok": True, "path": p}


@router.post("/api/touch")
async def api_touch(request: Request, body: PathBody):
    _admin_auth(request)
    p = _abs(body.path)
    if os.path.exists(p):
        _fail("已存在")
    Path(p).touch()
    return {"ok": True, "path": p}


@router.post("/api/delete")
async def api_delete(request: Request, body: DeleteBody):
    _admin_auth(request)
    if not body.paths:
        _fail("未选择")
    done, errors = [], []
    for raw in body.paths:
        p = _abs(raw)
        if p == "/" or os.path.dirname(p) == p:
            errors.append({"path": raw, "error": "拒绝删除根目录"})
            continue
        try:
            if os.path.isdir(p) and not os.path.islink(p):
                shutil.rmtree(p)
            else:
                os.remove(p)
            done.append(p)
        except Exception as e:
            errors.append({"path": raw, "error": str(e)})
    return {"ok": len(errors) == 0, "deleted": done, "errors": errors}


@router.post("/api/rename")
async def api_rename(request: Request, body: RenameBody):
    _admin_auth(request)
    p = _abs(body.path)
    _check_exists(p)
    new_name = body.new_name.strip()
    if not new_name or "/" in new_name or new_name in (".", ".."):
        _fail("非法名称")
    np = os.path.join(os.path.dirname(p), new_name)
    if os.path.exists(np):
        _fail("目标已存在")
    os.rename(p, np)
    return {"ok": True, "path": np}


def _unique_dest(dest_dir: str, name: str) -> str:
    target = os.path.join(dest_dir, name)
    if not os.path.exists(target):
        return target
    base, ext = os.path.splitext(name)
    i = 1
    while os.path.exists(os.path.join(dest_dir, f"{base}({i}){ext}")):
        i += 1
    return os.path.join(dest_dir, f"{base}({i}){ext}")


@router.post("/api/copy")
async def api_copy(request: Request, body: TransferBody):
    _admin_auth(request)
    dest = _abs(body.dest)
    if not os.path.isdir(dest):
        _fail(f"目标目录不存在: {dest}")
    done, errors = [], []
    for raw in body.srcs:
        src = _abs(raw)
        _check_exists(src)
        if src == dest or dest.startswith(src + os.sep):
            errors.append({"path": raw, "error": "不能复制到自身内部"})
            continue
        target = _unique_dest(dest, os.path.basename(src))
        try:
            if os.path.isdir(src) and not os.path.islink(src):
                shutil.copytree(src, target, symlinks=True)
            else:
                shutil.copy2(src, target, follow_symlinks=False)
            done.append(target)
        except Exception as e:
            errors.append({"path": raw, "error": str(e)})
    return {"ok": len(errors) == 0, "copied": done, "errors": errors}


@router.post("/api/move")
async def api_move(request: Request, body: TransferBody):
    _admin_auth(request)
    dest = _abs(body.dest)
    if not os.path.isdir(dest):
        _fail(f"目标目录不存在: {dest}")
    done, errors = [], []
    for raw in body.srcs:
        src = _abs(raw)
        _check_exists(src)
        if src == dest or dest.startswith(src + os.sep):
            errors.append({"path": raw, "error": "不能移动到自身内部"})
            continue
        target = _unique_dest(dest, os.path.basename(src))
        try:
            shutil.move(src, target)
            done.append(target)
        except Exception as e:
            errors.append({"path": raw, "error": str(e)})
    return {"ok": len(errors) == 0, "moved": done, "errors": errors}


@router.post("/api/upload")
async def api_upload(request: Request, dest: str = Form(...), file: UploadFile = File(...)):
    _admin_auth(request)
    d = _abs(dest)
    if not os.path.isdir(d):
        _fail(f"目标目录不存在: {d}")
    target = _unique_dest(d, os.path.basename(file.filename or "upload.bin"))
    size = 0
    with open(target, "wb") as out:
        while chunk := await file.read(1024 * 1024):
            out.write(chunk)
            size += len(chunk)
    return {"ok": True, "path": target, "size": size}


@router.get("/api/download")
async def api_download(request: Request, path: str, background: BackgroundTasks):
    _admin_auth(request)
    p = _abs(path)
    _check_exists(p)
    if os.path.isfile(p):
        return FileResponse(p, filename=os.path.basename(p))
    if not os.path.isdir(p):
        _fail("不支持的对象类型")
    tmp = tempfile.mktemp(prefix="fb-dl-", suffix=".zip", dir=tempfile.gettempdir())
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(p):
            for fn in files:
                full = os.path.join(root, fn)
                zf.write(full, os.path.relpath(full, os.path.dirname(p)))
    background.add_task(os.remove, tmp)
    return FileResponse(tmp, filename=os.path.basename(p) + ".zip")


@router.get("/api/preview")
async def api_preview(request: Request, path: str):
    _admin_auth(request)
    p = _abs(path)
    _check_exists(p)
    mime, _ = mimetypes.guess_type(p)
    if not mime or not mime.startswith(("image/", "video/", "audio/", "application/pdf")):
        _fail("不支持的预览类型")
    return FileResponse(p, media_type=mime)


ARCHIVE_SUFFIXES = (".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz")


@router.post("/api/unarchive")
async def api_unarchive(request: Request, body: UnarchiveBody):
    _admin_auth(request)
    p = _abs(body.path)
    _check_exists(p)
    lower = p.lower()
    if not lower.endswith(ARCHIVE_SUFFIXES):
        _fail("不支持的压缩格式")
    dest = _abs(body.dest) if body.dest else os.path.join(
        os.path.dirname(p), os.path.basename(p).split(".")[0] or "extracted")
    os.makedirs(dest, exist_ok=True)
    try:
        if lower.endswith(".zip"):
            with zipfile.ZipFile(p) as zf:
                for m in zf.namelist():
                    t = os.path.realpath(os.path.join(dest, m))
                    if not t.startswith(dest + os.sep) and t != dest:
                        _fail("压缩包含不安全路径")
                zf.extractall(dest)
        else:
            with tarfile.open(p) as tf:
                for m in tf.getmembers():
                    t = os.path.realpath(os.path.join(dest, m.name))
                    if not t.startswith(dest + os.sep) and t != dest:
                        _fail("压缩包含不安全路径")
                tf.extractall(dest)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"解压失败: {e}")
    return {"ok": True, "dest": dest}


SKIP_DIRS = {"/proc", "/sys", "/dev", "/run"}


@router.post("/api/search")
async def api_search(request: Request, body: SearchBody):
    _admin_auth(request)
    root = _abs(body.dir)
    _check_exists(root)
    q = body.q.strip().lower()
    if not q:
        _fail("关键词为空")
    limit = max(1, min(body.limit, 1000))
    deadline = time.time() + 4.0
    results = []

    def walk(d):
        if len(results) >= limit or time.time() > deadline:
            return
        try:
            with os.scandir(d) as it:
                for e in it:
                    if len(results) >= limit or time.time() > deadline:
                        return
                    if q in e.name.lower():
                        try:
                            st = e.stat(follow_symlinks=False)
                            results.append({
                                "path": e.path, "name": e.name,
                                "is_dir": e.is_dir(follow_symlinks=False),
                                "size": 0 if e.is_dir(follow_symlinks=False) else st.st_size,
                                "mtime": int(st.st_mtime)})
                        except OSError:
                            pass
                    if e.is_dir(follow_symlinks=False):
                        if e.path not in SKIP_DIRS and not e.path.startswith(tuple(s + "/" for s in SKIP_DIRS)):
                            walk(e.path)
        except (PermissionError, OSError):
            pass

    walk(root)
    return {"ok": True, "results": results, "truncated": len(results) >= limit}


def _proc_mem_bytes() -> int:
    total = 0
    for p in psutil.process_iter():
        try:
            total += p.memory_full_info().pss
        except Exception:
            try:
                total += p.memory_info().rss
            except Exception:
                pass
    return total


def _mem():
    vm = psutil.virtual_memory()
    sm = psutil.swap_memory()
    used = _proc_mem_bytes()
    return {
        "total": vm.total, "used": used,
        "avail": max(0, vm.total - used),
        "os_avail": vm.available,
        "pct": round(used / vm.total * 100, 1) if vm.total else 0.0,
        "swap_total": sm.total, "swap_used": sm.used, "swap_pct": sm.percent,
    }


def _sample_procs():
    alive = set()
    for p in psutil.process_iter(["pid", "name", "username", "memory_percent", "status"]):
        try:
            alive.add(p.pid)
            c = _proc_cache.get(p.pid)
            if c is None:
                c = {"proc": p, "cpu": 0.0}
                _proc_cache[p.pid] = c
                p.cpu_percent(interval=None)
            else:
                c["cpu"] = p.cpu_percent(interval=None)
            c["info"] = p.info
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    for pid in [k for k in _proc_cache if k not in alive]:
        _proc_cache.pop(pid, None)


def _sample_loop():
    psutil.cpu_percent(interval=None, percpu=True)
    net = psutil.net_io_counters()
    nic = psutil.net_io_counters(pernic=True)
    disk = psutil.disk_io_counters()
    last = (time.time(), net, disk, nic)
    while True:
        time.sleep(1.0)
        now = time.time()
        net = psutil.net_io_counters()
        disk = psutil.disk_io_counters()
        nic = psutil.net_io_counters(pernic=True)
        dt = now - last[0]
        pnic = last[3]
        ifs = []
        if nic:
            for name, io in nic.items():
                p = pnic.get(name)
                ifs.append({
                    "name": name,
                    "recv": max(0.0, (io.bytes_recv - p.bytes_recv) / dt) if p else 0.0,
                    "sent": max(0.0, (io.bytes_sent - p.bytes_sent) / dt) if p else 0.0,
                    "total_recv": io.bytes_recv,
                    "total_sent": io.bytes_sent,
                })
        ifs.sort(key=lambda x: -(x["total_recv"] + x["total_sent"]))
        s = {
            "ts": int(now),
            "cpu": psutil.cpu_percent(interval=None),
            "cores": psutil.cpu_percent(interval=None, percpu=True),
            "load": _loadavg(),
            "mem": _mem(),
            "net_recv": max(0.0, (net.bytes_recv - last[1].bytes_recv) / dt) if net and last[1] else 0.0,
            "net_sent": max(0.0, (net.bytes_sent - last[1].bytes_sent) / dt) if net and last[1] else 0.0,
            "net_total_recv": net.bytes_recv if net else 0,
            "net_total_sent": net.bytes_sent if net else 0,
            "net_ifs": ifs,
            "disk_read": max(0.0, (disk.read_bytes - last[2].read_bytes) / dt) if disk and last[2] else 0.0,
            "disk_write": max(0.0, (disk.write_bytes - last[2].write_bytes) / dt) if disk and last[2] else 0.0,
            "disk_total_read": disk.read_bytes if disk else 0,
            "disk_total_write": disk.write_bytes if disk else 0,
        }
        last = (now, net, disk, nic)
        with hist_lock:
            hist.append(s)
        _sample_procs()


def _top_procs(n=20):
    by_cpu, by_mem = [], []
    for c in _proc_cache.values():
        info = c.get("info") or {}
        item = {
            "pid": info.get("pid"),
            "name": info.get("name") or "?",
            "user": info.get("username") or "?",
            "cpu": round(c.get("cpu", 0.0), 1),
            "mem": round(info.get("memory_percent") or 0.0, 1),
            "rss": 0,
            "status": info.get("status") or "",
        }
        try:
            item["rss"] = c["proc"].memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        by_cpu.append(item)
        by_mem.append(item)
    by_cpu.sort(key=lambda x: -x["cpu"])
    by_mem.sort(key=lambda x: -x["mem"])
    return by_cpu[:n], by_mem[:n]


def _disks():
    out = []
    for part in psutil.disk_partitions(all=False):
        if part.fstype in ("squashfs", "tmpfs", "devtmpfs", "overlay", "iso9660"):
            continue
        try:
            u = psutil.disk_usage(part.mountpoint)
            out.append({"mount": part.mountpoint, "dev": part.device,
                        "total": u.total, "used": u.used, "free": u.free,
                        "pct": u.percent})
        except (PermissionError, OSError):
            continue
    out.sort(key=lambda x: x["mount"])
    return out


def _scan_dir_sizes(path: str) -> dict:
    """扫描 path 下 depth-1 子目录与散文件的累计大小（字节数）。

    跳过符号链接与跨挂载点目录（du 语义），OSError 一律忽略。
    纯 Python 递归较慢（几万文件秒级~几十秒），只在后台线程里跑并缓存。
    """
    base_dev = os.stat(path).st_dev

    def tree_size(p: str) -> int:
        total = 0
        try:
            it = os.scandir(p)
        except OSError:
            return 0
        with it:
            for e in it:
                try:
                    if e.is_symlink():
                        continue
                    if e.is_dir(follow_symlinks=False):
                        if os.stat(e.path).st_dev != base_dev:
                            continue
                        total += tree_size(e.path)
                    elif e.is_file(follow_symlinks=False):
                        total += e.stat(follow_symlinks=False).st_size
                except OSError:
                    continue
        return total

    dirs: list[dict] = []
    files_total = 0
    try:
        it = os.scandir(path)
    except OSError as e:
        return {"dirs": dirs, "files": 0, "error": str(e)[:200]}
    with it:
        for e in it:
            try:
                if e.is_symlink():
                    continue
                if e.is_dir(follow_symlinks=False):
                    if os.stat(e.path).st_dev != base_dev:
                        continue
                    dirs.append({"name": e.name, "path": e.path,
                                 "size": tree_size(e.path)})
                elif e.is_file(follow_symlinks=False):
                    files_total += e.stat(follow_symlinks=False).st_size
            except OSError:
                continue
    dirs.sort(key=lambda x: -x["size"])
    return {"dirs": dirs, "files": files_total, "error": ""}


# 目录占用扫描缓存：path -> {"ts","dirs","files","scanning","elapsed","error"}
_disk_scan_cache: dict = {}
_disk_scan_lock = threading.Lock()

# 扫描设置：auto=缓存到期后自动重扫；interval=自动重扫间隔（秒）。
# 持久化到 disk_scan_config.json，重启保留。
_disk_scan_cfg = {"auto": True, "interval": 600}
_disk_cfg_path = os.path.join(BASE_DIR, "disk_scan_config.json")


def _load_scan_cfg():
    try:
        with open(_disk_cfg_path, encoding="utf-8") as f:
            d = json.load(f)
        _disk_scan_cfg["auto"] = bool(d.get("auto", True))
        _disk_scan_cfg["interval"] = max(60, min(int(d.get("interval", 600)), 86400))
    except Exception:
        pass


def _save_scan_cfg():
    try:
        with open(_disk_cfg_path, "w", encoding="utf-8") as f:
            json.dump(_disk_scan_cfg, f)
    except Exception:
        pass


_load_scan_cfg()


def _scan_dir_sizes_cached(path: str, force: bool = False) -> dict:
    """带缓存的目录占用扫描。

    auto 开启：缓存 interval 秒内直接返回，到期后下一次请求触发重扫；
    auto 关闭：缓存一直有效（扫描一次后只看缓存），点「重新扫描」才重扫；
    force=True：忽略缓存立即重扫（扫描进行中时仍返回 scanning 状态）。
    """
    now = time.time()
    with _disk_scan_lock:
        c = _disk_scan_cache.get(path)
        if c and c.get("scanning"):
            return {"path": path, "scanning": True, "dirs": [], "files": 0}
        if c and not force:
            if _disk_scan_cfg["auto"]:
                if now - c["ts"] < _disk_scan_cfg["interval"]:
                    return {**c, "cached": True}
            else:
                return {**c, "cached": True}
        _disk_scan_cache[path] = {"path": path, "ts": now, "scanning": True,
                                  "dirs": [], "files": 0, "error": ""}

    def _work():
        t0 = time.time()
        try:
            r = _scan_dir_sizes(path)
        except Exception as e:
            r = {"dirs": [], "files": 0, "error": str(e)[:200]}
        r.update({"path": path, "ts": time.time(), "scanning": False,
                  "elapsed": round(time.time() - t0, 1)})
        with _disk_scan_lock:
            _disk_scan_cache[path] = r

    threading.Thread(target=_work, daemon=True).start()
    return {"path": path, "scanning": True, "dirs": [], "files": 0}


def _net_if_detail(ifs):
    """给每网卡采样补上 IP / 启用状态 / 链路速率，供前端网络详情表展示。"""
    if not ifs:
        return []
    try:
        addrs = psutil.net_if_addrs()
    except Exception:
        addrs = {}
    try:
        st = psutil.net_if_stats()
    except Exception:
        st = {}
    out = []
    for it in ifs:
        addr = ""
        for a in addrs.get(it["name"], []):
            if a.family == socket.AF_INET:
                addr = a.address
                break
        s = st.get(it["name"])
        out.append({**it, "addr": addr,
                    "up": bool(s.isup) if s else None,
                    "speed": s.speed if s and s.speed and s.speed > 0 else 0})
    return out


def _cpu_model():
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if "model name" in line:
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    if platform.system() == "Windows":
        return os.environ.get("PROCESSOR_IDENTIFIER", "") or "unknown"
    return platform.processor() or "unknown"


def _loadavg():
    """Linux/mac 返回真实负载；Windows 由 psutil 模拟（基于 CPU 占用），失败返回空。"""
    try:
        return [round(x, 2) for x in psutil.getloadavg()]
    except Exception:
        return []


@router.get("/api/stats")
async def stats(request: Request):
    _admin_auth(request)
    with hist_lock:
        snap = list(hist)
    cur = snap[-1] if snap else None
    top_cpu, top_mem = _top_procs()
    nconn = -1
    try:
        nconn = len(psutil.net_connections(kind="inet"))
    except Exception:
        pass
    return {
        "sys": {
            "hostname": platform.node(),
            "platform": f"{platform.system()} {platform.release()}",
            "python": platform.python_version(),
            "boot": int(psutil.boot_time()),
            "uptime": int(time.time() - psutil.boot_time()),
            "cpu_count": psutil.cpu_count(logical=True),
            "cpu_model": _cpu_model(),
            "nconn": nconn,
        },
        "cur": cur,
        "hist": {
            "ts": [s["ts"] for s in snap],
            "cpu": [s["cpu"] for s in snap],
            "mem": [s["mem"]["pct"] for s in snap],
            "net_recv": [round(s["net_recv"]) for s in snap],
            "net_sent": [round(s["net_sent"]) for s in snap],
            "disk_read": [round(s["disk_read"]) for s in snap],
            "disk_write": [round(s["disk_write"]) for s in snap],
        },
        "disks": _disks(),
        "net_ifs": _net_if_detail(cur.get("net_ifs") if cur else []),
        "top_cpu": top_cpu,
        "top_mem": top_mem,
    }


@router.get("/api/disk/usage")
async def api_disk_usage(request: Request, path: str = "/", force: bool = False):
    """目录占用明细：depth-1 子目录大小排行（后台扫描 + 缓存）。

    缓存策略由 /api/disk/scan_config 控制（自动扫描开关 + 间隔）；
    force=1 忽略缓存立即重扫。返回 scanning=True 表示扫描进行中，前端轮询。
    """
    _admin_auth(request)
    path = os.path.normpath(path)
    if not os.path.isdir(path):
        raise HTTPException(404, "目录不存在")
    return _scan_dir_sizes_cached(path, force=force)


class ScanCfgBody(BaseModel):
    auto: bool | None = None
    interval: int | None = None  # 秒；写入时夹到 60~86400


@router.get("/api/disk/scan_config")
async def api_disk_scan_config_get(request: Request):
    _admin_auth(request)
    return {**_disk_scan_cfg}


@router.post("/api/disk/scan_config")
async def api_disk_scan_config_set(request: Request, body: ScanCfgBody):
    _admin_auth(request)
    if body.auto is not None:
        _disk_scan_cfg["auto"] = bool(body.auto)
    if body.interval is not None:
        _disk_scan_cfg["interval"] = max(60, min(int(body.interval), 86400))
    _save_scan_cfg()
    return {"ok": True, **_disk_scan_cfg}


@router.post("/api/kill")
async def api_kill(request: Request, body: KillBody):
    _admin_auth(request)
    if body.pid in (1, os.getpid()):
        _fail("拒绝终止该进程")
    try:
        pr = psutil.Process(body.pid)
    except psutil.NoSuchProcess:
        raise HTTPException(404, "进程不存在")
    try:
        pr.terminate()
        gone, _ = psutil.wait_procs([pr], timeout=5)
        if gone:
            return {"ok": True, "mode": "terminate"}
        pr.kill()
        return {"ok": True, "mode": "kill"}
    except psutil.AccessDenied:
        raise HTTPException(403, "无权限终止该进程")


threading.Thread(target=_sample_loop, daemon=True).start()

