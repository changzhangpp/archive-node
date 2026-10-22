#!/usr/bin/env python3
"""archive-node 分片发送组件（独立部署，零第三方依赖，Python 3.8+）。

对接 archive-node 的分片归档协议：
  POST /archive/push_chunk   逐片上传（每片 < 128KB，默认 48KB）
  GET  /archive/push_status  查询已收片序号（断点续传）
  POST /archive/push_commit  按序号拼装 -> 整体 sha256 校验 -> 落盘记账

职责：计算文件 size/sha256 -> 规划分片 -> 补传缺失片 -> 提交校验。
既可作为库被 import，也可作为 CLI（python -m chunk_sender）。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import socket
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

__version__ = "1.0.0"

DEFAULT_CHUNK_SIZE = 48 * 1024
MAX_CHUNK_SIZE = 128 * 1024
MIN_CHUNK_SIZE = 1024
READ_BLOCK = 1024 * 1024


class SendError(RuntimeError):
    """发送失败（网络、鉴权、校验、配额等）。"""

    def __init__(self, message: str, status: Optional[int] = None, body: Optional[str] = None):
        super().__init__(message)
        self.status = status
        self.body = body


@dataclass
class SenderConfig:
    """发送端配置。base_url 形如 https://host:port（不带 /archive 后缀）。"""

    base_url: str
    key: str
    chunk_size: int = DEFAULT_CHUNK_SIZE
    timeout: float = 30.0
    retries: int = 4
    backoff: float = 0.8
    verify_tls: bool = True
    user_agent: str = f"archive-chunk-sender/{__version__}"

    def __post_init__(self) -> None:
        self.base_url = (self.base_url or "").strip().rstrip("/")
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError("base_url 必须以 http:// 或 https:// 开头")
        if not self.key:
            raise ValueError("缺少归档 key（ARCHIVE_KEY）")
        try:
            size = int(self.chunk_size)
        except (TypeError, ValueError):
            raise ValueError("chunk_size 必须是整数")
        if not (MIN_CHUNK_SIZE <= size <= MAX_CHUNK_SIZE):
            raise ValueError(f"chunk_size 须在 {MIN_CHUNK_SIZE}..{MAX_CHUNK_SIZE} 字节之间")
        self.chunk_size = size
        self.timeout = float(self.timeout)
        self.retries = max(0, int(self.retries))
        self.backoff = max(0.0, float(self.backoff))

    @classmethod
    def from_env(cls, env: Optional[dict] = None) -> "SenderConfig":
        env = os.environ if env is None else env
        return cls(
            base_url=env.get("ARCHIVE_URL", ""),
            key=env.get("ARCHIVE_KEY", ""),
            chunk_size=int(env.get("ARCHIVE_CHUNK_SIZE", DEFAULT_CHUNK_SIZE)),
            timeout=float(env.get("ARCHIVE_TIMEOUT", "30")),
            retries=int(env.get("ARCHIVE_RETRIES", "4")),
            backoff=float(env.get("ARCHIVE_BACKOFF", "0.8")),
            verify_tls=env.get("ARCHIVE_VERIFY_TLS", "1").lower() not in ("0", "false", "no"),
        )


def scan_file(path: Path):
    """流式计算 (size, sha256)。"""
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        while True:
            block = f.read(READ_BLOCK)
            if not block:
                break
            size += len(block)
            h.update(block)
    return size, h.hexdigest()


def plan_chunks(size: int, chunk_size: int) -> int:
    """分片数量；空文件按 1 个空片处理（服务端 commit 要求至少 1 片）。"""
    if size <= 0:
        return 1
    return (size + chunk_size - 1) // chunk_size


class ArchiveSender:
    """分片发送器：一个实例可复用，线程内串行使用。"""

    def __init__(self, config: SenderConfig):
        self.cfg = config
        self._ssl_ctx = None
        if config.base_url.startswith("https://") and not config.verify_tls:
            self._ssl_ctx = ssl._create_unverified_context()

    # ---------- HTTP ----------
    def _url(self, path: str, params: Optional[dict] = None) -> str:
        url = self.cfg.base_url + path
        if params:
            clean = {k: v for k, v in params.items() if v is not None}
            url += "?" + urllib.parse.urlencode(clean)
        return url

    def _open(self, req):
        if self._ssl_ctx is not None:
            return urllib.request.urlopen(req, timeout=self.cfg.timeout, context=self._ssl_ctx)
        return urllib.request.urlopen(req, timeout=self.cfg.timeout)

    def _sleep(self, attempt: int) -> None:
        time.sleep(min(self.cfg.backoff * (2 ** attempt), 10.0))

    def _request(self, method: str, path: str, params: Optional[dict] = None,
                 body: Optional[bytes] = None, headers: Optional[dict] = None) -> dict:
        url = self._url(path, params)
        hdrs = {
            "X-Archive-Key": self.cfg.key,
            "User-Agent": self.cfg.user_agent,
            "Accept": "application/json",
        }
        if body is not None:
            hdrs["Content-Type"] = "application/octet-stream"
        if headers:
            hdrs.update(headers)

        attempt = 0
        while True:
            req = urllib.request.Request(url, data=body, method=method, headers=hdrs)
            try:
                with self._open(req) as resp:
                    raw = resp.read()
                return json.loads(raw.decode("utf-8")) if raw else {}
            except urllib.error.HTTPError as exc:
                raw = exc.read().decode("utf-8", "replace")
                retryable = exc.code >= 500 or exc.code == 429
                if retryable and attempt < self.cfg.retries:
                    self._sleep(attempt)
                    attempt += 1
                    continue
                raise SendError(
                    f"{method} {path} -> HTTP {exc.code}: {raw[:300]}",
                    status=exc.code, body=raw,
                )
            except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError) as exc:
                if attempt < self.cfg.retries:
                    self._sleep(attempt)
                    attempt += 1
                    continue
                raise SendError(f"{method} {path} 网络错误: {exc}")
            except ValueError as exc:
                raise SendError(f"{method} {path} 响应不是合法 JSON: {exc}")

    # ---------- 协议 ----------
    def whoami(self) -> dict:
        """校验 key 并返回归档机登记的来源机名。"""
        return self._request("GET", "/archive/whoami")

    def push_status(self, upload_id: str, fname: str):
        res = self._request("GET", "/archive/push_status",
                            {"upload_id": upload_id, "fname": fname})
        return list(res.get("received") or [])

    def put_chunk(self, fname: str, upload_id: str, seq: int, data: bytes) -> dict:
        return self._request(
            "POST", "/archive/push_chunk",
            {"fname": fname, "upload_id": upload_id, "seq": seq}, body=data,
        )

    def commit(self, fname: str, upload_id: str, size: int, sha256: str,
               mtime: int = 0, task_id: str = "") -> dict:
        res = self._request(
            "POST", "/archive/push_commit",
            {"fname": fname, "upload_id": upload_id, "size": size,
             "sha256": sha256, "mtime": mtime, "task_id": task_id},
        )
        got = res.get("sha256")
        if got and got != sha256:
            raise SendError(f"commit 回执 sha256 不一致: {got} != {sha256}")
        return res

    # ---------- 发送 ----------
    def send(self, path, fname: Optional[str] = None, task_id: str = "",
             mtime: Optional[int] = None, chunk_size: Optional[int] = None,
             upload_id: Optional[str] = None, resume: bool = True,
             on_progress: Optional[Callable[[int, int, int], None]] = None,
             dry_run: bool = False) -> dict:
        """分片发送单个文件。

        resume=True 时先查已收片，只补缺失片；跨进程续传可显式传同一 upload_id。
        """
        path = Path(path)
        if not path.is_file():
            raise SendError(f"文件不存在: {path}")

        fname = fname or path.name
        if not fname or Path(fname).name != fname or fname.startswith("."):
            raise SendError(f"非法 fname（仅允许文件名，不以点开头）: {fname}")

        csize = int(chunk_size or self.cfg.chunk_size)
        if not (MIN_CHUNK_SIZE <= csize <= MAX_CHUNK_SIZE):
            raise ValueError(f"chunk_size 须在 {MIN_CHUNK_SIZE}..{MAX_CHUNK_SIZE} 字节之间")

        if mtime is None:
            mtime = int(path.stat().st_mtime)

        size, digest = scan_file(path)
        total = plan_chunks(size, csize)
        uid = upload_id or secrets.token_hex(16)

        result = {
            "ok": True, "fname": fname, "upload_id": uid,
            "size": size, "sha256": digest, "chunk_size": csize,
            "chunks_total": total, "chunks_uploaded": 0,
            "chunks_skipped": 0, "dry_run": dry_run,
        }
        if dry_run:
            return result

        received = set()
        if resume:
            received = {int(s) for s in self.push_status(uid, fname)}
        remaining = total - len([s for s in received if 0 <= s < total])
        uploaded = 0

        with open(path, "rb") as f:
            for seq in range(total):
                if seq in received:
                    result["chunks_skipped"] += 1
                    continue
                start = seq * csize
                want = 0 if size == 0 else min(csize, size - start)
                f.seek(start)
                data = f.read(want)
                if len(data) != want:
                    raise SendError(f"读取分片失败: seq={seq} got={len(data)} want={want}")
                self.put_chunk(fname, uid, seq, data)
                uploaded += 1
                result["chunks_uploaded"] = uploaded
                if on_progress:
                    on_progress(uploaded, remaining, len(data))

        res = self.commit(fname, uid, size, digest, mtime=int(mtime), task_id=task_id)
        result.update({
            "ok": bool(res.get("ok", True)),
            "dedup": res.get("dedup"),
            "usage_bytes": res.get("usage_bytes"),
        })
        return result

    def send_bytes(self, data: bytes, fname: str, **kwargs) -> dict:
        """发送内存中的字节内容（内部落临时文件）。"""
        with tempfile.TemporaryDirectory(prefix="chunk-sender-") as td:
            p = Path(td) / fname
            p.write_bytes(data)
            if kwargs.get("mtime") is None:
                kwargs["mtime"] = int(time.time())
            return self.send(p, fname=fname, **kwargs)

    def send_dir(self, root, pattern: str = "*", recursive: bool = True, **kwargs):
        """批量发送目录下文件（按 basename 归档；重名以前者为准，服务端按名覆盖）。"""
        root = Path(root)
        if not root.is_dir():
            raise SendError(f"目录不存在: {root}")
        it = root.rglob(pattern) if recursive else root.glob(pattern)
        results = []
        for p in sorted(x for x in it if x.is_file()):
            results.append(self.send(p, **kwargs))
        return results


# ---------------- CLI ----------------
def _make_progress():
    state = {"last": 0.0}

    def cb(done: int, total: int, nbytes: int) -> None:
        now = time.time()
        if done == total or now - state["last"] >= 0.5:
            state["last"] = now
            sys.stderr.write(f"\r  分片 {done}/{total} (+{nbytes}B)")
            sys.stderr.flush()

    return cb


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="archive-send",
        description="将文件分片并发送到 archive-node（支持断点续传）",
    )
    p.add_argument("--url", default=os.environ.get("ARCHIVE_URL"),
                   help="归档机地址，如 https://host:8100（或 ARCHIVE_URL）")
    p.add_argument("--key", default=os.environ.get("ARCHIVE_KEY"),
                   help="归档 key（或 ARCHIVE_KEY）")
    p.add_argument("--chunk-size", type=int,
                   default=int(os.environ.get("ARCHIVE_CHUNK_SIZE", DEFAULT_CHUNK_SIZE)),
                   help=f"分片字节数（{MIN_CHUNK_SIZE}..{MAX_CHUNK_SIZE}，默认 {DEFAULT_CHUNK_SIZE}）")
    p.add_argument("--timeout", type=float, default=float(os.environ.get("ARCHIVE_TIMEOUT", "30")))
    p.add_argument("--retries", type=int, default=int(os.environ.get("ARCHIVE_RETRIES", "4")))
    p.add_argument("--insecure", action="store_true", help="跳过 TLS 证书校验")
    p.add_argument("--json", action="store_true", help="仅输出 JSON 结果")

    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("whoami", help="校验 key 并显示来源机名")

    s = sub.add_parser("send", help="发送一个或多个文件")
    s.add_argument("files", nargs="+")
    s.add_argument("--fname", help="归档文件名（仅单文件时可用，默认取 basename）")
    s.add_argument("--task-id", default="")
    s.add_argument("--upload-id", help="复用指定 upload_id 以跨进程续传")
    s.add_argument("--no-resume", action="store_true", help="不查已收片，全部重传")
    s.add_argument("--dry-run", action="store_true", help="只计算分片计划，不传输")

    d = sub.add_parser("send-dir", help="发送目录内文件（按 basename 归档）")
    d.add_argument("dir")
    d.add_argument("--pattern", default="*")
    d.add_argument("--no-recursive", action="store_true")
    d.add_argument("--task-id", default="")
    d.add_argument("--dry-run", action="store_true")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if not args.url or not args.key:
        print("错误：需提供 --url/--key，或设置 ARCHIVE_URL/ARCHIVE_KEY", file=sys.stderr)
        return 2
    try:
        cfg = SenderConfig(
            base_url=args.url, key=args.key, chunk_size=args.chunk_size,
            timeout=args.timeout, retries=args.retries, verify_tls=not args.insecure,
        )
    except ValueError as exc:
        print(f"配置错误: {exc}", file=sys.stderr)
        return 2
    sender = ArchiveSender(cfg)

    try:
        if args.cmd == "whoami":
            res = sender.whoami()
            if args.json:
                print(json.dumps(res, ensure_ascii=False, indent=2))
            else:
                print(f"来源机: {res.get('source')}  (归档机版本 {res.get('version')})")
            return 0

        progress = None if args.json else _make_progress()
        results = []
        if args.cmd == "send":
            if args.fname and len(args.files) > 1:
                print("错误：--fname 仅适用于单文件", file=sys.stderr)
                return 2
            for path in args.files:
                if not args.json:
                    print(f"→ {path}", file=sys.stderr)
                res = sender.send(
                    path, fname=(args.fname if len(args.files) == 1 else None),
                    task_id=args.task_id, upload_id=args.upload_id,
                    resume=not args.no_resume, on_progress=progress,
                    dry_run=args.dry_run,
                )
                if progress:
                    sys.stderr.write("\n")
                results.append(res)
        else:
            results = sender.send_dir(
                args.dir, pattern=args.pattern, recursive=not args.no_recursive,
                task_id=args.task_id, on_progress=progress, dry_run=args.dry_run,
            )

        if args.json:
            print(json.dumps(results, ensure_ascii=False, indent=2))
        else:
            for res in results:
                if res.get("dry_run"):
                    print(f"  计划: {res['fname']} {res['size']}B -> "
                          f"{res['chunks_total']} 片 x {res['chunk_size']}B（未传输）")
                else:
                    print(f"  完成: {res['fname']} {res['size']}B sha256={res['sha256'][:16]} "
                          f"分片={res['chunks_total']}(上传 {res['chunks_uploaded']}, "
                          f"跳过 {res['chunks_skipped']}) dedup={res.get('dedup')}")
        return 0
    except (SendError, ValueError, OSError) as exc:
        print(f"失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
