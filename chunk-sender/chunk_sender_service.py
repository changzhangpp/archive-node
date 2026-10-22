#!/usr/bin/env python3
"""chunk-sender 可选 HTTP 微服务。

用途：让非 Python 系统（其他语言/容器）也能复用同一套分片发送逻辑——
本服务在来源机侧接收原始文件 body，内部调用 chunk_sender 完成分片与上传。

  GET  /health                      服务与上游状态
  GET  /whoami                      校验归档 key，返回来源机名
  GET  /list?source=                归档清单
  GET  /stats                       占用统计
  GET  /fetch?fname=&source=        回源下载（响应体为文件原始字节）
  POST /send?fname=..&task_id=..    请求体为文件原始字节，执行分片发送

鉴权：设置 SERVICE_TOKEN 后，除 /health 外的接口需带
      X-Service-Token: <token> 或 Authorization: Bearer <token>。
"""

import json
import os
import secrets
import tempfile
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from chunk_sender import ArchiveSender, SendError, SenderConfig, __version__

try:
    CONFIG = SenderConfig.from_env()
except ValueError as exc:  # 启动即暴露配置问题
    raise SystemExit(f"配置错误: {exc}")

SENDER = ArchiveSender(CONFIG)
SERVICE_TOKEN = os.environ.get("SERVICE_TOKEN", "")
MAX_BODY = int(float(os.environ.get("SERVICE_MAX_MB", "0")) * 1024 * 1024)
READ_BLOCK = 1024 * 1024


def _first(q, name, default=""):
    values = q.get(name)
    return values[0] if values else default


def _as_int(q, name, default):
    raw = _first(q, name, "")
    if raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"参数 {name} 必须是整数")


class Handler(BaseHTTPRequestHandler):
    server_version = f"chunk-sender-service/{__version__}"

    def log_message(self, fmt, *args):
        print("[chunk-sender] " + (fmt % args), flush=True)

    def _json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _auth_ok(self):
        if not SERVICE_TOKEN:
            return True
        got = self.headers.get("X-Service-Token") or ""
        if not got:
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                got = auth[7:]
        return secrets.compare_digest(got, SERVICE_TOKEN)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            return self._json(200, {
                "ok": True, "component": "chunk-sender", "version": __version__,
                "upstream": CONFIG.base_url, "chunk_size": CONFIG.chunk_size,
            })
        if path == "/whoami":
            if not self._auth_ok():
                return self._json(401, {"ok": False, "error": "unauthorized"})
            try:
                return self._json(200, SENDER.whoami())
            except SendError as exc:
                return self._json(exc.status or 502, {"ok": False, "error": str(exc)})

        if path == "/list":
            if not self._auth_ok():
                return self._json(401, {"ok": False, "error": "unauthorized"})
            q = parse_qs(urlparse(self.path).query)
            try:
                return self._json(200, SENDER.list_files(_first(q, "source") or None))
            except SendError as exc:
                return self._json(exc.status or 502, {"ok": False, "error": str(exc)})

        if path == "/stats":
            if not self._auth_ok():
                return self._json(401, {"ok": False, "error": "unauthorized"})
            try:
                return self._json(200, SENDER.stats())
            except SendError as exc:
                return self._json(exc.status or 502, {"ok": False, "error": str(exc)})

        if path == "/fetch":
            if not self._auth_ok():
                return self._json(401, {"ok": False, "error": "unauthorized"})
            q = parse_qs(urlparse(self.path).query)
            fname = _first(q, "fname")
            if not fname:
                return self._json(400, {"ok": False, "error": "缺少 fname"})
            source = _first(q, "source") or None
            try:
                resp, meta = SENDER.open_file(fname, source)
            except SendError as exc:
                return self._json(exc.status or 502, {"ok": False, "error": str(exc)})
            try:
                self.send_response(getattr(resp, "status", 200))
                self.send_header("Content-Type",
                                 resp.headers.get("Content-Type", "application/octet-stream"))
                length = resp.headers.get("Content-Length")
                if length:
                    self.send_header("Content-Length", length)
                self.send_header("Content-Disposition",
                                 f'attachment; filename="{meta["fname"]}"')
                self.end_headers()
                while True:
                    block = resp.read(READ_BLOCK)
                    if not block:
                        break
                    self.wfile.write(block)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                resp.close()
            return

        return self._json(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/send":
            return self._json(404, {"ok": False, "error": "not found"})
        if not self._auth_ok():
            return self._json(401, {"ok": False, "error": "unauthorized"})

        q = parse_qs(parsed.query)
        try:
            fname = _first(q, "fname")
            if not fname:
                return self._json(400, {"ok": False, "error": "缺少 fname"})
            length_raw = self.headers.get("Content-Length")
            if not length_raw:
                return self._json(411, {"ok": False, "error": "需要 Content-Length"})
            length = int(length_raw)
            if length < 0:
                return self._json(400, {"ok": False, "error": "Content-Length 非法"})
            if MAX_BODY and length > MAX_BODY:
                return self._json(413, {"ok": False, "error": "body 超出 SERVICE_MAX_MB"})
            task_id = _first(q, "task_id")
            chunk_size = _as_int(q, "chunk_size", None)
            upload_id = _first(q, "upload_id") or None
            mtime = _as_int(q, "mtime", None)
            resume = _first(q, "resume", "1").lower() not in ("0", "false", "no")
        except ValueError as exc:
            return self._json(400, {"ok": False, "error": str(exc)})

        suffix = Path(fname).suffix
        tmp = tempfile.NamedTemporaryFile(prefix="chunk-sender-", suffix=suffix, delete=False)
        try:
            remaining = length
            with tmp:
                while remaining > 0:
                    block = self.rfile.read(min(READ_BLOCK, remaining))
                    if not block:
                        break
                    tmp.write(block)
                    remaining -= len(block)
            if remaining > 0:
                return self._json(400, {"ok": False, "error": "请求体不完整"})
            res = SENDER.send(
                tmp.name, fname=fname, task_id=task_id,
                mtime=(int(time.time()) if mtime is None else mtime),
                chunk_size=chunk_size, upload_id=upload_id, resume=resume,
            )
            return self._json(200, res)
        except SendError as exc:
            return self._json(exc.status or 502, {"ok": False, "error": str(exc)})
        except (ValueError, OSError) as exc:
            return self._json(400, {"ok": False, "error": str(exc)})
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass


def main():
    host = os.environ.get("SERVICE_HOST", "0.0.0.0")
    port = int(os.environ.get("SERVICE_PORT", "8089"))
    if not SERVICE_TOKEN:
        print("[chunk-sender] 警告：未设置 SERVICE_TOKEN，服务无鉴权，请仅在内网使用", flush=True)
    print(f"[chunk-sender] 监听 {host}:{port} -> 上游 {CONFIG.base_url} "
          f"(chunk={CONFIG.chunk_size}B)", flush=True)
    httpd = ThreadingHTTPServer((host, port), Handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
