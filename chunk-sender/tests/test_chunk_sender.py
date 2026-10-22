#!/usr/bin/env python3
"""chunk_sender 单元测试：用本地 mock 归档机验证分片/续传/重试/空文件。"""

import hashlib
import json
import os
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chunk_sender import (  # noqa: E402
    ArchiveSender, SendError, SenderConfig, plan_chunks, scan_file,
)


class MockState:
    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        with getattr(self, "lock", threading.Lock()):
            self.chunks = {}
            self.commits = []
            self.put_seqs = []
            self.requests = 0
            self.fail_chunk_times = 0


STATE = MockState()


def _q(query):
    return {k: v[0] for k, v in parse_qs(query).items()}


class MockHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def do_GET(self):
        u = urlparse(self.path)
        q = _q(u.query)
        if u.path == "/archive/whoami":
            return self._json(200, {"ok": True, "source": "测试来源机", "version": "test"})
        if u.path == "/archive/push_status":
            uid = q.get("upload_id", "")
            with STATE.lock:
                got = sorted(seq for (u2, seq) in STATE.chunks if u2 == uid)
            return self._json(200, {"ok": True, "upload_id": uid, "received": got})
        return self._json(404, {"ok": False})

    def do_POST(self):
        u = urlparse(self.path)
        q = _q(u.query)
        data = self._body()
        if u.path == "/archive/push_chunk":
            uid = q.get("upload_id", "")
            seq = int(q.get("seq", "0"))
            with STATE.lock:
                STATE.requests += 1
                if STATE.fail_chunk_times > 0:
                    STATE.fail_chunk_times -= 1
                    return self._json(500, {"ok": False, "error": "injected"})
                STATE.chunks[(uid, seq)] = data
                STATE.put_seqs.append(seq)
            return self._json(200, {"ok": True, "seq": seq, "bytes": len(data)})

        if u.path == "/archive/push_commit":
            uid = q.get("upload_id", "")
            size = int(q.get("size", "0"))
            sha = q.get("sha256", "")
            with STATE.lock:
                parts = [STATE.chunks[(u2, seq)] for (u2, seq) in sorted(STATE.chunks) if u2 == uid]
            blob = b"".join(parts)
            if len(blob) != size or hashlib.sha256(blob).hexdigest() != sha:
                return self._json(400, {"ok": False, "error": "size/sha256 mismatch"})
            with STATE.lock:
                STATE.commits.append({"upload_id": uid, "size": size, "sha256": sha, "blob": blob})
            return self._json(200, {
                "ok": True, "dedup": False, "fname": q.get("fname", ""),
                "sha256": sha, "usage_bytes": size,
            })
        return self._json(404, {"ok": False})


class ChunkSenderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), MockHandler)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()

    def setUp(self):
        STATE.reset()
        self.sender = ArchiveSender(SenderConfig(
            base_url=f"http://127.0.0.1:{self.port}", key="test-key",
            chunk_size=4096, backoff=0.01, retries=4,
        ))

    def _write(self, data, name="a.bin"):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        p = Path(td.name) / name
        p.write_bytes(data)
        return p

    def test_helpers(self):
        p = self._write(b"hello")
        self.assertEqual(scan_file(p), (5, hashlib.sha256(b"hello").hexdigest()))
        self.assertEqual(plan_chunks(0, 4096), 1)
        self.assertEqual(plan_chunks(1, 4096), 1)
        self.assertEqual(plan_chunks(4096, 4096), 1)
        self.assertEqual(plan_chunks(4097, 4096), 2)

    def test_whoami(self):
        self.assertEqual(self.sender.whoami()["source"], "测试来源机")

    def test_send_roundtrip(self):
        data = os.urandom(15000)
        p = self._write(data)
        res = self.sender.send(p)
        self.assertEqual(res["size"], len(data))
        self.assertEqual(res["sha256"], hashlib.sha256(data).hexdigest())
        self.assertEqual(res["chunks_total"], 4)
        self.assertEqual(res["chunks_uploaded"], 4)
        self.assertEqual(res["chunks_skipped"], 0)
        self.assertEqual(len(STATE.commits), 1)
        self.assertEqual(STATE.commits[0]["blob"], data)

    def test_resume_skips_received(self):
        data = os.urandom(10000)
        p = self._write(data)
        uid = "a" * 32
        chunks = [data[i:i + 4096] for i in range(0, len(data), 4096)]
        self.sender.put_chunk("b.bin", uid, 0, chunks[0])
        self.sender.put_chunk("b.bin", uid, 2, chunks[2])
        STATE.put_seqs.clear()

        res = self.sender.send(p, fname="b.bin", upload_id=uid)
        self.assertEqual(res["chunks_total"], 3)
        self.assertEqual(res["chunks_skipped"], 2)
        self.assertEqual(res["chunks_uploaded"], 1)
        self.assertEqual(STATE.put_seqs, [1])
        self.assertEqual(STATE.commits[0]["blob"], data)

    def test_retry_on_5xx(self):
        STATE.fail_chunk_times = 2
        p = self._write(b"x" * 5000)
        res = self.sender.send(p)
        self.assertEqual(res["chunks_uploaded"], 2)
        self.assertGreaterEqual(STATE.requests, 4)
        self.assertEqual(len(STATE.commits), 1)

    def test_empty_file(self):
        p = self._write(b"")
        res = self.sender.send(p)
        self.assertEqual(res["size"], 0)
        self.assertEqual(res["chunks_total"], 1)
        self.assertEqual(res["chunks_uploaded"], 1)
        self.assertEqual(STATE.commits[0]["blob"], b"")

    def test_dry_run_no_network(self):
        p = self._write(b"y" * 9000)
        res = self.sender.send(p, dry_run=True)
        self.assertTrue(res["dry_run"])
        self.assertEqual(res["chunks_total"], 3)
        self.assertEqual(STATE.requests, 0)
        self.assertEqual(STATE.commits, [])

    def test_bad_fname_rejected(self):
        p = self._write(b"z")
        with self.assertRaises(SendError):
            self.sender.send(p, fname="../evil.bin")

    def test_chunk_size_bounds(self):
        with self.assertRaises(ValueError):
            SenderConfig(base_url="http://x", key="k", chunk_size=64)
        with self.assertRaises(ValueError):
            SenderConfig(base_url="http://x", key="k", chunk_size=256 * 1024)
        with self.assertRaises(ValueError):
            SenderConfig(base_url="ftp://x", key="k")


if __name__ == "__main__":
    unittest.main(verbosity=2)
