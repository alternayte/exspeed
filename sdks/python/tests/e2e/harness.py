"""Starts real exspeed servers for the end-to-end tests.

The binary comes from ``EXSPEED_BIN``, or else ``target/debug/exspeed`` at the
repository root (``cargo build -p exspeed --bin exspeed``). When neither
exists the e2e tests are skipped.
"""

from __future__ import annotations

import asyncio
import itertools
import os
import shutil
import signal
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

from exspeed import ExspeedClient

T = TypeVar("T")

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_BIN = REPO_ROOT / "target" / "debug" / "exspeed"


def find_binary() -> tuple[Path | None, str | None]:
    """The server binary, or ``None`` and the reason the e2e tests are skipped."""
    env = os.environ.get("EXSPEED_BIN")
    if env:
        p = Path(env).resolve()
        if not p.exists():
            raise RuntimeError(f"EXSPEED_BIN={env} does not exist")
        return p, None
    if DEFAULT_BIN.exists():
        return DEFAULT_BIN, None
    return None, (
        "e2e tests skipped: set EXSPEED_BIN to an exspeed server binary, or build one with "
        f"`cargo build -p exspeed --bin exspeed` (looked for {DEFAULT_BIN})"
    )


SERVER_BIN, SKIP_REASON = find_binary()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@dataclass
class ServerOptions:
    auth_token: str | None = None
    tls_cert: str | None = None
    tls_key: str | None = None
    #: Require client certificates signed by this CA (mutual TLS).
    tls_client_ca: str | None = None


class ExspeedServer:
    """A server process on free ports with a temp data directory."""

    def __init__(self, port: int, api_port: int, workdir: Path, opts: ServerOptions) -> None:
        self.port = port
        self.api_port = api_port
        self.workdir = workdir
        self.data_dir = workdir / "data"
        self.log_path = workdir / "server.log"
        self.opts = opts
        self.proc: subprocess.Popen[bytes] | None = None

    @classmethod
    def start(cls, opts: ServerOptions | None = None) -> ExspeedServer:
        if SERVER_BIN is None:
            raise RuntimeError(SKIP_REASON)
        workdir = Path(tempfile.mkdtemp(prefix="exspeed-py-e2e-"))
        server = cls(free_port(), free_port(), workdir, opts or ServerOptions())
        try:
            server._spawn()
        except BaseException:
            server.stop()
            raise
        return server

    def _spawn(self) -> None:
        assert SERVER_BIN is not None
        args = [
            str(SERVER_BIN),
            "server",
            "--bind",
            f"127.0.0.1:{self.port}",
            "--api-bind",
            f"127.0.0.1:{self.api_port}",
            "--data-dir",
            str(self.data_dir),
        ]
        o = self.opts
        if o.auth_token:
            args += ["--auth-token", o.auth_token]
        if o.tls_cert and o.tls_key:
            args += ["--tls-cert", o.tls_cert, "--tls-key", o.tls_key]
        if o.tls_client_ca:
            args += ["--tls-client-ca", o.tls_client_ca]
        # Don't let the developer's environment change the server under test.
        env = {k: v for k, v in os.environ.items() if not k.startswith("EXSPEED_")}
        env.setdefault("RUST_LOG", "warn")
        log = open(self.log_path, "ab")  # noqa: SIM115
        try:
            self.proc = subprocess.Popen(args, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        finally:
            log.close()
        try:
            self._wait_ready()
        except Exception as e:
            self.kill()
            raise RuntimeError(f"{e}\n--- server log ---\n{self.log_tail()}") from None

    def _wait_ready(self, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            assert self.proc is not None
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited with code {self.proc.returncode}")
            if self.readyz() == 200:
                return
            time.sleep(0.05)
        raise RuntimeError("server did not become ready")

    def readyz(self) -> int:
        """GET /readyz (over HTTPS when the server runs with TLS); 0 when unreachable."""
        tls = bool(self.opts.tls_cert)
        url = f"{'https' if tls else 'http'}://127.0.0.1:{self.api_port}/readyz"
        ctx = ssl._create_unverified_context() if tls else None
        try:
            with urllib.request.urlopen(url, timeout=1, context=ctx) as resp:
                return int(resp.status)
        except urllib.error.HTTPError as e:
            return int(e.code)
        except (OSError, ValueError):
            return 0

    def log_tail(self, lines: int = 80) -> str:
        try:
            return "\n".join(self.log_path.read_text(errors="replace").splitlines()[-lines:])
        except OSError:
            return ""

    def connect(self, **kw: Any) -> Awaitable[ExspeedClient]:
        """Connect a client to this server (reconnect off unless asked)."""
        opts: dict[str, Any] = {"port": self.port, "reconnect": False, **kw}
        return ExspeedClient.connect(**opts)

    def restart(self) -> None:
        """Stop the process and start a new one on the same ports and data directory."""
        self.kill()
        self._spawn()

    def kill(self) -> None:
        p = self.proc
        if p is None or p.poll() is not None:
            return
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(5)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(5)

    def stop(self) -> None:
        self.kill()
        shutil.rmtree(self.workdir, ignore_errors=True)


_counter = itertools.count()


def uniq(prefix: str) -> str:
    """A unique, valid stream/consumer name."""
    return f"{prefix}-{os.getpid()}-{int(time.time() * 1000):x}-{next(_counter)}"


async def eventually(fn: Callable[[], Awaitable[T]], timeout: float = 10.0) -> T:
    """Poll ``fn`` until it returns a truthy value, and return that."""
    deadline = time.monotonic() + timeout
    last: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            v = await fn()
            if v:
                return v
        except Exception as e:
            last = e
        await asyncio.sleep(0.05)
    raise AssertionError(f"eventually: timed out{f' (last error: {last!r})' if last else ''}")


def openssl_available() -> bool:
    return shutil.which("openssl") is not None


def openssl(cwd: Path, *args: str) -> None:
    r = subprocess.run(["openssl", *args], cwd=cwd, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"openssl {args[0]} failed: {r.stderr.decode(errors='replace')}")
