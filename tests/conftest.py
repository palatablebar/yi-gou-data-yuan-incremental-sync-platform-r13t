"""测试公共夹具：把 backend(src) 与 worker(worker_src) 两个包挂到 sys.path，
并提供内存版 Redis / httpx 替身，测试不依赖真实 Postgres / Redis / HTTP 服务。
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
WORKER = ROOT / "worker"
# backend 以顶层包 `src` 导入；worker 在容器内叫 `worker_src`，
# 本地目录是 worker/src，这里用 importlib 把同一目录注册成 worker_src。
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import importlib  # noqa: E402

if "worker_src" not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        "worker_src", WORKER / "src" / "__init__.py", submodule_search_locations=[str(WORKER / "src")]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["worker_src"] = module
    spec.loader.exec_module(module)


class FakeRedis:
    """最小内存 Redis：string get/set/delete/expire + incr，全部带 TTL 但不主动过期。"""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.counters: dict[str, int] = {}

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex

    def delete(self, key: str) -> None:
        self.store.pop(key, None)
        self.ttls.pop(key, None)

    def expire(self, key: str, ttl: int) -> bool:
        if key not in self.store:
            return False
        self.ttls[key] = ttl
        return True

    def incr(self, key: str) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    def flushall(self) -> None:
        self.store.clear()
        self.ttls.clear()
        self.counters.clear()


@pytest.fixture
def fake_redis(monkeypatch) -> FakeRedis:
    from src.utils import mock_rest, redis_client

    fake = FakeRedis()
    # mock_rest 用 `from ... import get_redis` 直接持有引用，必须在使用点替换
    monkeypatch.setattr(mock_rest, "get_redis", lambda: fake)
    monkeypatch.setattr(redis_client, "get_redis", lambda: fake)
    return fake


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.headers: dict[str, str] = {}

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttp:
    """httpx 替身：按注册的 handler 响应请求，可注入临时 5xx 验证重试。"""

    def __init__(self) -> None:
        self.handlers: list = []
        self.calls: list[dict] = []
        self.fail_times = 0
        self.fail_status = 500

    def get(self, url: str, params: dict | None = None, timeout: float | None = None) -> FakeResponse:
        self.calls.append({"method": "GET", "url": url, "params": params or {}})
        if self.fail_times > 0:
            self.fail_times -= 1
            return FakeResponse(self.fail_status, {"code": "UPSTREAM_ERROR"}, text="injected")
        for handler in self.handlers:
            resp = handler("GET", url, params or {})
            if resp is not None:
                return resp
        return FakeResponse(404, text=f"no handler for {url}")

    def post(self, url: str, timeout: float | None = None) -> FakeResponse:
        self.calls.append({"method": "POST", "url": url})
        for handler in self.handlers:
            resp = handler("POST", url, {})
            if resp is not None:
                return resp
        return FakeResponse(404, text=f"no handler for {url}")

    def delete(self, url: str, timeout: float | None = None) -> FakeResponse:
        self.calls.append({"method": "DELETE", "url": url})
        for handler in self.handlers:
            resp = handler("DELETE", url, {})
            if resp is not None:
                return resp
        return FakeResponse(404, text=f"no handler for {url}")


@pytest.fixture
def fake_http(monkeypatch) -> FakeHttp:
    from worker_src.connectors import rest_connector

    http = FakeHttp()
    monkeypatch.setattr(rest_connector.httpx, "get", http.get)
    monkeypatch.setattr(rest_connector.httpx, "post", http.post)
    monkeypatch.setattr(rest_connector.httpx, "delete", http.delete)
    # 重试不睡觉，测试跑得快
    monkeypatch.setattr(rest_connector.time, "sleep", lambda _: None)
    return http


@pytest.fixture
def deterministic_mock(monkeypatch, fake_redis):
    """让 worker 侧直接复用 backend 的 mock_rest 分页逻辑挂到 FakeHttp 上。"""
    from src.utils import mock_rest

    def handler(method: str, url: str, params: dict) -> FakeResponse | None:
        base = "http://backend:8000/api/mock/rest/shop_orders"
        if method == "POST" and url == f"{base}/snapshot":
            return FakeResponse(200, mock_rest.create_snapshot())
        if method == "DELETE" and url.startswith(f"{base}/snapshot/"):
            snapshot_id = url.rsplit("/", 1)[-1]
            mock_rest.delete_snapshot(snapshot_id)
            return FakeResponse(200, {"deleted": True})
        if method == "GET" and url.startswith(f"{base}/snapshot/"):
            snapshot_id = url.rsplit("/", 1)[-1]
            snapshot = mock_rest.get_snapshot(snapshot_id)
            if snapshot is None:
                return FakeResponse(409, {"code": "SNAPSHOT_EXPIRED"})
            return FakeResponse(200, {"snapshot_id": snapshot_id, "total": snapshot["total"]})
        if method == "GET" and url == base:
            snapshot_id = params.get("snapshot_id")
            try:
                payload = mock_rest.paginate(
                    int(params.get("page", 1)),
                    int(params.get("page_size", 50)),
                    snapshot_id=snapshot_id,
                )
            except mock_rest.SnapshotExpired:
                return FakeResponse(409, {"code": "SNAPSHOT_EXPIRED"})
            payload["has_next"] = payload["page"] < payload["total_pages"]
            return FakeResponse(200, payload)
        return None

    http = FakeHttp()
    http.handlers.append(handler)

    from worker_src.connectors import rest_connector

    monkeypatch.setattr(rest_connector.httpx, "get", http.get)
    monkeypatch.setattr(rest_connector.httpx, "post", http.post)
    monkeypatch.setattr(rest_connector.httpx, "delete", http.delete)
    monkeypatch.setattr(rest_connector.time, "sleep", lambda _: None)
    return SimpleNamespace(http=http, mock_rest=mock_rest)
