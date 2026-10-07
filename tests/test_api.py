"""HTTP API 测试（Flask test client）与真实 HTTP + 子进程崩溃端到端。"""

import json
import os
import subprocess
import sys
import time

import pytest

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import server


@pytest.fixture()
def client(monkeypatch, tmp_path):
    data = str(tmp_path / "data")
    monkeypatch.setattr(server, "DATA_DIR", data)
    server.get_store().recover()
    server.app.config["TESTING"] = True
    with server.app.test_client() as c:
        yield c


def _payload(**over):
    p = {
        "compaction_id": "web-drill",
        "artifacts": [
            {"name": "全景", "fragments": ["A-", "B"]},
            {"name": "侧视", "fragments": ["B", "C"]},
        ],
    }
    p.update(over)
    return p


def test_health_and_index(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.get_json()["status"] == "ok"
    r = client.get("/")
    assert r.status_code == 200
    assert "地面成像站" in r.get_data(as_text=True)


def test_compact_ok_then_status(client):
    r = client.post("/api/compact", json=_payload())
    assert r.status_code == 200, r.get_data(as_text=True)
    data = r.get_json()
    assert data["ok"] is True
    assert data["result"]["generation"] == 1
    assert data["status_after_reopen"]["active_generation"] == 1

    r2 = client.get("/api/status")
    assert r2.get_json()["catalog"]["artifact_summaries"]["全景"]["preview"] \
        == "A-B"


def test_retransmit_idempotent_over_http(client):
    client.post("/api/compact", json=_payload())
    r = client.post("/api/compact", json=_payload())
    assert r.status_code == 200
    d = r.get_json()
    assert d["result"]["idempotent"] is True
    assert d["result"]["segment_reused"] is True


def test_reused_id_different_sets_409(client):
    client.post("/api/compact", json=_payload())
    bad = _payload(artifacts=[
        {"name": "全景", "fragments": ["A-", "B"]},
        {"name": "别的", "fragments": ["Z"]},
    ])
    r = client.post("/api/compact", json=bad)
    assert r.status_code == 409
    d = r.get_json()
    assert d["rejected"] is True
    assert "工件集合不同" in d["reason"]
    assert d["status_after_reopen"]["active_generation"] == 1


def test_bad_artifact_count_400(client):
    r = client.post("/api/compact", json=_payload(artifacts=[
        {"name": "only", "fragments": ["x"]}]))
    assert r.status_code == 400
    assert "2 至 8" in r.get_json()["reason"]


def test_simulated_crash_via_subprocess_http(tmp_path):
    """通过真实 HTTP 服务（子进程）验证崩溃响应与重开状态。"""
    data = str(tmp_path / "data")
    port = 18080 + (os.getpid() % 5000)
    env = dict(os.environ, IMAGING_DATA_DIR=data, PORT=str(port),
               HEALTH_PATH="/healthz", PYTHONPATH=os.getcwd())
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.server"], env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        base = f"http://127.0.0.1:{port}"
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                if requests.get(base + "/healthz", timeout=1).ok:
                    break
            except requests.RequestException:
                time.sleep(0.2)
        else:
            raise AssertionError("server did not start: " +
                                 proc.stderr.read().decode()[:500])

        # after_segments 断电：HTTP 返回崩溃说明 + 重开收敛状态
        r = requests.post(base + "/api/compact", json=_payload(
            crash="after_segments"), timeout=30)
        assert r.status_code == 200
        d = r.json()
        assert d["simulated_crash"] is True
        assert d["stage"] == "after_segments"
        snap = d["status_after_reopen"]
        assert snap["active_generation"] is None
        assert snap["recovery"]["orphan_segments"]

        # 重传 => 成功、段复用、代次 1
        r2 = requests.post(base + "/api/compact", json=_payload(), timeout=30)
        d2 = r2.json()
        assert d2["ok"] is True
        assert d2["result"]["generation"] == 1
        assert d2["result"]["segment_reused"] is True

        # 再重传 => 完全幂等
        r3 = requests.post(base + "/api/compact", json=_payload(), timeout=30)
        assert r3.json()["result"]["idempotent"] is True

        # 状态与恢复裁决可查
        s = requests.get(base + "/api/status", timeout=10).json()
        assert s["active_generation"] == 1
        assert s["catalog"]["artifact_summaries"]["侧视"]["preview"] == "BC"
        assert s["recovery"]["active_complete"] is True
    finally:
        proc.terminate()
        proc.wait(timeout=10)
