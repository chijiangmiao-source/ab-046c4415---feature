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


def test_compaction_records_list_and_trace(client):
    r = client.post("/api/compact", json=_payload())
    assert r.status_code == 200
    cid = _payload()["compaction_id"]

    # 状态快照内联已发布记录清单
    s = client.get("/api/status").get_json()
    ids = [x["compaction_id"] for x in s["compaction_records"]]
    assert ids == [cid]
    assert s["compaction_records"][0]["verifiable"] is True

    # 列表接口
    rl = client.get("/api/compactions")
    assert rl.status_code == 200
    assert rl.get_json()["count"] == 1

    # 追溯接口：发布代次、输入工件摘要、新段摘要、被替代段清单、核验结果
    r = client.get(f"/api/compactions/{cid}")
    assert r.status_code == 200
    d = r.get_json()
    assert d["found"] is True and d["verifiable"] is True
    assert d["generation"] == 1
    assert {a["name"] for a in d["input_artifacts"]} == {"全景", "侧视"}
    assert len(d["new_segments"]) == 1
    assert d["new_segments"][0]["fragments"]
    assert d["replaced_segments"] == []
    assert d["reassembly_verification"]["ok"] is True


def test_compaction_record_unknown_id_404(client):
    client.post("/api/compact", json=_payload())
    r = client.get("/api/compactions/no-such-job")
    assert r.status_code == 404
    d = r.get_json()
    assert d["found"] is False
    assert "未找到" in d["reason"]


def test_interrupted_job_has_no_record(client):
    # 通过 API 派发工作子进程，在新段落盘后断电：仅在途段，不得生成记录
    r = client.post("/api/compact",
                    json=_payload(compaction_id="crashed",
                                  crash="after_segments"))
    assert r.status_code == 200
    assert r.get_json()["simulated_crash"] is True
    client.post("/api/recover")
    r = client.get("/api/compactions/crashed")
    assert r.status_code == 404
    assert client.get("/api/compactions").get_json()["count"] == 0

    # 重传完成后记录才出现，且代次从 1 开始（中断未产生过半截记录）
    r = client.post("/api/compact",
                    json=_payload(compaction_id="crashed"))
    assert r.status_code == 200
    got = client.get("/api/compactions/crashed").get_json()
    assert got["verifiable"] is True and got["generation"] == 1


def test_compaction_record_replaced_segments_and_immutability(client):
    client.post("/api/compact", json=_payload())
    cid = _payload()["compaction_id"]
    first = client.get(f"/api/compactions/{cid}").get_json()
    first_seg = first["new_segments"][0]["name"]

    # 同标识重传不改写记录
    client.post("/api/compact", json=_payload())
    again = client.get(f"/api/compactions/{cid}").get_json()
    assert again["new_segments"][0]["name"] == first_seg
    assert again["generation"] == 1

    # 后续整理：新记录列出被替代旧段；首次记录仍可追溯且段集合不变
    payload2 = {"compaction_id": "web-drill-2", "artifacts": [
        {"name": "全景", "fragments": ["X-", "Y"]},
        {"name": "侧视", "fragments": ["Z"]},
    ]}
    client.post("/api/compact", json=payload2)
    r2 = client.get("/api/compactions/web-drill-2").get_json()
    assert [s["name"] for s in r2["replaced_segments"]] == [first_seg]
    assert r2["replaced_segments"][0]["last_seen_in_generation"] == 1
    hist = client.get(f"/api/compactions/{cid}").get_json()
    assert hist["verifiable"] is True
    assert hist["new_segments"][0]["name"] == first_seg
    assert hist["generation"] == 1
    # 列表按代次排序
    ids = [x["compaction_id"]
           for x in client.get("/api/compactions").get_json()["records"]]
    assert ids == [cid, "web-drill-2"]


def test_compaction_record_reports_unverifiable(client, monkeypatch):
    import os as _os
    client.post("/api/compact", json=_payload())
    cid = _payload()["compaction_id"]
    rec = client.get(f"/api/compactions/{cid}").get_json()
    seg = rec["new_segments"][0]["name"]
    # 物理删除记录所列段（正式段区与 trash 均无）
    _os.unlink(_os.path.join(server.DATA_DIR, "segments", seg))
    r = client.get(f"/api/compactions/{cid}")
    assert r.status_code == 200
    d = r.get_json()
    assert d["found"] is True and d["verifiable"] is False
    assert any("段缺失" in p for p in d["problems"])
    # 历史证据仍是首次裁决登记的段，而非当前目录其它内容
    assert d["new_segments"][0]["name"] == seg


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
