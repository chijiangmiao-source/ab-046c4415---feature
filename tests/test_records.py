"""不可变整理记录（归属证据）测试。

记录只在“完整新目录可重组全部工件且 active 原子切换”之后随裁决固化；
追溯只认首次裁决封存的证据段与发布代次目录，绝不拿当前目录段顶替。
"""

import json
import os
import subprocess
import sys

import pytest

from app.storage import Artifact, Fragment, Rejected, Store, digest_text


A1 = [Artifact("全景", [Fragment("卫星过境-"), Fragment("多光谱扫描"),
                        Fragment("·晴空")]),
      Artifact("局部", [Fragment("·晴空"), Fragment("多光谱扫描"),
                        Fragment("雷达回波")])]
A2 = [Artifact("全景", [Fragment("卫星过境-"), Fragment("多光谱扫描-X")]),
      Artifact("局部", [Fragment("多光谱扫描-X"), Fragment("雷达回波")])]
A3 = [Artifact("全景", [Fragment("完全不同")]),
      Artifact("局部", [Fragment("毫无重叠")])]


def _ev_path(store: Store, cid: str, seg: str) -> str:
    return store._evidence_path(cid, seg)


def test_record_sealed_after_success_with_full_attribution(store):
    store.compact("job-1", A1)
    item = store.get_record("job-1")
    assert item is not None
    rec, ver = item["record"], item["verification"]
    assert ver["verifiable"] is True
    assert rec["published_generation"] == 1
    assert rec["previous_generation"] is None
    assert rec["reassembly_check"]["verified"] is True
    # 输入工件摘要
    names = {a["name"] for a in rec["input_artifacts"]}
    assert names == {"全景", "局部"}
    pan = next(a for a in rec["input_artifacts"] if a["name"] == "全景")
    assert pan["fragment_count"] == 3
    cat = store.read_active_catalog()
    assert pan["fragment_digests"] == \
        cat["artifacts"]["全景"]["fragment_digests"]
    # 每个新段的内容摘要：段体摘要 + 段内每条片段登记
    assert len(rec["new_segments"]) == 1
    seg = rec["new_segments"][0]
    assert seg["body_digest"] and seg["size"] > 0
    assert len(seg["records"]) == 4  # 去重后四个唯一片段
    # 首次整理没有被替代段
    assert rec["replaced_segments"] == []
    assert rec["replaced_safe_basis"]


def test_record_captures_replaced_segments_and_persists_after_sweep(store):
    store.compact("job-1", A1)
    r2 = store.compact("job-2", A2)
    assert r2["generation"] == 2
    rec1 = store.get_record("job-1")["record"]
    rec2 = store.get_record("job-2")["record"]
    old_seg = rec1["new_segments"][0]["segment"]
    new_seg = rec2["new_segments"][0]["segment"]
    assert old_seg != new_seg
    # job-2 记录稳定登记了被 job-1 段取代的旧段
    assert [s["segment"] for s in rec2["replaced_segments"]] == [old_seg]
    # 旧段已被清扫进 trash，但追溯 job-2 仍可凭证据副本复核
    assert old_seg in store.list_trash()
    ver = store.get_record("job-2")["verification"]
    assert ver["verifiable"] is True
    rep = ver["replaced_segments"][0]
    assert rep["evidence_present"] is True
    assert rep["body_digest_ok"] is True
    assert rep["current_location"] == "trash"
    # 追溯 job-1（其新段现在躺在 trash）仍可核验，且重组出首次内容
    v1 = store.get_record("job-1")["verification"]
    assert v1["verifiable"] is True
    assert v1["new_segments"][0]["current_location"] == "trash"
    assert v1["reassembly"]["previews"]["全景"] == \
        "卫星过境-多光谱扫描·晴空"


def test_history_returns_first_verdict_not_current_catalog(store):
    store.compact("job-1", A1)
    store.compact("job-2", A2)
    store.compact("job-3", A3)  # 活动目录推进到第三代，内容完全不同
    assert store.active_generation() == 3
    item = store.get_record("job-1")
    rec, ver = item["record"], item["verification"]
    assert rec["published_generation"] == 1
    assert ver["current_active_generation"] == 3
    # 复核的是首次裁决的段集合与重组结果，而非当前目录
    assert ver["verifiable"] is True
    assert ver["checked_against_generation"] == 1
    assert ver["reassembly"]["previews"]["全景"] == \
        "卫星过境-多光谱扫描·晴空"
    current = store.read_active_catalog()
    assert "卫星过境-" not in store.reassemble(current, "全景")


def _subprocess_worker(data: str, *args: str, payload=None):
    req = os.path.join(data, "req-worker.json")
    os.makedirs(data, exist_ok=True)
    cmd = [sys.executable, "-m", "app.worker", "--data", data, *args]
    if payload is not None:
        with open(req, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        cmd += ["--input", req]
    return subprocess.run(cmd, capture_output=True, text=True)


def test_no_record_for_crash_pending_or_rejected(tmp_path):
    data = str(tmp_path / "data")
    store = Store(data)
    p1 = {"compaction_id": "crash-job", "artifacts": [
        {"name": "a", "fragments": ["one", "two"]},
        {"name": "b", "fragments": ["two"]}]}

    # 写到一半断电：仅在途段残留，无记录
    assert _subprocess_worker(
        data, "compact", "--crash", "during_segments",
        payload=p1).returncode == 1
    store.recover()
    assert store.get_record("crash-job") is None

    # 新段落盘后断电（开关未切换）：仍无记录
    assert _subprocess_worker(
        data, "compact", "--crash", "after_segments",
        payload=p1).returncode == 1
    store.recover()
    assert store.get_record("crash-job") is None

    # 被拒绝的请求不产生记录；标识此前从未成功
    bad = dict(p1)
    bad["artifacts"] = [{"name": "a", "fragments": ["one", "two"]},
                        {"name": "z", "fragments": ["q"]}]
    # 先让同标识成功一次，再以不同集合重传被拒
    assert _subprocess_worker(data, "compact", payload=p1).returncode == 0
    assert _subprocess_worker(data, "compact", payload=bad).returncode == 2
    assert store.get_record("crash-job")  # 首次成功记录仍在
    assert len(store.list_records()) == 1


def test_record_immutable_after_retransmit_compact_recover(store):
    store.compact("job-1", A1)
    path = store._record_path("job-1")
    before = open(path, "rb").read()
    sealed_before = json.loads(before)["sealed_at"]

    res = store.compact("job-1", A1)    # 相同请求重传（活动作业幂等）
    assert res["idempotent"] is True
    store.compact("job-2", A2)          # 后续整理（旧段移入 trash）
    store.recover()                     # 显式重开

    after = open(path, "rb").read()
    assert after == before
    rec = json.loads(after)
    assert rec["sealed_at"] == sealed_before
    # 查询历史标识仍返回首次裁决的代次与段集合
    item = store.get_record("job-1")
    assert item["record"]["published_generation"] == 1
    assert item["verification"]["verifiable"] is True
    assert item["verification"]["reassembly"]["previews"]["全景"] == \
        "卫星过境-多光谱扫描·晴空"


def test_rejected_retransmission_keeps_first_record(store):
    store.compact("job-1", A1)
    with pytest.raises(Rejected):
        store.compact("job-1", A2)
    rec = store.get_record("job-1")["record"]
    # 记录仍是首次裁决的三段片段集合
    assert len(rec["new_segments"][0]["records"]) == 4
    ver = store.get_record("job-1")["verification"]
    assert ver["verifiable"] is True


def test_missing_evidence_makes_record_unverifiable(store):
    store.compact("job-1", A1)
    store.compact("job-2", A3)  # job-1 段进 trash
    seg = store.get_record("job-1")["record"]["new_segments"][0]["segment"]
    # 同时删掉证据副本与 trash 中的段：不得用当前正式段顶替
    os.unlink(_ev_path(store, "job-1", seg))
    os.unlink(os.path.join(store.trash_dir, seg))
    item = store.get_record("job-1")
    assert item["found"] is True
    ver = item["verification"]
    assert ver["verifiable"] is False
    assert any("证据副本缺失" in p for p in ver["problems"])
    assert ver["new_segments"][0]["current_location"] == "missing"


def test_tampered_evidence_makes_record_unverifiable(store):
    store.compact("job-1", A1)
    seg = store.get_record("job-1")["record"]["new_segments"][0]["segment"]
    ev = _ev_path(store, "job-1", seg)
    with open(ev, "r+b") as f:
        f.seek(5)
        f.write(b"\x00\x00")
    ver = store.get_record("job-1")["verification"]
    assert ver["verifiable"] is False
    assert any("摘要不符" in p or "读取失败" in p for p in ver["problems"])


def test_missing_published_generation_makes_record_unverifiable(store):
    store.compact("job-1", A1)
    os.unlink(store._gen_path(1))
    ver = store.get_record("job-1")["verification"]
    assert ver["verifiable"] is False
    assert ver["generation_present"] is False


def test_record_file_itself_tampered_is_reported(tmp_path):
    store = Store(str(tmp_path / "data"))
    store.compact("job-1", A1)
    path = store._record_path("job-1")
    with open(path, "wb") as f:
        f.write(b"{not json")
    item = store.get_record("job-1")
    assert item["found"] is True
    assert item["record"] is None
    assert item["verification"]["verifiable"] is False


def test_nonexistent_id_not_found(store):
    assert store.get_record("does-not-exist") is None


def test_record_index_sorted_and_flags_verifiability(store):
    store.compact("job-1", A1)
    store.compact("job-2", A2)
    idx = store.list_records()
    assert [x["record"]["compaction_id"] for x in idx] == ["job-1", "job-2"]
    assert all(x["verification"]["verifiable"] for x in idx)


def test_after_switch_crash_recover_seals_before_sweep(tmp_path):
    """切换后/清扫前断电：重开补固化记录，且先封存被替代段再清扫。"""
    import subprocess
    import sys
    data = str(tmp_path / "data")

    def worker(*args, payload):
        req = os.path.join(data, "req.json")
        os.makedirs(data, exist_ok=True)
        if payload is not None:
            with open(req, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
        cmd = [sys.executable, "-m", "app.worker", "--data", data, *args]
        if payload is not None:
            cmd += ["--input", req]
        return subprocess.run(cmd, capture_output=True, text=True)

    p1 = {"compaction_id": "j1", "artifacts": [
        {"name": "a", "fragments": ["one", "two"]},
        {"name": "b", "fragments": ["two"]}]}
    p2 = {"compaction_id": "j2", "artifacts": [
        {"name": "a", "fragments": ["ONE"]},
        {"name": "b", "fragments": ["TWO"]}]}
    assert worker("compact", payload=p1).returncode == 0
    assert worker("compact", "--crash", "after_switch",
                  payload=p2).returncode == 1
    assert worker("recover", payload=None).returncode == 0

    store = Store(data)
    item = store.get_record("j2")
    assert item is not None
    ver = item["verification"]
    assert ver["verifiable"] is True
    # 被替代段虽已在清扫后移入 trash，证据副本必须可复核
    assert len(ver["replaced_segments"]) == 1
    rep = ver["replaced_segments"][0]
    assert rep["evidence_present"] is True
    assert rep["body_digest_ok"] is True
