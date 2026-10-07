"""不可变整理记录（归属证据）测试。

覆盖：
- 成功整理后记录固化：发布代次、输入工件摘要、新段内容摘要、被替代段清单；
- 中断（写段中途/新段落盘后）不产生记录；目录落盘后/切换后断电由重开补固化；
- 拒绝不产生记录；同标识重传、后续整理、旧段移入 trash 都不改写首次记录；
- 历史标识查询返回首次裁决的段集合与重组核验结果；
- 不存在标识明确未找到；记录所列段被物理清除/摘要损坏时报“不可验证”，
  而不是回退显示当前活动目录的段。
"""

import json
import os
import subprocess
import sys

import pytest

from app.storage import Artifact, Fragment, Store, digest_text

PYTHON = sys.executable


def _arts1():
    return [
        Artifact("全景", [Fragment("卫星过境-"), Fragment("多光谱扫描"),
                         Fragment("·晴空")]),
        Artifact("局部", [Fragment("·晴空"), Fragment("雷达回波")]),
    ]


def _arts2():
    return [
        Artifact("全景", [Fragment("卫星过境-"), Fragment("多光谱扫描")]),
        Artifact("附注", [Fragment("雷达回波"), Fragment("·晴空")]),
    ]


def _arts3():
    return [
        Artifact("全景", [Fragment("全新内容A")]),
        Artifact("侧视", [Fragment("全新内容B")]),
    ]


# --- 记录内容 -------------------------------------------------------------

def test_record_finalized_after_successful_compaction(store):
    res = store.compact("job-1", _arts1())
    rec = store.get_record("job-1")
    assert rec is not None and rec["found"] is True
    assert rec["verifiable"] is True
    assert rec["generation"] == res["generation"] == 1
    assert rec["compaction_id"] == "job-1"
    assert rec["created_at"] == res["created_at"]
    # 输入工件摘要：两份工件、片段摘要序列、重组全文摘要、预览
    names = [a["name"] for a in rec["input_artifacts"]]
    assert names == ["全景", "局部"]
    by_name = {a["name"]: a for a in rec["input_artifacts"]}
    assert by_name["全景"]["reassembled_digest"] == \
        digest_text("卫星过境-多光谱扫描·晴空")
    assert by_name["局部"]["preview"] == "·晴空雷达回波"
    assert by_name["全景"]["fragment_digests"] == [
        digest_text("卫星过境-"), digest_text("多光谱扫描"),
        digest_text("·晴空")]
    # 该次接管的新段（恰好一个）及每段内容摘要、段内片段摘要
    assert len(rec["new_segments"]) == 1
    seg = rec["new_segments"][0]
    assert seg["name"] == res["entries"][0]["segment"]
    assert len(seg["content_digest"]) == 64
    frag_digests = sorted(f["digest"] for f in seg["fragments"])
    assert frag_digests == sorted(e["digest"] for e in res["entries"])
    # 首次发布无被替代段；重组核验通过
    assert rec["replaced_segments"] == []
    assert rec["reassembly_verification"]["ok"] is True
    assert {r["name"] for r in rec["reassembly_verification"]["artifact_results"]} \
        == {"全景", "局部"}


def test_second_compaction_record_lists_replaced_segments(store):
    r1 = store.compact("job-1", _arts1())
    r2 = store.compact("job-2", _arts2())
    assert r2["generation"] == 2
    old_seg = r1["entries"][0]["segment"]
    new_seg = r2["entries"][0]["segment"]
    assert old_seg != new_seg

    rec2 = store.get_record("job-2")
    assert rec2["generation"] == 2
    assert [s["name"] for s in rec2["new_segments"]] == [new_seg]
    # 被替代段稳定清单：旧段名、最后所属代次、内容指纹；固化时旧段已在
    # 新目录重组全部工件并切换成功之后（本进程内尚未清扫，仍在正式段区）
    assert [s["name"] for s in rec2["replaced_segments"]] == [old_seg]
    rep = rec2["replaced_segments"][0]
    assert rep["last_seen_in_generation"] == 1
    assert rep["location_at_publish"] == "segments"
    assert len(rep["content_digest"]) == 64
    # 清扫后旧段移入 trash：记录不改写，且其 content_digest 仍可对照 trash
    assert old_seg in store.list_trash()


def test_record_files_listed(store):
    store.compact("job-1", _arts1())
    store.compact("job-2", _arts2())
    lst = store.list_records()
    assert [r["compaction_id"] for r in lst] == ["job-1", "job-2"]
    assert all(r["verifiable"] for r in lst)
    assert lst[1]["replaced_segments"] == [
        store.get_record("job-1")["new_segments"][0]["name"]]


# --- 中断：何时不产生记录、何时补固化 --------------------------------------

def test_no_record_for_interrupted_or_rejected_jobs(store, tmp_path):
    """走子进程：写段中途/新段落盘后断电，重开后仍不得有记录。"""
    payload = {"compaction_id": "crash-job", "artifacts": [
        {"name": "全景", "fragments": ["卫星过境-", "多光谱扫描"]},
        {"name": "局部", "fragments": ["雷达回波"]}]}

    for point in ("during_segments", "after_segments"):
        d = os.path.join(str(tmp_path), f"data-{point}")
        os.makedirs(d, exist_ok=True)
        req = os.path.join(d, "req.json")
        with open(req, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        p = subprocess.run(
            [PYTHON, "-m", "app.worker", "--data", d, "compact",
             "--crash", point, "--input", req],
            capture_output=True, text=True)
        assert p.returncode == 1
        subprocess.run([PYTHON, "-m", "app.worker", "--data", d, "recover"],
                       capture_output=True, text=True)
        fresh = Store(d)
        # 仅写入在途段：不得生成记录
        assert fresh.get_record("crash-job") is None
        assert fresh.list_records() == []


def test_record_backfilled_after_catalog_and_switch_crash(tmp_path):
    data = str(tmp_path / "data")

    def run(*args, payload):
        os.makedirs(data, exist_ok=True)
        req = os.path.join(data, "req.json")
        with open(req, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        return subprocess.run(
            [PYTHON, "-m", "app.worker", "--data", data, *args, "--input", req],
            capture_output=True, text=True)

    payload = {"compaction_id": "drill-c", "artifacts": [
        {"name": "a", "fragments": ["one", "two"]},
        {"name": "b", "fragments": ["two"]}]}

    for point in ("after_catalog", "after_switch"):
        d = f"{data}-{point}"
        req = os.path.join(d, "req.json")
        os.makedirs(d, exist_ok=True)
        with open(req, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        crashed = subprocess.run(
            [PYTHON, "-m", "app.worker", "--data", d, "compact",
             "--crash", point, "--input", req],
            capture_output=True, text=True)
        assert crashed.returncode == 1
        # 崩溃现场尚未固化（after_catalog 时甚至还没切换）
        assert Store(d).get_record("drill-c") is None
        rec = subprocess.run(
            [PYTHON, "-m", "app.worker", "--data", d, "recover"],
            capture_output=True, text=True)
        assert rec.returncode == 0
        store = Store(d)
        got = store.get_record("drill-c")
        assert got is not None and got["verifiable"] is True
        assert got["generation"] == 1
        assert [a["name"] for a in got["input_artifacts"]] == ["a", "b"]


def test_rejected_job_creates_no_record(store):
    store.compact("job-1", _arts1())
    with pytest.raises(Exception):
        store.compact("job-1", [Artifact("全景", [Fragment("X")]),
                                Artifact("别的", [Fragment("Y")])])
    # 只有 job-1 一条记录
    assert [r["compaction_id"] for r in store.list_records()] == ["job-1"]


# --- 不可变性 -------------------------------------------------------------

def test_retransmission_and_later_compactions_never_rewrite_record(store):
    store.compact("job-1", _arts1())
    path = store._record_path("job-1")
    with open(path, "rb") as f:
        first_bytes = f.read()
    mtime = os.path.getmtime(path)

    # 同标识重传：不改写
    store.compact("job-1", _arts1())
    with open(path, "rb") as f:
        assert f.read() == first_bytes

    # 后续整理接管新段、把旧段移入 trash，记录仍原样
    store.compact("job-2", _arts2())
    store.compact("job-3", _arts3())
    with open(path, "rb") as f:
        assert f.read() == first_bytes
    assert os.path.getmtime(path) == mtime

    rec1 = store.get_record("job-1")
    # 历史标识仍返回首次裁决的段集合（不是当前 job-3 的段）
    first_seg = rec1["new_segments"][0]["name"]
    current_seg = store.read_active_catalog()["entries"][0]["segment"]
    assert first_seg != current_seg
    assert [a["name"] for a in rec1["input_artifacts"]] == ["全景", "局部"]
    assert rec1["generation"] == 1


def test_explicit_recover_does_not_rewrite_record(store):
    store.compact("job-1", _arts1())
    path = store._record_path("job-1")
    with open(path, "rb") as f:
        first_bytes = f.read()
    store.recover()
    store.recover()
    with open(path, "rb") as f:
        assert f.read() == first_bytes


def test_historical_id_reused_after_later_compaction_keeps_first_record(store):
    r1 = store.compact("job-1", _arts1())
    store.compact("job-2", _arts2())
    # 此时活动目录已是 job-2；复用历史标识 job-1（即便内容相同）在既有
    # 设计中作为新请求处理。无论是否产生新代次，已发布记录都不得被改写。
    first_path = store._record_path("job-1")
    with open(first_path, "rb") as f:
        first_bytes = f.read()
    store.compact("job-1", _arts1())
    with open(first_path, "rb") as f:
        assert f.read() == first_bytes
    rec = store.get_record("job-1")
    # 查询历史标识仍返回首次裁决的代次与段集合
    assert rec["generation"] == 1
    assert rec["new_segments"][0]["name"] == r1["entries"][0]["segment"]
    assert [a["name"] for a in rec["input_artifacts"]] == ["全景", "局部"]


# --- 未找到 / 不可验证 -----------------------------------------------------

def test_unknown_compaction_id_not_found(store):
    store.compact("job-1", _arts1())
    assert store.get_record("never-existed") is None
    assert store.get_record("") is None


def test_record_unverifiable_when_segment_gone(store):
    """记录所列段被物理清除（正式段区与 trash 均无）时报不可验证。"""
    r1 = store.compact("job-1", _arts1())
    store.compact("job-2", _arts2())
    seg1 = r1["entries"][0]["segment"]
    # job-1 的段已移入 trash；模拟其被物理清除（trash 仅留最近一代供观察）
    assert seg1 in store.list_trash()
    os.unlink(os.path.join(store.trash_dir, seg1))
    assert store._find_segment_file(seg1) is None

    rec1 = store.get_record("job-1")
    assert rec1["found"] is True
    assert rec1["verifiable"] is False
    problems = "\n".join(rec1["problems"])
    assert "段缺失" in problems
    # 接口不得把当前目录的段误显示为历史证据：记录里仍是首次裁决的段名
    assert rec1["new_segments"][0]["name"] == seg1
    current_seg = store.read_active_catalog()["entries"][0]["segment"]
    assert current_seg != seg1

    # 当前活动代次（job-2）自身记录仍可验证
    assert store.get_record("job-2")["verifiable"] is True


def test_record_unverifiable_when_segment_digest_corrupted(store):
    store.compact("job-1", _arts1())
    rec = store.get_record("job-1")
    seg_name = rec["new_segments"][0]["name"]
    path = store._seg_path(seg_name)
    with open(path, "r+b") as f:
        f.seek(4)  # 破坏首条片段正文
        f.write(b"\x00\x00")
    bad = store.get_record("job-1")
    assert bad["found"] is True
    assert bad["verifiable"] is False
    assert any("无法复核" in p or "不符" in p for p in bad["problems"])


def test_trash_segment_still_verifies_record(store):
    """新段已移入 trash（但未物理清除）时，记录仍可据 trash 段体复核。"""
    r1 = store.compact("job-1", _arts1())
    store.compact("job-2", _arts2())
    seg1 = r1["entries"][0]["segment"]
    assert seg1 in store.list_trash()
    rec1 = store.get_record("job-1")
    assert rec1["verifiable"] is True
