"""存储引擎单元测试：去重、偏移登记、重组、幂等重传、拒因、清扫。"""

import json
import os

import pytest

from app.storage import (
    Artifact,
    Fragment,
    Rejected,
    Store,
    digest_text,
    read_record_at,
)


def test_fragment_dedup_and_offsets(store, sample_artifacts):
    res = store.compact("job-1", sample_artifacts)
    assert res["generation"] == 1
    # 7 个引用位置、4 个去重后唯一片段
    assert len(res["entries"]) == 4
    cat = store.read_active_catalog()
    # 登记偏移单调递增且每条记录都能按偏移读回
    offsets = [e["offset"] for e in res["entries"]]
    assert offsets == sorted(offsets)
    for e in res["entries"]:
        text, dg = read_record_at(store._seg_path(e["segment"]), e["offset"])
        assert dg == e["digest"]
        assert dg == digest_text(text)
    # 每份工件都能按登记顺序重新拼出
    assert store.reassemble(cat, "全景") == "卫星过境-多光谱扫描·晴空"
    assert store.reassemble(cat, "局部") == "·晴空多光谱扫描雷达回波"
    assert store.reassemble(cat, "附注") == "卫星过境-雷达回波"
    # 每个 artifact 只在一个段内
    for spec in cat["artifacts"].values():
        assert spec["segments"] == [res["entries"][0]["segment"]]


def test_segment_content_addressed(store, sample_artifacts):
    r1 = store.compact("job-1", sample_artifacts)
    segs1 = {x["name"] for x in store.list_segments()}
    r2 = store.compact("job-2",
                       [Artifact("x", [Fragment("卫星过境-")]),
                        Artifact("y", [Fragment("多光谱扫描")])])
    # 新整理的段内容不同 => 新段；旧段在新目录验证后被清扫进 trash
    assert {x["name"] for x in store.list_segments()} != segs1
    assert store.reassemble(store.read_active_catalog(), "x") == "卫星过境-"
    # 旧段只在 trash 中
    trash = store.list_trash()
    assert segs1 <= set(trash)


def test_idempotent_retransmission_same_id(store, sample_artifacts):
    store.compact("job-1", sample_artifacts)
    segs_before = {x["name"]: x["size"] for x in store.list_segments()}
    gen_before = store.active_generation()
    # 完全相同请求重传：不新建段、不换代次、结果一致
    res = store.compact("job-1", sample_artifacts)
    assert res["idempotent"] is True
    assert store.active_generation() == gen_before
    assert {x["name"]: x["size"] for x in store.list_segments()} == segs_before
    cat = store.read_active_catalog()
    assert cat["compaction_id"] == "job-1"
    original = {a.name: "".join(f.text for f in a.fragments)
                for a in sample_artifacts}
    for name, expected in original.items():
        assert store.reassemble(cat, name) == expected


def test_reject_reused_id_artifact_set_differs(store, sample_artifacts):
    store.compact("job-1", sample_artifacts)
    other = [a for a in sample_artifacts if a.name != "附注"]  # 去掉一份
    with pytest.raises(Rejected) as ei:
        store.compact("job-1", other)
    assert "工件集合不同" in ei.value.reason
    assert "附注" in ei.value.reason  # 首个拒因指出缺失工件
    # 原活动目录保留
    assert store.active_generation() == 1
    assert store.reassemble(store.read_active_catalog(), "附注") == \
        "卫星过境-雷达回波"


def test_reject_reused_id_digest_mismatch(store, sample_artifacts):
    store.compact("job-1", sample_artifacts)
    changed = [Artifact("全景",
                        [Fragment("卫星过境-"), Fragment("多光谱扫描-X"),
                         Fragment("·晴空")]),
               sample_artifacts[1], sample_artifacts[2]]
    with pytest.raises(Rejected) as ei:
        store.compact("job-1", changed)
    assert "片段摘要不符" in ei.value.reason
    assert store.active_generation() == 1
    # 原文未受影响
    assert store.reassemble(store.read_active_catalog(), "全景") == \
        "卫星过境-多光谱扫描·晴空"


def test_reject_missing_segment_keeps_active(store, sample_artifacts, monkeypatch):
    store.compact("job-1", sample_artifacts)
    # 模拟段缺失：直接删掉活动段后，用同标识重传必须被拒（新段缺失）
    cat = store.read_active_catalog()
    seg = cat["entries"][0]["segment"]
    os.unlink(store._seg_path(seg))
    with pytest.raises(Rejected) as ei:
        store.compact("job-1", sample_artifacts)
    assert "新段缺失" in ei.value.reason
    assert store.active_generation() == 1


def test_validation_artifact_count(store):
    one = [Artifact("a", [Fragment("x")]),
           Artifact("b", [Fragment("y")])]
    store.compact("ok", one)
    with pytest.raises(Rejected):
        store.compact("bad", [Artifact("a", [Fragment("x")])])
    with pytest.raises(Rejected):
        store.compact("bad9",
                      [Artifact(f"a{i}", [Fragment("z")]) for i in range(9)])


def test_segment_layout_single_segment(store, sample_artifacts):
    res = store.compact("job-1", sample_artifacts)
    names = {e["segment"] for e in res["entries"]}
    assert len(names) == 1
    offsets = sorted(e["offset"] for e in res["entries"])
    # 第一条记录偏移为 0
    assert offsets[0] == 0
    # 验证记录定长：4 长度头 + utf8 字节数 + 32 摘要
    cat = store.read_active_catalog()
    by_dg = {e["digest"]: e for e in cat["entries"]}
    texts = {"卫星过境-", "多光谱扫描", "·晴空", "雷达回波"}
    for t in texts:
        e = by_dg[digest_text(t)]
        assert e["length"] == len(t.encode("utf-8"))
    for i in range(len(offsets) - 1):
        e = next(x for x in res["entries"] if x["offset"] == offsets[i])
        gap = offsets[i + 1] - offsets[i]
        assert gap == 4 + e["length"] + 32


def test_segment_record_format_roundtrip(store, sample_artifacts):
    store.compact("job-1", sample_artifacts)
    cat = store.read_active_catalog()
    for name in cat["artifacts"]:
        text = store.reassemble(cat, name)
        frag_digests = cat["artifacts"][name]["fragment_digests"]
        assert digest_text(text) == store.status_payload(cat)[
            "artifact_summaries"][name]["digest"]
        assert len(frag_digests) >= 2


def test_trash_only_after_verification(store, sample_artifacts):
    store.compact("job-1", sample_artifacts)
    assert store.list_trash() == []
    r2 = store.compact("job-2", [Artifact("m", [Fragment("1")]),
                                 Artifact("n", [Fragment("2")])])
    assert r2["generation"] == 2
    # 旧段已不在正式段区，只在清扫暂存区
    old = set()
    assert all("seg-" in x for x in store.list_trash())
