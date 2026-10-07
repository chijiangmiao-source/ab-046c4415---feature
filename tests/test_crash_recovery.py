"""断电-重开测试：工作子进程被 os._exit(1) 杀死后，重开必须收敛。

每个用例都在全新数据目录上：
  1) 以 --crash=<阶段> 运行工作进程，断言退出码为 1；
  2) 运行 recover，检查恢复裁决；
  3) 重传同一请求，断言不新建段、结果不变，且全部工件可重组。
"""

import json
import os
import subprocess
import sys

import pytest

PYTHON = sys.executable

PAYLOAD = {
    "compaction_id": "drill-crash",
    "artifacts": [
        {"name": "全景", "fragments": ["卫星过境-", "多光谱扫描", "·晴空"]},
        {"name": "局部", "fragments": ["·晴空", "雷达回波"]},
    ],
}

CRASH_POINTS = ["during_segments", "after_segments", "after_catalog",
                "after_switch"]


def _worker(data_dir, *args, payload=None):
    os.makedirs(data_dir, exist_ok=True)
    req = None
    if payload is not None:
        req = os.path.join(data_dir, "req-input.json")
        with open(req, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    cmd = [PYTHON, "-m", "app.worker", "--data", data_dir, *args]
    if req:
        cmd += ["--input", req]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc


@pytest.mark.parametrize("point", CRASH_POINTS)
def test_crash_then_reopen_converges(tmp_path, point):
    data = str(tmp_path / "data")
    proc = _worker(data, "compact", "--crash", point, payload=PAYLOAD)
    assert proc.returncode == 1, (point, proc.stdout, proc.stderr)

    rec = _worker(data, "recover")
    assert rec.returncode == 0, rec.stderr
    verdict = json.loads(rec.stdout)["recovery"]

    # after_catalog / after_switch：新目录已完整落盘 => 前滚完成切换
    if point in ("after_catalog", "after_switch"):
        assert verdict["found_active"] == 1
        assert verdict["active_complete"] is True
        assert verdict["completed_switch"] == (point == "after_catalog")
    else:
        # during_segments / after_segments：目录尚未持久化，active 仍为空
        assert verdict["found_active"] is None
        assert verdict["active"] is None

    # 重传：必须成功，且最终只有一个正式段、一个代次
    again = _worker(data, "compact", payload=PAYLOAD)
    assert again.returncode == 0, (point, again.stdout, again.stderr)
    result = json.loads(again.stdout)["result"]
    assert result["generation"] == 1
    assert result["compaction_id"] == "drill-crash"

    segs = _worker(data, "segments")
    info = json.loads(segs.stdout)
    assert len(info["segments"]) == 1, (point, info)

    # 恢复裁决：一份完整目录
    verdict2 = json.loads(_worker(data, "recover").stdout)["recovery"]
    assert verdict2["active_complete"] is True
    assert verdict2["found_active"] == 1
    summaries = verdict2["active"]["artifact_summaries"]
    assert summaries["全景"]["preview"] == "卫星过境-多光谱扫描·晴空"
    assert summaries["局部"]["preview"] == "·晴空雷达回波"
    # 再重传一次仍幂等：段数量、代次不变
    third = _worker(data, "compact", payload=PAYLOAD)
    assert json.loads(third.stdout)["result"]["idempotent"] is True
    info = json.loads(_worker(data, "segments").stdout)
    assert len(info["segments"]) == 1


def test_retransmit_after_segment_crash_reuses_segment(tmp_path):
    """新段落盘后断电：重传复用同一内容寻址段，绝不新建第二段。"""
    data = str(tmp_path / "data")
    _worker(data, "compact", "--crash", "after_segments", payload=PAYLOAD)
    # 此时盘上有一个未引用段
    before = json.loads(_worker(data, "recover").stdout)["recovery"]
    assert len(before["orphan_segments"]) == 1
    orphan = before["orphan_segments"][0]
    # 重传成功
    out = json.loads(_worker(data, "compact", payload=PAYLOAD).stdout)
    assert out["result"]["segment_reused"] is True
    info = json.loads(_worker(data, "segments").stdout)
    assert [s["name"] for s in info["segments"]] == [orphan]
    assert info["trash"] == []


def test_switch_crash_then_sweep_completes(tmp_path):
    """切换后/清扫前断电：旧代无旧段时 trash 为空；有旧代时补完清扫。"""
    data = str(tmp_path / "data")
    first = {"compaction_id": "j1", "artifacts": [
        {"name": "a", "fragments": ["one", "two"]},
        {"name": "b", "fragments": ["two"]}]}
    assert _worker(data, "compact", payload=first).returncode == 0
    second = {"compaction_id": "j2", "artifacts": [
        {"name": "a", "fragments": ["ONE", "TWO"]},
        {"name": "b", "fragments": ["THREE"]}]}
    assert _worker(data, "compact", "--crash", "after_switch",
                   payload=second).returncode == 1
    verdict = json.loads(_worker(data, "recover").stdout)["recovery"]
    assert verdict["found_active"] == 2
    assert verdict["active_complete"] is True
    info = json.loads(_worker(data, "segments").stdout)
    # 正式段区只剩新代次段；旧段在 trash
    assert len(info["segments"]) == 1
    assert len(info["trash"]) == 1
    # 新目录可重组
    assert verdict["active"]["artifact_summaries"]["a"]["preview"] == "ONETWO"


def test_rejected_after_crash_keeps_last_complete_catalog(tmp_path):
    """先完成 job-1；job-2 在 after_catalog 断电（新目录悬挂但完整，会前滚）。

    另验证：after_segments 断电后用同标识 job-2 提交不同工件集合……
    应正常作为新请求处理（该标识此前从未完成），不受悬挂段影响。
    """
    data = str(tmp_path / "data")
    j1 = {"compaction_id": "job-1", "artifacts": [
        {"name": "a", "fragments": ["hello"]},
        {"name": "b", "fragments": ["world"]}]}
    json.loads(_worker(data, "compact", payload=j1).stdout)
    # job-2 段已写但目录未落盘时断电
    j2 = {"compaction_id": "job-2", "artifacts": [
        {"name": "c", "fragments": ["new"]},
        {"name": "d", "fragments": ["data"]}]}
    assert _worker(data, "compact", "--crash", "after_segments",
                   payload=j2).returncode == 1
    verdict = json.loads(_worker(data, "recover").stdout)["recovery"]
    # 活动目录仍是完整的 job-1（代次 1），悬挂段未被引用
    assert verdict["found_active"] == 1
    assert verdict["active"]["compaction_id"] == "job-1"
    assert len(verdict["orphan_segments"]) == 1
    # job-1 同标识重传不同集合 => 409 风格拒绝（退出码 2），原目录保留
    bad = dict(j1)
    bad["artifacts"] = [{"name": "a", "fragments": ["hello"]},
                        {"name": "ZZZ", "fragments": ["x"]}]
    p = _worker(data, "compact", payload=bad)
    assert p.returncode == 2
    rejected = json.loads(p.stdout)
    assert rejected["rejected"] is True
    assert "工件集合不同" in rejected["reason"]
    still = json.loads(_worker(data, "recover").stdout)["recovery"]
    assert still["active"]["compaction_id"] == "job-1"
