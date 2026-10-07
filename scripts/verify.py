"""Compose 中名为 verify 的单次服务入口。

依次核对并以退出码结束：
  1) 构建检查：全部模块语法编译、API 可导入；
  2) 代码测试：pytest 全套；
  3) 恢复与重传核对：在独立数据目录上对每个中断点做
     “断电(子进程退出码1) → 重开收敛 → 重传不新建段/结果不变”；
  4) API/HTTP 冒烟：对运行中的 api 服务打健康入口、建演练、压缩、
     模拟断电、重开、幂等重传、拒绝裁决等真实 HTTP 请求。

API_BASE 环境变量指向 api 服务（Compose 网络内 http://api:8080）。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable
API_BASE = os.environ.get("API_BASE", "http://api:8080").rstrip("/")
HEALTH_PATH = os.environ.get("HEALTH_PATH", "/healthz")

PASS, FAIL = "✓", "✗"
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    mark = PASS if ok else FAIL
    print(f"  {mark} {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        failures.append(name)


def step(title: str) -> None:
    print(f"\n=== {title} ===")


# --- 1) 构建检查 ----------------------------------------------------------

def build_check() -> None:
    step("构建检查（语法编译 / 导入）")
    proc = subprocess.run(
        [PYTHON, "-m", "compileall", "-q", os.path.join(ROOT, "app")],
        capture_output=True, text=True)
    check("app 包全部模块可编译", proc.returncode == 0, proc.stderr[:300])
    proc = subprocess.run(
        [PYTHON, "-c", "from app import server, storage, worker; "
                       "print('import ok')"],
        capture_output=True, text=True, cwd=ROOT)
    check("server/storage/worker 可导入", proc.returncode == 0,
          proc.stderr[-300:])


# --- 2) 代码测试 ----------------------------------------------------------

def pytest_run() -> None:
    step("代码测试（pytest）")
    proc = subprocess.run(
        [PYTHON, "-m", "pytest", "tests", "-q"],
        cwd=ROOT, capture_output=True, text=True)
    tail = "\n".join(proc.stdout.strip().splitlines()[-3:])
    check("pytest 全部通过", proc.returncode == 0, tail)
    print(proc.stdout[-1200:])


# --- 3) 恢复与重传核对 ----------------------------------------------------

PAYLOAD = {
    "compaction_id": "verify-drill",
    "artifacts": [
        {"name": "全景", "fragments": ["卫星过境-", "多光谱扫描", "·晴空"]},
        {"name": "局部", "fragments": ["·晴空", "雷达回波"]},
    ],
}
CRASH_POINTS = ["during_segments", "after_segments", "after_catalog",
                "after_switch"]


def _worker(data, *args, payload=None):
    req = None
    if payload is not None:
        os.makedirs(data, exist_ok=True)
        req = os.path.join(data, "req.json")
        with open(req, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    cmd = [PYTHON, "-m", "app.worker", "--data", data, *args]
    if req:
        cmd += ["--input", req]
    return subprocess.run(cmd, capture_output=True, text=True)


def recovery_retransmission_check() -> None:
    step("恢复与重传核对（真实断电子进程）")
    for point in CRASH_POINTS:
        data = tempfile.mkdtemp(prefix=f"verify-{point}-")
        crashed = _worker(data, "compact", "--crash", point, payload=PAYLOAD)
        check(f"[{point}] 断电工作进程以 1 退出", crashed.returncode == 1,
              f"rc={crashed.returncode} {crashed.stderr[-200:]}")

        rec = _worker(data, "recover")
        verdict = json.loads(rec.stdout)["recovery"] if rec.returncode == 0 else {}
        if point in ("after_catalog", "after_switch"):
            converged = verdict.get("found_active") == 1 \
                and verdict.get("active_complete") is True
        else:
            # 开关切换前断电：正确收敛态是“无活动目录、无悬挂代次”，
            # 只有一个待重传复用的未引用段；完整目录在重传后形成。
            converged = (
                rec.returncode == 0
                and verdict.get("found_active") is None
                and verdict.get("active") is None
                and verdict.get("rolled_back_pending") == []
                and verdict.get("orphan_generations") == []
            )
        check(f"[{point}] 重开后现场收敛（无悬挂目录）", converged,
              rec.stdout[-300:])

        again = _worker(data, "compact", payload=PAYLOAD)
        result = json.loads(again.stdout).get("result", {}) if again.returncode == 0 else {}
        check(f"[{point}] 重传成功且代为 1、结果一致",
              again.returncode == 0 and result.get("generation") == 1
              and result["artifact_summaries"]["全景"]["preview"]
              == "卫星过境-多光谱扫描·晴空"
              and result["artifact_summaries"]["局部"]["preview"]
              == "·晴空雷达回波",
              again.stdout[-300:])

        # 重传后的重开：必须是且仅是一份完整目录
        final = json.loads(_worker(data, "recover").stdout)["recovery"]
        check(f"[{point}] 重传重开后收敛为唯一完整目录",
              final.get("found_active") == 1
              and final.get("active_complete") is True
              and final.get("rolled_back_pending") == []
              and final.get("orphan_generations") == [],
              json.dumps(final, ensure_ascii=False)[:300])

        info = json.loads(_worker(data, "segments").stdout)
        check(f"[{point}] 重传不新建段（正式段恰 1 个）",
              len(info["segments"]) == 1, str(info))

        third = _worker(data, "compact", payload=PAYLOAD)
        t = json.loads(third.stdout).get("result", {})
        check(f"[{point}] 再次重传仍幂等", third.returncode == 0
              and t.get("idempotent") is True
              and len(json.loads(_worker(data, "segments").stdout)
                      ["segments"]) == 1)

    # 拒绝裁决：复用标识但工件集合不同 => 退出码 2，首个拒因，目录保留
    data = tempfile.mkdtemp(prefix="verify-reject-")
    _worker(data, "compact", payload=PAYLOAD)
    bad = dict(PAYLOAD)
    bad["artifacts"] = [
        {"name": "全景", "fragments": ["卫星过境-", "多光谱扫描", "·晴空"]},
        {"name": "被换掉", "fragments": ["X"]},
    ]
    p = _worker(data, "compact", payload=bad)
    rejected = json.loads(p.stdout) if p.returncode == 2 else {}
    check("标识复用但集合不同 => 拒绝(2) 且给出首个拒因",
          p.returncode == 2 and rejected.get("rejected") is True
          and "工件集合不同" in rejected.get("reason", ""), p.stdout[-300:])
    still = json.loads(_worker(data, "recover").stdout)["recovery"]
    check("拒绝后原活动目录保留且可重组",
          still["active"]["compaction_id"] == "verify-drill"
          and still["active_complete"] is True)


# --- 4) API / HTTP 冒烟 ---------------------------------------------------

def _post(path, payload):
    return requests.post(API_BASE + path, json=payload, timeout=30)


def http_smoke() -> None:
    step(f"API/HTTP 冒烟（{API_BASE}）")
    deadline = time.time() + 40
    healthy = False
    last = ""
    while time.time() < deadline:
        try:
            r = requests.get(API_BASE + HEALTH_PATH, timeout=3)
            if r.status_code == 200 and r.json().get("status") == "ok":
                healthy = True
                break
            last = f"{r.status_code} {r.text[:120]}"
        except requests.RequestException as exc:
            last = str(exc)
        time.sleep(1)
    check(f"健康入口 {HEALTH_PATH} 返回 ok", healthy, last)
    if not healthy:
        return

    r = requests.get(API_BASE + "/", timeout=10)
    check("首页 HTML 可访问", r.status_code == 200 and "地面成像站" in r.text)

    cid = "smoke-" + str(int(time.time()))
    payload = {"compaction_id": cid, "artifacts": [
        {"name": "全景", "fragments": ["A-", "B"]},
        {"name": "侧视", "fragments": ["B", "C"]},
    ]}
    r = _post("/api/compact", payload)
    d = r.json()
    check("压缩整理成功（2 份工件、去重片段、代次递增）",
          r.status_code == 200 and d.get("ok") and
          d["result"]["generation"] >= 1 and len(d["result"]["entries"]) == 3,
          r.text[:300])

    # 幂等重传：不新建段、不改结果
    seg_before = [s["name"] for s in
                  requests.get(API_BASE + "/api/status").json()["segments"]]
    r = _post("/api/compact", payload)
    d = r.json()
    seg_after = [s["name"] for s in
                 requests.get(API_BASE + "/api/status").json()["segments"]]
    check("同标识重传幂等：未新建段、结果不变",
          d.get("ok") and d["result"].get("idempotent") is True
          and seg_before == seg_after, r.text[:300])

    # 模拟断电（新段落盘后）
    crash_payload = {"compaction_id": cid + "-crash", "crash": "after_segments",
                     "artifacts": payload["artifacts"]}
    r = _post("/api/compact", crash_payload)
    d = r.json()
    check("after_segments 断电返回收敛状态且 API 存活",
          r.status_code == 200 and d.get("simulated_crash") is True
          and d.get("status_after_reopen") is not None, r.text[:300])

    # 重开收敛
    r = requests.post(API_BASE + "/api/recover", timeout=30)
    check("重开接口返回一份目录裁决", r.status_code == 200
          and "recovery" in r.json(), r.text[:300])

    # 重传崩溃作业 => 成功、复用段
    retry = {"compaction_id": cid + "-crash",
             "artifacts": payload["artifacts"]}
    r = _post("/api/compact", retry)
    d = r.json()
    check("崩溃作业重传成功且复用既有段",
          d.get("ok") and d["result"].get("segment_reused") is True,
          r.text[:300])

    # 拒绝裁决：用一个“刚刚完成”的全新标识来探测，避免受前面崩溃作业影响
    rej_cid = "reject-" + str(int(time.time()))
    rej_payload = {"compaction_id": rej_cid, "artifacts": payload["artifacts"]}
    r = _post("/api/compact", rej_payload)
    check("拒绝裁决基线作业整理成功", r.status_code == 200 and r.json().get("ok"),
          r.text[:300])

    bad = {"compaction_id": rej_cid, "artifacts": [
        {"name": "全景", "fragments": ["A-", "B"]},
        {"name": "别的", "fragments": ["Z"]},
    ]}
    r = _post("/api/compact", bad)
    d = r.json()
    check("复用标识+不同集合 => 409 + 首个拒因 + 原目录保留",
          r.status_code == 409 and d.get("rejected") is True
          and "工件集合不同" in d.get("reason", "")
          and d["status_after_reopen"]["active_generation"] is not None,
          r.text[:300])

    # 片段摘要不符（同名工件、同顺序、第二片段被改）
    bad2 = dict(rej_payload)
    bad2["artifacts"] = [
        {"name": "全景", "fragments": ["A-", "改"]},
        {"name": "侧视", "fragments": ["B", "C"]},
    ]
    r = _post("/api/compact", bad2)
    check("片段摘要不符 => 409 拒因",
          r.status_code == 409 and "摘要不符" in r.json().get("reason", ""),
          r.text[:200])

    # 被拒后原目录仍可重组原内容
    s = requests.get(API_BASE + "/api/status", timeout=10).json()
    check("拒绝后活动目录保持原作业且完整",
          s["catalog"]["compaction_id"] == rej_cid
          and s["recovery"]["active_complete"] is True
          and s["catalog"]["artifact_summaries"]["全景"]["preview"] == "A-B",
          json.dumps(s["catalog"]["compaction_id"], ensure_ascii=False)[:200])

    check("状态接口给出活动代次/重组摘要/段与偏移/恢复裁决",
          s["active_generation"] is not None and s["catalog"] is not None
          and len(s["catalog"]["entries"]) >= 1,
          json.dumps(s, ensure_ascii=False)[:300])

    # 不可变整理归属证据：相同请求重传不改写、被替代段清单、历史追溯、未找到
    hist_cid = "hist-" + str(int(time.time()))
    p_hist = {"compaction_id": hist_cid, "artifacts": payload["artifacts"]}
    assert _post("/api/compact", p_hist).json().get("ok")
    rec_hist = requests.get(
        API_BASE + "/api/compactions/" + hist_cid, timeout=10).json()
    gen_hist = rec_hist.get("generation")
    first_seg = rec_hist["new_segments"][0]["name"]
    # 相同请求重传 + 显式恢复，均不得改写首次裁决记录
    _post("/api/compact", p_hist)
    requests.post(API_BASE + "/api/recover", timeout=30)
    again = requests.get(
        API_BASE + "/api/compactions/" + hist_cid, timeout=10).json()
    check("相同请求重传/显式恢复后历史标识仍返回首次裁决且可复核",
          again.get("verifiable") is True
          and again.get("generation") == gen_hist
          and again["new_segments"][0]["name"] == first_seg
          and [a["name"] for a in again["input_artifacts"]] == ["全景", "侧视"],
          json.dumps(again, ensure_ascii=False)[:300])

    # 再做一次内容不同的整理：新记录须列出被替代旧段的稳定清单
    next_cid = "hist2-" + str(int(time.time()))
    p_next = {"compaction_id": next_cid, "artifacts": [
        {"name": "全景", "fragments": ["历史-", "新段"]},
        {"name": "侧视", "fragments": ["对照"]},
    ]}
    assert _post("/api/compact", p_next).json().get("ok")
    rec_next = requests.get(
        API_BASE + "/api/compactions/" + next_cid, timeout=10).json()
    check("新记录登记被替代旧段稳定清单（段名/最后所属代次/内容摘要）",
          rec_next.get("verifiable") is True
          and [s["name"] for s in rec_next.get("replaced_segments", [])]
          == [first_seg]
          and all(s.get("content_digest")
                  for s in rec_next["replaced_segments"]),
          json.dumps(rec_next.get("replaced_segments"), ensure_ascii=False)[:300])
    # 旧段已移入 trash：历史标识仍返回首次裁决段集合（可据 trash 段体复核）
    old = requests.get(
        API_BASE + "/api/compactions/" + hist_cid, timeout=10).json()
    check("旧段移入 trash、后续整理后历史记录不被改写且仍可复核",
          old.get("verifiable") is True and old.get("generation") == gen_hist
          and old["new_segments"][0]["name"] == first_seg
          and first_seg in requests.get(
              API_BASE + "/api/status", timeout=10).json().get("trash", []),
          json.dumps(old, ensure_ascii=False)[:200])

    nf = requests.get(API_BASE + "/api/compactions/no-such-id", timeout=10)
    check("不存在的标识明确返回 404 未找到",
          nf.status_code == 404 and nf.json().get("found") is False,
          nf.text[:200])


def main() -> int:
    print("地面成像站 · verify 单次核对")
    build_check()
    pytest_run()
    recovery_retransmission_check()
    http_smoke()

    step("汇总")
    if failures:
        print(f"{FAIL} {len(failures)} 项核对失败：")
        for f in failures:
            print("   - " + f)
        print("RESULT: FAIL")
        return 1
    print(f"{PASS} 全部核对通过（恢复/重传、pytest、构建、API/HTTP 冒烟）")
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
