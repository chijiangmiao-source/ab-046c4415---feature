"""地面成像站 · HTTP API 服务。

宿主端口与健康入口由 Compose 注入环境变量配置：
  PORT           监听端口（默认 8080）
  HOST           监听地址（默认 0.0.0.0）
  HEALTH_PATH    健康入口路径（默认 /healthz）
  IMAGING_DATA_DIR 数据根目录（默认 /data）

整理请求一律派发给独立工作进程（:mod:`app.worker`）执行：这样模拟断电时
只有工作进程被 ``os._exit(1)`` 杀死，API 进程依然存活；下一次
``GET /api/status``（或 ``POST /api/recover``）重开收敛，再重传同一请求
必须得到相同结果且不新建段。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from typing import Any, Dict, Tuple

from flask import Flask, jsonify, request, send_from_directory

from .storage import Artifact, Fragment, Rejected, Store

DATA_DIR = os.environ.get("IMAGING_DATA_DIR", "/data")
HEALTH_PATH = os.environ.get("HEALTH_PATH", "/healthz")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="/static")


def get_store() -> Store:
    return Store(DATA_DIR)


def _validate_payload(payload: Any) -> Tuple[str, list]:
    """返回 (错误信息, artifacts)；错误信息非空表示 400。"""
    if not isinstance(payload, dict):
        return "请求体必须是 JSON 对象", []
    cid = payload.get("compaction_id")
    if not isinstance(cid, str) or not cid.strip():
        return "缺少稳定整理标识 compaction_id", []
    raw_artifacts = payload.get("artifacts")
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        return "artifacts 必须是非空列表", []
    if not 2 <= len(raw_artifacts) <= 8:
        return "每份演练须含 2 至 8 份工件", []
    artifacts = []
    names = set()
    for i, item in enumerate(raw_artifacts):
        if not isinstance(item, dict):
            return f"第 {i + 1} 份工件格式错误", []
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            return f"第 {i + 1} 份工件缺少名称", []
        if name in names:
            return f"工件名称重复：{name}", []
        names.add(name)
        frags = item.get("fragments")
        if not isinstance(frags, list) or not frags:
            return f"工件 {name} 至少需要一个按顺序排列的片段", []
        parsed_frags = []
        for j, text in enumerate(frags):
            if not isinstance(text, str) or not text:
                return f"工件 {name} 第 {j + 1} 个片段为空或非文本", []
            parsed_frags.append(Fragment(text=text))
        artifacts.append(Artifact(name=name, fragments=parsed_frags))
    return "", artifacts


def _status_snapshot() -> Dict[str, Any]:
    store = get_store()
    verdict = store.recover()
    gen_files = sorted(
        n for n in os.listdir(store.gen_dir) if n.endswith(".json"))
    rolled_back = []
    if os.path.isdir(store.pending_dir):
        rolled_back = sorted(
            n for n in os.listdir(store.pending_dir)
            if n.startswith("rolled-back-"))
    return {
        "active_generation": store.active_generation(),
        "active_link": os.readlink(store.active_link())
        if os.path.islink(store.active_link()) else None,
        "catalog": verdict["active"],
        "recovery": {k: v for k, v in verdict.items() if k != "active"},
        "generations_on_disk": gen_files,
        "rolled_back_generations": rolled_back,
        "compaction_records": store.list_records(),
        "segments": store.list_segments(),
        "trash": store.list_trash(),
    }


@app.get(HEALTH_PATH)
def healthz() -> Any:
    store = get_store()
    return jsonify({
        "status": "ok",
        "data_dir": store.root,
        "active_generation": store.active_generation(),
    })


@app.get("/")
def index() -> Any:
    return send_from_directory(STATIC_DIR, "index.html")


@app.get("/api/status")
def api_status() -> Any:
    return jsonify(_status_snapshot())


@app.get("/api/compactions")
def api_compactions() -> Any:
    """已发布整理记录（不可变归属证据）清单。"""
    store = get_store()
    with store._xlatch():
        records = store.list_records()
    return jsonify({"records": records, "count": len(records)})


@app.get("/api/compactions/<compaction_id>")
def api_compaction_record(compaction_id: str) -> Any:
    """按稳定整理标识追溯首次裁决的归属证据。

    200 且 ``found=true``：记录存在，``verifiable`` 标明当前是否可据其
    登记的 (段,偏移) 重组全部工件，``problems`` 给出不可复核的具体原因；
    历史证据只取自持久化记录本身，绝不用当前活动目录顶替。
    """
    store = get_store()
    with store._xlatch():
        rec = store.get_record(compaction_id)
    if rec is None:
        return jsonify({
            "found": False,
            "verifiable": False,
            "compaction_id": compaction_id,
            "reason": "未找到该整理标识的已发布记录",
        }), 404
    return jsonify(rec)


@app.post("/api/recover")
def api_recover() -> Any:
    return jsonify(_status_snapshot())


@app.post("/api/compact")
def api_compact() -> Any:
    payload = request.get_json(silent=True)
    error, _artifacts = _validate_payload(payload)
    if error:
        return jsonify({"ok": False, "rejected": False, "reason": error}), 400

    crash = payload.get("crash") or None
    if crash not in (None, "during_segments", "after_segments",
                     "after_catalog", "after_switch"):
        return jsonify({"ok": False, "reason": f"未知中断点 {crash}"}), 400

    # 派发独立工作进程执行整理（崩溃只杀死工作进程）
    with tempfile.NamedTemporaryFile(
            "w", suffix=".json", prefix="req-", delete=False,
            encoding="utf-8") as tf:
        json.dump(payload, tf, ensure_ascii=False)
        req_path = tf.name
    try:
        cmd = [sys.executable, "-m", "app.worker", "--data", DATA_DIR,
               "compact", "--input", req_path]
        if crash:
            cmd += ["--crash", crash]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "reason": "工作进程超时"}), 504
    finally:
        try:
            os.unlink(req_path)
        except OSError:
            pass

    stdout = proc.stdout.strip()
    parsed = json.loads(stdout) if stdout else {}
    if proc.returncode == 1:
        # 模拟断电：工作进程在指定阶段被杀死。立即给出重开后的收敛状态。
        snap = _status_snapshot()
        stage = crash or "未知"
        return jsonify({
            "ok": True,
            "simulated_crash": True,
            "stage": stage,
            "message": f"已在「{stage}」阶段模拟断电，重开后目录已收敛",
            "status_after_reopen": snap,
        })
    if proc.returncode == 2:
        return jsonify({
            "ok": False,
            "rejected": True,
            "reason": parsed.get("reason", "请求被拒绝"),
            "details": parsed.get("details", {}),
            "status_after_reopen": _status_snapshot(),
        }), 409
    if proc.returncode != 0:
        return jsonify({
            "ok": False,
            "reason": parsed.get("error", proc.stderr.strip() or "工作进程失败"),
        }), 500
    return jsonify({"ok": True, "result": parsed["result"],
                    "status_after_reopen": _status_snapshot()})


@app.errorhandler(Rejected)
def _on_rejected(exc: Rejected) -> Any:  # pragma: no cover - 兜底
    return jsonify({"ok": False, "rejected": True, "reason": exc.reason}), 409


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    # 启动即做一次重开收敛
    get_store().recover()
    app.run(host=host, port=port, threaded=True)


if __name__ == "__main__":
    main()
