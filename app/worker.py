"""整理工作进程 CLI。

崩溃模拟通过子进程实现：``--crash`` 在指定阶段注入故障，存储引擎调用
``os._exit(1)`` 立即终止本进程（不执行 finally/atexit），等效断电。
重开 = 再次运行 recover/新请求，存储必须自行收敛。

退出码：
  0 成功；1 模拟断电；2 请求被拒（首个拒因写入 JSON）；3 其它错误。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import List

from .storage import (
    Artifact,
    Fragment,
    Rejected,
    Store,
)

CRASH_POINTS = (
    "during_segments",
    "after_segments",
    "after_catalog",
    "after_switch",
)


def _load_artifacts(payload: dict) -> List[Artifact]:
    raw = payload.get("artifacts")
    if not isinstance(raw, list):
        raise Rejected("缺少 artifacts 列表")
    artifacts: List[Artifact] = []
    for item in raw:
        frags = []
        for idx, text in enumerate(item.get("fragments", [])):
            if not isinstance(text, str) or not text:
                raise Rejected(
                    f"工件 {item.get('name')} 第 {idx + 1} 个片段为空")
            frags.append(Fragment(text=text))
        artifacts.append(Artifact(name=str(item.get("name", "")), fragments=frags))
    return artifacts


def cmd_compact(args: argparse.Namespace) -> int:
    with open(args.input, "r", encoding="utf-8") as f:
        payload = json.load(f)
    compaction_id = payload.get("compaction_id") or args.id
    artifacts = _load_artifacts(payload)
    store = Store(args.data)
    if args.crash:
        if args.crash not in CRASH_POINTS:
            print(json.dumps({"error": f"未知故障点 {args.crash}"}),
                  file=sys.stderr)
            return 3
        store.set_fault(args.crash)
    try:
        result = store.compact(compaction_id, artifacts)
    except Rejected as rej:
        json.dump({"rejected": True, "reason": rej.reason,
                   "details": rej.details},
                  sys.stdout, ensure_ascii=False)
        sys.stdout.write("\n")
        sys.stdout.flush()
        return 2
    json.dump({"ok": True, "result": result}, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0


def cmd_recover(args: argparse.Namespace) -> int:
    store = Store(args.data)
    verdict = store.recover()
    json.dump({"ok": True, "recovery": verdict}, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0


def cmd_segments(args: argparse.Namespace) -> int:
    store = Store(args.data)
    store.recover()
    json.dump({"segments": store.list_segments(),
               "trash": store.list_trash()},
              sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="地面成像站整理工作进程")
    parser.add_argument("--data", default=os.environ.get("IMAGING_DATA_DIR",
                                                         "/data"))
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_compact = sub.add_parser("compact", help="发起一次压缩整理")
    p_compact.add_argument("--id", default=None)
    p_compact.add_argument("--input", required=True,
                           help="JSON 请求文件路径")
    p_compact.add_argument("--crash", choices=CRASH_POINTS, default=None,
                           help="在指定阶段模拟断电")
    p_compact.set_defaults(func=cmd_compact)

    p_rec = sub.add_parser("recover", help="重开后收敛目录")
    p_rec.set_defaults(func=cmd_recover)

    p_seg = sub.add_parser("segments", help="查看段与清扫区")
    p_seg.set_defaults(func=cmd_segments)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except Rejected as rej:
        json.dump({"rejected": True, "reason": rej.reason},
                  sys.stdout, ensure_ascii=False)
        sys.stdout.write("\n")
        return 2
    except Exception as exc:  # noqa: BLE001 - CLI 边界
        json.dump({"error": f"{type(exc).__name__}: {exc}"},
                  sys.stderr, ensure_ascii=False)
        sys.stderr.write("\n")
        return 3


if __name__ == "__main__":
    sys.exit(main())
