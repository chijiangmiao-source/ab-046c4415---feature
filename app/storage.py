"""崩溃安全的共享片段紧凑存储引擎。

磁盘布局（数据根目录）::

    root/
      segments/                 # 不可变段文件，名 e.g. seg-000007
      generations/              # 代次目录（JSON），名 gen-000007.json
      active -> gen-000006.json # 唯一活动目录（符号链接，rename 原子切换）
      pending/                  # 本次整理的临时区：清单、新段暂存
      trash/                    # 旧段清扫暂存区（非关键）
      records/rec-<标识>.json   # 不可变整理记录（归属证据，见下）

整理记录（attribution record）只在“完整新目录已能重组全部工件 *且*
active 已原子切换”之后，与当次裁决一并固化：记录发布代次、输入工件
摘要、每个新段内容摘要（含全文摘要/偏移/段体摘要）与被替代段的稳定
清单。中断、被拒、仅写在途段的作业一律不产生记录。记录一经写入永不
改写；后续整理、清扫入 trash、显式重开、同请求重传都不改动它。查询
历史标识时，依据记录本身与其发布代次目录重新核验段字节，绝不拿当前
活动目录的段顶替；段缺失或摘要不符 => 记录不可验证。

整理（compaction）协议，每个整理标识 ``compaction_id`` 建立子锁：

  阶段 A 校验：旧目录必须能完整重组所有工件，且每个片段摘要相符；
  阶段 B 新段落盘：内容定长、``fsync``、不可变，重传时内容寻址复用；
  阶段 C 持久化完整新索引（新代次 JSON，fsync 后 rename 入位）；
  阶段 D 原子切换唯一活动目录（符号链接 rename(2)）；
  阶段 E 切换完成后才允许校验通过并返回成功；旧段仅在新目录可重新
         拼出全部工件后才进入清扫。

任何阶段断电后重开，:func:`Store.recover` 都把存储收敛为一份完整目录：
新段已写但未切换 => 回滚（旧目录仍完整，新段留作重传复用，不被引用）；
新目录已持久化但链接没切过去 => 完成切换；切换发生在 rename 之间，崩溃
只会看到旧或新其中一份完整目录，绝不会出现指向错误偏移的登记项。
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import tempfile
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple

# --- 崩溃注入点 -----------------------------------------------------------
# 生产环境不设置；测试与 verify 通过 store.set_fault() 在指定阶段后“断电”。
FAULT_NONE = ""
FAULT_AFTER_SEGMENTS = "after_segments"      # 新段落盘(fsync)后
FAULT_AFTER_CATALOG = "after_catalog"        # 新目录已 fsync+rename 入位后
FAULT_AFTER_SWITCH = "after_switch"          # active 原子切换后（清扫前）
FAULT_DURING_SEGMENTS = "during_segments"    # 写到一半（段体不完整）

_SEGMENT_PREFIX = "seg-"
_GEN_PREFIX = "gen-"
_RECORD_PREFIX = "rec-"
_ACTIVE_NAME = "active"
_RECORD_VERSION = 1


class StoreError(Exception):
    """存储层错误。"""


class Rejected(Exception):
    """整理请求被拒绝，``reason`` 为首个拒因。"""

    def __init__(self, reason: str, details: Optional[dict] = None):
        super().__init__(reason)
        self.reason = reason
        self.details = details or {}


@dataclass
class Fragment:
    text: str
    digest: str = ""

    def __post_init__(self) -> None:
        if not self.digest:
            self.digest = digest_text(self.text)


@dataclass
class Artifact:
    name: str
    fragments: List[Fragment] = field(default_factory=list)


# --- 摘要工具 -------------------------------------------------------------

def digest_text(text: str) -> str:
    if isinstance(text, str):
        data = text.encode("utf-8")
    else:
        data = text
    return hashlib.sha256(data).hexdigest()


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write_bytes(target: str, data: bytes) -> None:
    """同目录临时文件 + fsync + rename，保证要么完整要么不存在。"""
    d = os.path.dirname(target)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=d)
    ok = False
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, target)
        _fsync_dir(d)
        ok = True
    finally:
        if not ok and os.path.exists(tmp):
            os.unlink(tmp)


def _read_fixed_record(f: Any, size: int, where: str) -> bytes:
    chunk = b""
    while len(chunk) < size:
        part = f.read(size - len(chunk))
        if not part:
            break
        chunk += part
    if len(chunk) != size:
        raise StoreError(f"段内记录不完整 @ {where}")
    return chunk


# --- 段文件格式 -----------------------------------------------------------
# 段体由定长记录组成，便于用偏移稳定引用：
#   [4 字节大端长度 N][N 字节 UTF-8 文本][32 字节 sha256 摘要]
# 每条记录的偏移即其首字节位置；段文件本身内容寻址复用。

def encode_fragment(text: str) -> Tuple[bytes, int]:
    body = text.encode("utf-8")
    rec = (
        len(body).to_bytes(4, "big")
        + body
        + bytes.fromhex(digest_text(body))
    )
    return rec, len(body)


def read_record_at(path: str, offset: int) -> Tuple[str, str]:
    with open(path, "rb") as f:
        f.seek(offset)
        length = int.from_bytes(_read_fixed_record(f, 4, f"{path}:{offset}"), "big")
        body = _read_fixed_record(f, length, f"{path}:{offset}:body")
        got_digest = _read_fixed_record(f, 32, f"{path}:{offset}:digest").hex()
    text = body.decode("utf-8")
    want = digest_text(body)
    if got_digest != want:
        raise StoreError(f"段内摘要不符 @ {path}:{offset}")
    return text, got_digest


# --- 代次目录结构 ---------------------------------------------------------
# {
#   "generation": 7,
#   "compaction_id": "job-42",
#   "created_at": "2026-10-07T..Z",
#   "artifacts": {
#       "alpha": {"fragment_digests": ["ab12..."], "segments": ["seg-000007"],
#                 "total_bytes": 42}
#   },
#   "entries": [
#       {"digest": "ab12...", "segment": "seg-000007", "offset": 0,
#        "length": 5, "next": null}
#   ]
# }


def catalog_fingerprint(cat: dict) -> str:
    """目录指纹：与工件集合差异/摘要不符/段缺失检测无关，仅用于观察。"""
    basis = json.dumps(
        {"a": cat.get("artifacts"), "e": cat.get("entries")},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return digest_text(basis)[:16]


class Store:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.seg_dir = os.path.join(self.root, "segments")
        self.gen_dir = os.path.join(self.root, "generations")
        self.pending_dir = os.path.join(self.root, "pending")
        self.trash_dir = os.path.join(self.root, "trash")
        self.record_dir = os.path.join(self.root, "records")
        for d in (self.root, self.seg_dir, self.gen_dir,
                  self.pending_dir, self.trash_dir, self.record_dir):
            os.makedirs(d, exist_ok=True)
        self._fault = FAULT_NONE
        self._fault_used = False
        self._fault_lock = threading.RLock()
        self._global = threading.RLock()
        self._job_locks: Dict[str, threading.RLock] = {}
        self._job_guard = threading.Lock()
        self._lock_path = os.path.join(self.root, ".store.lock")
        self._lock_fd: Optional[int] = None

    @contextmanager
    def _xlatch(self) -> Iterator[None]:
        """跨进程互斥锁：所有改变/收敛目录的操作都在同一把 flock 内。"""
        fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    # -- 故障注入 ----------------------------------------------------------
    def set_fault(self, fault: str) -> None:
        with self._fault_lock:
            self._fault = fault
            self._fault_used = False

    def _crash(self, fault_point: str) -> None:
        """模拟断电：立即以退出码 1 终止进程（不运行 finally/atexit）。"""
        with self._fault_lock:
            trigger = self._fault == fault_point and not self._fault_used
            if trigger:
                self._fault_used = True
        if trigger:
            os._exit(1)

    def _job_lock(self, compaction_id: str) -> threading.RLock:
        with self._job_guard:
            return self._job_locks.setdefault(compaction_id, threading.RLock())

    # -- 目录与 active 符号链接 -------------------------------------------
    def _gen_path(self, gen: int) -> str:
        return os.path.join(self.gen_dir, f"{_GEN_PREFIX}{gen:06d}.json")

    def _seg_path(self, name: str) -> str:
        return os.path.join(self.seg_dir, name)

    def _read_json(self, path: str) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def active_link(self) -> str:
        return os.path.join(self.root, _ACTIVE_NAME)

    def active_generation(self) -> Optional[int]:
        link = self.active_link()
        if not os.path.islink(link):
            return None
        target = os.readlink(link)
        return int(os.path.basename(target)[len(_GEN_PREFIX):].split(".")[0])

    def read_active_catalog(self) -> Optional[dict]:
        gen = self.active_generation()
        if gen is None:
            return None
        return self._read_json(self._gen_path(gen))

    def latest_generation_number(self) -> int:
        try:
            names = [n for n in os.listdir(self.gen_dir)
                     if n.startswith(_GEN_PREFIX) and n.endswith(".json")]
        except FileNotFoundError:
            return 0
        best = 0
        for n in names:
            try:
                best = max(best, int(n[len(_GEN_PREFIX):-len(".json")]))
            except ValueError:
                continue
        return best

    def _switch_active(self, gen: int) -> None:
        link = self.active_link()
        tmp_link = os.path.join(self.root, f".active.tmp.{uuid.uuid4().hex[:8]}")
        os.symlink(os.path.join("generations", f"{_GEN_PREFIX}{gen:06d}.json"),
                   tmp_link)
        _fsync_dir(self.root)
        os.replace(tmp_link, link)      # 原子切换，旧目录瞬间被替换
        _fsync_dir(self.root)

    # -- 每作业清单 --------------------------------------------------------
    def _manifest_path(self, compaction_id: str) -> str:
        return os.path.join(self.pending_dir, _safe(compaction_id),
                            "manifest.json")

    def _write_manifest(self, compaction_id: str, segment: str,
                        switched_generation: Optional[int]) -> None:
        path = self._manifest_path(compaction_id)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data = {
            "compaction_id": compaction_id,
            "segment": segment,
            "switched_generation": switched_generation,
        }
        _atomic_write_bytes(
            path, json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def _remove_manifest(self, compaction_id: str) -> None:
        path = self._manifest_path(compaction_id)
        if os.path.exists(path):
            os.unlink(path)
            _fsync_dir(self.pending_dir)

    def _read_manifests(self) -> List[dict]:
        out: List[dict] = []
        if not os.path.isdir(self.pending_dir):
            return out
        for name in os.listdir(self.pending_dir):
            p = os.path.join(self.pending_dir, name, "manifest.json")
            if os.path.isfile(p):
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        out.append(json.load(f))
                except (OSError, json.JSONDecodeError):
                    continue
        return out

    def _catalog_gen_for_job(self, compaction_id: str) -> Optional[int]:
        for gen in range(self.latest_generation_number(), 0, -1):
            p = self._gen_path(gen)
            if os.path.exists(p):
                try:
                    cat = self._read_json(p)
                except (OSError, json.JSONDecodeError):
                    continue
                if cat.get("compaction_id") == _safe(compaction_id):
                    return gen
        return None

    # -- 恢复 --------------------------------------------------------------
    def recover(self) -> dict:
        """断电重开后收敛为一份完整目录。返回恢复裁决。"""
        with self._xlatch():
            with self._global:
                return self._recover_locked()

    def _recover_locked(self) -> dict:
        verdict: Dict[str, Any] = {
            "found_active": None,
            "highest_on_disk": self.latest_generation_number(),
            "completed_switch": False,
            "rolled_back_pending": [],
            "orphan_segments": [],
            "orphan_generations": [],
            "active_complete": False,
            "active": None,
        }
        # 1) 清理悬空临时符号链接与半成品段（rename 前崩溃的残留物，
        #    它们从未被任何目录引用）
        for n in list(os.listdir(self.root)):
            p = os.path.join(self.root, n)
            if n.startswith(".active.tmp.") and os.path.islink(p):
                os.unlink(p)
        for dirpath, _dirs, files in os.walk(self.pending_dir):
            for n in files:
                if n.endswith(".part"):
                    os.unlink(os.path.join(dirpath, n))

        # 2) 处理已落盘但未切换的代次
        highest = verdict["highest_on_disk"]
        active_gen = self.active_generation()
        verdict["found_active"] = active_gen
        if highest > 0:
            hi_cat = self._read_json(self._gen_path(highest))
            hi_complete = self._catalog_reassemblable(hi_cat)
            if active_gen is None:
                # 初代：目录完整则切换，否则隔离该目录
                if hi_complete:
                    self._switch_active(highest)
                    verdict["completed_switch"] = True
                else:
                    self._quarantine(highest)
                    verdict["orphan_generations"].append(highest)
            elif highest > active_gen:
                # 新目录在盘上但 active 仍指旧代次
                if hi_complete:
                    # C 已完成、D 未完成（或切换未随目录 fsync 而留存）：
                    # 新目录完整，向前收敛完成切换
                    self._switch_active(highest)
                    verdict["completed_switch"] = True
                else:
                    # 新目录引用的段缺失/损坏：保持旧 active，悬挂目录留证
                    verdict["rolled_back_pending"].append(highest)
                    self._retire_generation(highest)

        # 3) active 自身必须完整；不完整时不抛异常——保留原活动目录，
        #    交由后续整理请求给出首个拒因（如“新段缺失”）。
        manifests = self._read_manifests()
        active_gen = self.active_generation()
        verdict["found_active"] = active_gen  # 收敛后的唯一活动代次
        if active_gen is not None:
            cat = self._read_json(self._gen_path(active_gen))
            ok, problems = self._catalog_errors(cat)
            verdict["active_complete"] = ok
            verdict["active_problems"] = problems
            if ok:
                verdict["active"] = self.status_payload(cat)
                # 前滚完成的代次若记录尚未固化（目录落盘后/切换后断电），
                # 必须在补完清扫*前*封存首次裁决证据与被替代段字节。
                prev_cat = self._predecessor_catalog(active_gen)
                self._ensure_record(cat, prev_cat)
                # 上次可能在切换后、清扫前断电：新目录已验证完整，补完清扫。
                # 只清扫“已切换（或已无在途目录）作业”的旧段；在途作业
                # （开关未切换）的新段必须保留，供重传内容寻址复用。
                self._recover_sweep(manifests)

        # 4) 清点未被活动目录引用的段（trash 观察区另列）
        referenced = self._referenced_segments()
        on_disk = {n for n in os.listdir(self.seg_dir)
                   if n.startswith(_SEGMENT_PREFIX)}
        verdict["orphan_segments"] = sorted(on_disk - referenced)
        verdict["trash"] = self.list_trash()
        return verdict

    def _in_flight_segments(self, manifests: List[dict]) -> set:
        """开关尚未切换、其新段必须保留以供重传复用的在途作业段。"""
        keep: set = set()
        for m in manifests:
            gen = self._catalog_gen_for_job(m["compaction_id"])
            active = self.active_generation()
            if gen is None or active is None or gen > active:
                keep.add(m["segment"])
        return keep

    def _recover_sweep(self, manifests: List[dict]) -> None:
        """按作业清单裁决旧段：已切换作业的旧段可清扫，在途段保留。"""
        keep = self._in_flight_segments(manifests)
        referenced = self._referenced_segments()
        self._sweep(extra_keep=keep | referenced)
        # 已随活动目录持久化且补完清扫的作业，其清单使命完成，清除
        active = self.active_generation()
        for m in manifests:
            gen = self._catalog_gen_for_job(m["compaction_id"])
            if gen is not None and active is not None and gen <= active:
                self._remove_manifest(m["compaction_id"])

    def _quarantine(self, gen: int) -> None:
        """初代目录不完整时把它移走（演示环境极少触发）。"""
        self._retire_generation(gen)

    def _retire_generation(self, gen: int) -> None:
        src = self._gen_path(gen)
        if os.path.exists(src):
            dst = os.path.join(self.pending_dir, f"rolled-back-{gen:06d}.json")
            os.replace(src, dst)
            _fsync_dir(self.gen_dir)
            _fsync_dir(self.pending_dir)

    def _referenced_segments(self) -> set:
        referenced: set = set()
        ag = self.active_generation()
        if ag is not None:
            cat = self._read_json(self._gen_path(ag))
            for e in cat.get("entries", []):
                referenced.add(e["segment"])
        return referenced

    # -- 目录完整性 --------------------------------------------------------
    def _catalog_errors(self, cat: dict) -> Tuple[bool, List[str]]:
        problems: List[str] = []
        entries = {e["digest"]: e for e in cat.get("entries", [])}
        for aname, art in cat.get("artifacts", {}).items():
            for idx, dg in enumerate(art.get("fragment_digests", [])):
                label = f"{aname}[{idx}]"
                e = entries.get(dg)
                if e is None:
                    problems.append(f"{label}: 目录缺少片段登记 {dg[:12]}")
                    continue
                seg_path = self._seg_path(e["segment"])
                if not os.path.exists(seg_path):
                    problems.append(f"{label}: 新段缺失 {e['segment']}")
                    continue
                try:
                    text, got = read_record_at(seg_path, e["offset"])
                except StoreError as exc:
                    problems.append(f"{label}: 段读取失败 {exc}")
                    continue
                if got != dg:
                    problems.append(
                        f"{label}: 片段摘要不符 登记={dg[:12]} 实读={got[:12]}")
        return (not problems), problems

    def _catalog_reassemblable(self, cat: dict) -> bool:
        ok, _ = self._catalog_errors(cat)
        return ok

    # -- 工件重组 ----------------------------------------------------------
    def reassemble(self, catalog: dict, artifact_name: str) -> str:
        art = catalog["artifacts"][artifact_name]
        parts: List[str] = []
        for dg in art["fragment_digests"]:
            e = next(x for x in catalog["entries"] if x["digest"] == dg)
            text, got = read_record_at(self._seg_path(e["segment"]), e["offset"])
            if got != dg:
                raise StoreError(f"重组 {artifact_name} 时摘要不符")
            parts.append(text)
        return "".join(parts)

    def status_payload(self, cat: dict) -> dict:
        artifacts = {}
        for aname, art in cat.get("artifacts", {}).items():
            text = self.reassemble(cat, aname)
            artifacts[aname] = {
                "name": aname,
                "fragment_count": len(art["fragment_digests"]),
                "fragment_digests": art["fragment_digests"],
                "segments": art["segments"],
                "total_bytes": art["total_bytes"],
                "digest": digest_text(text),
                "preview": text if len(text) <= 120 else text[:117] + "...",
            }
        return {
            "generation": cat["generation"],
            "compaction_id": cat.get("compaction_id"),
            "created_at": cat.get("created_at"),
            "fingerprint": catalog_fingerprint(cat),
            "artifact_summaries": artifacts,
            "entries": cat["entries"],
        }

    # -- 压缩整理 ----------------------------------------------------------
    def compact(
        self,
        compaction_id: str,
        artifacts: List[Artifact],
        *,
        expect_retransmission: Optional[bool] = None,
    ) -> dict:
        """以稳定整理标识发起压缩。

        幂等：同 ``compaction_id`` 重传完全相同的请求时不新建段、不改结果；
        标识复用但工件集合不同 / 摘要不符 / 新段缺失时保留原活动目录并
        返回首个拒因（:class:`Rejected`）。
        """
        if not compaction_id or not isinstance(compaction_id, str):
            raise Rejected("缺少整理标识 compaction_id")
        if not 2 <= len(artifacts) <= 8:
            raise Rejected("每份演练须含 2 至 8 份工件")
        names = [a.name for a in artifacts]
        if len(set(names)) != len(names):
            raise Rejected("工件名称重复")
        for a in artifacts:
            if not a.fragments:
                raise Rejected(f"工件 {a.name} 至少需要一个片段")

        with self._xlatch():
            with self._global:
                self._recover_locked()  # 进入前先把上次断电现场收敛
                with self._job_lock(compaction_id):
                    return self._compact_locked(compaction_id, artifacts)

    def _compact_locked(
        self, compaction_id: str, artifacts: List[Artifact]
    ) -> dict:
        active_cat = self.read_active_catalog()

        # 幂等快速通道：同标识 + 完全相同请求 => 返回已有结果，不新建段。
        if active_cat is not None and \
                active_cat.get("compaction_id") == _safe(compaction_id):
            same, reason = self._same_as_active(active_cat, artifacts)
            if same:
                # 目录已存在且完全一致：此前在途的作业清单已无用，清除它
                self._remove_manifest(compaction_id)
                payload = self.status_payload(active_cat)
                payload["idempotent"] = True
                payload["retransmission"] = True
                payload["segment_reused"] = True
                return payload
            raise Rejected(reason, {"compaction_id": compaction_id})

        # 标识首次使用：正常执行 A..E 协议。
        return self._run_new_compaction(compaction_id, artifacts, active_cat)

    def _same_as_active(
        self, active_cat: dict, artifacts: List[Artifact]
    ) -> Tuple[bool, str]:
        """重传内容必须与活动目录逐片段一致，否则给出首个拒因。"""
        active_names = set(active_cat["artifacts"].keys())
        new_names = {a.name for a in artifacts}
        if active_names != new_names:
            missing = sorted(active_names - new_names)
            extra = sorted(new_names - active_names)
            return False, (
                f"整理标识复用但工件集合不同：缺少工件 {missing}、"
                f"多出工件 {extra}".rstrip("、"))
        entries = {e["digest"]: e for e in active_cat["entries"]}
        for order, a in enumerate(artifacts):
            spec = active_cat["artifacts"][a.name]
            frags = fragments_of(a)
            if len(frags) != len(spec["fragment_digests"]):
                return False, (
                    f"工件 {a.name} 片段摘要不符：数量 "
                    f"{len(frags)} != {len(spec['fragment_digests'])}")
            for idx, frag in enumerate(frags):
                want_dg = spec["fragment_digests"][idx]
                if frag.digest != want_dg:
                    return False, (
                        f"工件 {a.name} 第 {idx + 1} 片段摘要不符："
                        f"提交 {frag.digest[:12]} 目录 {want_dg[:12]}")
                e = entries[want_dg]
                if not os.path.exists(self._seg_path(e["segment"])):
                    return False, f"新段缺失：{e['segment']}"
                try:
                    _, got = read_record_at(self._seg_path(e["segment"]),
                                            e["offset"])
                except StoreError as exc:
                    return False, f"新段缺失/损坏：{e['segment']} ({exc})"
                if got != want_dg:
                    return False, (
                        f"工件 {a.name} 第 {idx + 1} 片段摘要不符："
                        f"段内实读 {got[:12]}")
        return True, ""

    def _run_new_compaction(
        self, compaction_id: str, artifacts: List[Artifact],
        active_cat: Optional[dict],
    ) -> dict:
        # 阶段 A：旧目录必须能重新拼出全部既有工件（有旧目录时）。
        if active_cat is not None and not self._catalog_reassemblable(active_cat):
            raise Rejected("原活动目录不完整，拒绝发起新整理以保留现场")

        job_dir = os.path.join(self.pending_dir, _safe(compaction_id))
        os.makedirs(job_dir, exist_ok=True)

        # 期望布局（段内偏移在写盘前确定）
        entries: Dict[str, dict] = {}
        order: List[str] = []
        records: List[Tuple[str, bytes]] = []
        offset = 0
        for art in artifacts:
            for frag in fragments_of(art):
                if frag.digest not in entries:
                    rec, _ = encode_fragment(frag.text)
                    entries[frag.digest] = {
                        "digest": frag.digest, "segment": None,
                        "offset": offset,
                        "length": len(frag.text.encode("utf-8")),
                        "next": None,
                    }
                    order.append(frag.digest)
                    records.append((frag.digest, rec))
                    offset += len(rec)

        # 阶段 B：新段落盘。名按内容指纹，天然去重/幂等复用。
        body = b"".join(r for _, r in records)
        seg_digest = digest_text(body)
        seg_name = f"{_SEGMENT_PREFIX}{seg_digest[:16]}"
        seg_path = self._seg_path(seg_name)
        for dg in order:
            entries[dg]["segment"] = seg_name
        # 先持久化作业清单：声明该段属于一个“开关未切换”的在途作业，
        # 任何重开都不得把它当旧段清扫（重传要按内容寻址复用它）。
        self._write_manifest(compaction_id, seg_name, None)
        seg_existed = os.path.exists(seg_path)
        if not seg_existed:
            tmp_seg = os.path.join(job_dir, "segment.part")
            with open(tmp_seg, "wb") as f:
                # 故障：写到一半断电（段体不完整，且绝无目录引用它）
                if self._fault == FAULT_DURING_SEGMENTS:
                    f.write(body[: max(1, len(body) // 2)])
                    f.flush()
                    os.fsync(f.fileno())
                    self._crash(FAULT_DURING_SEGMENTS)
                f.write(body)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_seg, seg_path)
            _fsync_dir(self.seg_dir)
        self._crash(FAULT_AFTER_SEGMENTS)

        # 阶段 C：持久化完整新索引
        new_gen = self.latest_generation_number() + 1
        art_specs: Dict[str, dict] = {}
        for art in artifacts:
            digests = [f.digest for f in fragments_of(art)]
            art_specs[art.name] = {
                "fragment_digests": digests,
                "segments": sorted({entries[dg]["segment"] for dg in digests}),
                "total_bytes": sum(entries[dg]["length"] for dg in digests),
            }
        cat = {
            "generation": new_gen,
            "compaction_id": _safe(compaction_id),
            "created_at": _utcnow(),
            "artifacts": art_specs,
            "entries": [entries[dg] for dg in order],
        }
        _atomic_write_bytes(
            self._gen_path(new_gen),
            json.dumps(cat, ensure_ascii=False, indent=2).encode("utf-8"),
        )
        self._crash(FAULT_AFTER_CATALOG)

        # 阶段 D：原子切换唯一目录代次
        self._switch_active(new_gen)
        # 清单登记：开关已切换（崩溃在这之后也能凭目录判断作业已提交）
        self._write_manifest(compaction_id, seg_name, new_gen)
        self._crash(FAULT_AFTER_SWITCH)

        # 阶段 E：切换后验证新目录可重新拼出全部工件，然后清扫旧段。
        ok, problems = self._catalog_errors(cat)
        if not ok:
            # 极端情况：切换后才发现问题。不删任何旧段，报告并保留现场。
            raise StoreError(f"切换后校验失败: {problems}")
        # 与本次裁决一并固化不可变归属证据：必须先于清扫，把被替代段逐字节
        # 封存进证据区；此后整理/清扫/重开/重传都不得再改写这份记录。
        self._ensure_record(cat, active_cat)
        self._sweep()
        self._remove_manifest(compaction_id)

        payload = self.status_payload(cat)
        payload["idempotent"] = False
        payload["retransmission"] = False
        payload["segment_reused"] = seg_existed
        return payload

    def _sweep(self, extra_keep: Optional[set] = None) -> None:
        """旧段只能在新目录可重新拼出全部工件后清扫。

        ``extra_keep`` 中的段（在途作业、待重传复用）不动；其余未被活动
        目录引用的段先从 ``segments/`` 原子移入 ``trash/``（使其不再出现
        在正式存储中），trash 仅保留最近一代旧段供观察，其余物理清除。
        清扫中途断电只会留下垃圾文件，不影响目录与恢复正确性。
        """
        keep = set(extra_keep or ())
        referenced = self._referenced_segments() | keep
        moved = False
        for name in list(os.listdir(self.seg_dir)):
            if not name.startswith(_SEGMENT_PREFIX):
                continue
            if name in referenced:
                continue
            src = self._seg_path(name)
            dst = os.path.join(self.trash_dir, name)
            if os.path.exists(dst):
                os.unlink(src)
            else:
                os.replace(src, dst)
            moved = True
        if moved:
            _fsync_dir(self.seg_dir)
        trash = sorted(os.listdir(self.trash_dir))
        stale = trash[:-1] if len(trash) > 1 else []
        for name in stale:
            os.unlink(os.path.join(self.trash_dir, name))
        if stale:
            _fsync_dir(self.trash_dir)

    # -- 不可变整理记录（归属证据）----------------------------------------
    def _record_path(self, compaction_id: str) -> str:
        return os.path.join(self.record_dir,
                            f"{_RECORD_PREFIX}{_safe(compaction_id)}.json")

    def _locate_segment(self, name: str) -> Optional[str]:
        """在正式段区或清扫区定位段；都没有则 None（不看任何目录归属）。"""
        p = self._seg_path(name)
        if os.path.exists(p):
            return p
        t = os.path.join(self.trash_dir, name)
        if os.path.exists(t):
            return t
        return None

    @staticmethod
    def _catalog_segment_names(cat: Optional[dict]) -> set:
        if not cat:
            return set()
        return {e["segment"] for e in cat.get("entries", [])}

    def _segment_evidence(self, name: str) -> Optional[dict]:
        """段当前内容证据：整段 sha256 与字节数；缺失返回 None。"""
        path = self._locate_segment(name)
        if path is None:
            return None
        with open(path, "rb") as f:
            body = f.read()
        return {"segment": name, "body_digest": digest_text(body),
                "size": len(body)}

    def _build_record(
        self, cat: dict, previous_cat: Optional[dict]
    ) -> Optional[dict]:
        """依据“发布代次目录本身”构建不可变记录。

        只可在目录已完整可重组且 active 已切换到 ``cat`` 后调用：重组核验
        结果在此刻落锤。新段取本目录登记段；被替代段取旧目录登记、新目录
        不再引用的稳定清单。
        """
        new_names = sorted(self._catalog_segment_names(cat))
        old_names = self._catalog_segment_names(previous_cat)
        replaced_names = sorted(old_names - set(new_names))

        input_artifacts: List[dict] = []
        artifact_digests: Dict[str, str] = {}
        for aname, spec in cat.get("artifacts", {}).items():
            text = self.reassemble(cat, aname)  # 调用方已保证可重组
            artifact_digests[aname] = digest_text(text)
            input_artifacts.append({
                "name": aname,
                "fragment_count": len(spec["fragment_digests"]),
                "fragment_digests": spec["fragment_digests"],
                "total_bytes": spec["total_bytes"],
                "artifact_digest": artifact_digests[aname],
            })

        entries_by_seg: Dict[str, List[dict]] = {}
        for e in cat.get("entries", []):
            entries_by_seg.setdefault(e["segment"], []).append(
                {"digest": e["digest"], "offset": e["offset"],
                 "length": e["length"]})
        new_segments: List[dict] = []
        for name in new_names:
            ev = self._segment_evidence(name)
            if ev is None:
                # 切换后核验通过即意味着新段必然在盘上；缺失属内部错误。
                raise StoreError(f"固化整理记录时新段缺失：{name}")
            new_segments.append({
                "segment": name,
                "body_digest": ev["body_digest"],
                "size": ev["size"],
                "records": sorted(entries_by_seg.get(name, []),
                                  key=lambda r: r["offset"]),
            })

        replaced_segments: List[dict] = []
        for name in replaced_names:
            ev = self._segment_evidence(name)
            replaced_segments.append({
                "segment": name,
                # 清扫/重开补建时旧段可能已物理消失：摘要留空，核验时
                # 必须如实报告该被替代段无法复核。
                "body_digest": ev["body_digest"] if ev else None,
                "size": ev["size"] if ev else None,
            })

        return {
            "version": _RECORD_VERSION,
            "compaction_id": cat.get("compaction_id"),
            "published_generation": cat["generation"],
            "previous_generation": previous_cat["generation"]
            if previous_cat else None,
            "published_at": cat.get("created_at") or _utcnow(),
            "sealed_at": _utcnow(),
            "input_artifacts": input_artifacts,
            "new_segments": new_segments,
            "replaced_segments": replaced_segments,
            "reassembly_check": {
                "verified": True,
                "artifact_digests": artifact_digests,
                "checked_at": _utcnow(),
            },
            "replaced_safe_basis": (
                "活动目录已原子切换到发布代次，且新目录经逐条片段摘要核验"
                "可重组全部输入工件；下列旧段不再被任何活动目录登记引用，"
                "故当时可安全退出（先移入 trash），其字节已随本记录封存。"
            ),
        }

    def _write_record_if_absent(self, record: dict) -> bool:
        """固化记录：仅当不存在时写入，已发布记录绝不被改写。返回是否新写。"""
        path = self._record_path(record["compaction_id"])
        if os.path.exists(path):
            return False
        self._preserve_evidence(record)
        os.makedirs(self.record_dir, exist_ok=True)
        _atomic_write_bytes(
            path, json.dumps(record, ensure_ascii=False, indent=2)
            .encode("utf-8"))
        _fsync_dir(self.record_dir)
        return True

    def _evidence_dir(self, compaction_id: str) -> str:
        return os.path.join(self.record_dir, "evidence",
                            _safe(compaction_id))

    def _preserve_evidence(self, record: dict) -> None:
        """把记录所列段（新段与被替代段）逐字节封存进不可变证据区。

        段本身内容寻址、不可变；优先硬链接（同卷、原子且零拷贝），跨卷时
        退回读改写。必须在清扫旧段*之前*完成，使被替代段日后移出
        ``segments/``、被 trash 物理清除后仍可逐字节复核。
        """
        cid = record["compaction_id"]
        ev_dir = self._evidence_dir(cid)
        os.makedirs(ev_dir, exist_ok=True)
        listed = list(record.get("new_segments", [])) + list(
            record.get("replaced_segments", []))
        for rec in listed:
            name = rec["segment"]
            dst = os.path.join(ev_dir, name)
            if os.path.exists(dst):
                continue  # 上次固化途中崩溃的残留：内容寻址同名即同内容
            src = self._locate_segment(name)
            if src is None:
                if rec.get("body_digest") is None:
                    continue  # 构建时即已消失的被替代段，无字节可封存
                raise StoreError(f"固化整理记录时段缺失：{name}")
            try:
                os.link(src, dst)
            except OSError:
                with open(src, "rb") as f:
                    data = f.read()
                with open(dst, "wb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
        _fsync_dir(ev_dir)

    def _evidence_path(self, compaction_id: str, name: str) -> str:
        return os.path.join(self._evidence_dir(compaction_id), name)

    def _predecessor_catalog(self, gen: int) -> Optional[dict]:
        """切换前的活动目录：代次号小于 ``gen`` 且仍在 generations/ 的最近一份。"""
        for g in range(gen - 1, 0, -1):
            p = self._gen_path(g)
            if os.path.exists(p):
                try:
                    return self._read_json(p)
                except (OSError, json.JSONDecodeError):
                    continue
        return None

    def _ensure_record(
        self, cat: dict, previous_cat: Optional[dict]
    ) -> None:
        """切换+核验已成立但记录尚缺时（如断电在目录落盘后、重开前滚）
        补固化首次裁决证据；记录已存在则原封不动。必须在清扫旧段前调用。"""
        if os.path.exists(self._record_path(cat.get("compaction_id"))):
            return
        record = self._build_record(cat, previous_cat)
        self._write_record_if_absent(record)

    def list_records(self) -> List[dict]:
        """列出全部已发布整理记录（含实时可核验性），按发布代次排序。"""
        out: List[dict] = []
        if not os.path.isdir(self.record_dir):
            return out
        for n in sorted(os.listdir(self.record_dir)):
            if not n.startswith(_RECORD_PREFIX) or not n.endswith(".json"):
                continue
            cid = n[len(_RECORD_PREFIX):-len(".json")]
            item = self.get_record_by_safe_name(cid)
            if item is not None:
                out.append(item)
        out.sort(key=lambda x: (x["record"] or {}).get(
            "published_generation", 0))
        return out

    def get_record(self, compaction_id: str) -> Optional[dict]:
        return self.get_record_by_safe_name(_safe(compaction_id))

    def get_record_by_safe_name(self, safe_name: str) -> Optional[dict]:
        """按记录文件名取记录，并依据首次裁决封存的证据重新核验。

        核验只认记录自身、其发布代次目录与固化时封存的段字节副本
        （``records/evidence/<标识>/``）；**绝不**读取当前活动目录或当前
        正式段区的段充当历史证据——后者只作为“现状位置”展示。证据缺失
        （含被替代段未封存成功）或摘要不符 => ``verifiable=False``。
        """
        path = os.path.join(self.record_dir,
                            f"{_RECORD_PREFIX}{safe_name}.json")
        if not os.path.exists(path):
            return None
        problems: List[str] = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                record = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            return {
                "found": True,
                "compaction_id": safe_name,
                "record_file": os.path.basename(path),
                "record": None,
                "verification": {
                    "verifiable": False,
                    "problems": [f"记录文件无法读取或解析：{exc}"],
                },
            }

        cid = record.get("compaction_id")
        gen = record.get("published_generation")
        gen_path = self._gen_path(gen) if isinstance(gen, int) else None
        gen_present = bool(gen_path and os.path.exists(gen_path))

        # 1) 发布代次目录必须仍在，且其归属与记录一致（防目录被掉包）
        catalog_match = False
        pub_cat = None
        if not gen_present:
            problems.append(
                f"发布代次目录缺失：gen-{gen:06d}.json"
                if isinstance(gen, int) else "发布代次目录缺失")
        else:
            try:
                with open(gen_path, "r", encoding="utf-8") as f:
                    pub_cat = json.load(f)
                catalog_match = self._record_matches_catalog(record, pub_cat)
                if not catalog_match:
                    problems.append(
                        "发布代次目录内容与记录登记不一致（归属被改动）")
            except (OSError, json.JSONDecodeError) as exc:
                problems.append(f"发布代次目录无法解析：{exc}")

        # 2) 逐段核对“首次裁决封存副本”，并标注其当前现状位置（仅供观察）
        def _check_segment(rec: dict, *, is_new: bool) -> dict:
            name = rec["segment"]
            ev_path = self._evidence_path(cid, name)
            live = self._seg_path(name)
            trash = os.path.join(self.trash_dir, name)
            if os.path.exists(live):
                current_location = "segments"
            elif os.path.exists(trash):
                current_location = "trash"
            else:
                current_location = "missing"
            item = {"segment": name, "current_location": current_location,
                    "evidence_present": False, "body_digest_ok": False,
                    "records_ok": None, "problems": []}
            if not os.path.exists(ev_path):
                item["problems"].append(
                    f"{name}: 首次裁决证据副本缺失（不可以当前目录段顶替）")
                problems.extend(item["problems"])
                return item
            item["evidence_present"] = True
            with open(ev_path, "rb") as f:
                body = f.read()
            got_body = digest_text(body)
            want_body = rec.get("body_digest")
            if not want_body:
                item["problems"].append(
                    f"{name}: 记录未固化段体摘要，无法复核")
            elif got_body != want_body:
                item["problems"].append(
                    f"{name}: 证据段体摘要不符 记录={want_body[:12]} "
                    f"实读={got_body[:12]}")
            else:
                item["body_digest_ok"] = True
            if is_new:
                rec_ok = True
                contents: List[dict] = []
                for r in rec.get("records", []):
                    try:
                        text, got = read_record_at(ev_path, r["offset"])
                    except (StoreError, UnicodeDecodeError, OSError) as exc:
                        item["problems"].append(
                            f"{name}@{r['offset']}: 证据段读取失败 {exc}")
                        rec_ok = False
                        continue
                    if got != r["digest"]:
                        item["problems"].append(
                            f"{name}@{r['offset']}: 片段摘要不符 "
                            f"记录={r['digest'][:12]} 实读={got[:12]}")
                        rec_ok = False
                    contents.append({
                        "digest": r["digest"], "offset": r["offset"],
                        "length": r["length"],
                        "preview": text if len(text) <= 80
                        else text[:77] + "...",
                    })
                item["records_ok"] = rec_ok
                item["contents"] = contents
            if item["problems"]:
                problems.extend(item["problems"])
            return item

        new_results = [_check_segment(r, is_new=True)
                       for r in record.get("new_segments", [])]
        replaced_results = [_check_segment(r, is_new=False)
                            for r in record.get("replaced_segments", [])]
        evidence_ok = all(
            x["evidence_present"] and x["body_digest_ok"]
            and (x["records_ok"] in (True, None))
            for x in new_results + replaced_results) \
            and bool(record.get("new_segments"))

        # 3) 仅凭“发布代次目录顺序 + 封存证据段”重组全部输入工件
        reassembly = {"ok": False, "artifact_digests": {}, "previews": {},
                      "problems": []}
        if pub_cat is not None and catalog_match and evidence_ok:
            entries = {e["digest"]: e for e in pub_cat.get("entries", [])}
            ok_all = True
            first_verdict = record.get("reassembly_check", {}) \
                .get("artifact_digests", {})
            for spec in record.get("input_artifacts", []):
                aname = spec["name"]
                parts: List[str] = []
                for dg in pub_cat["artifacts"][aname]["fragment_digests"]:
                    e = entries.get(dg)
                    if e is None:
                        reassembly["problems"].append(
                            f"工件 {aname}: 发布目录缺片段登记 {dg[:12]}")
                        ok_all = False
                        break
                    ev_path = self._evidence_path(cid, e["segment"])
                    try:
                        text, got = read_record_at(ev_path, e["offset"])
                    except (StoreError, UnicodeDecodeError, OSError) as exc:
                        reassembly["problems"].append(
                            f"工件 {aname}: 证据段无法读取 {exc}")
                        ok_all = False
                        break
                    if got != dg:
                        reassembly["problems"].append(
                            f"工件 {aname}: 片段摘要不符 {dg[:12]}")
                        ok_all = False
                        break
                    parts.append(text)
                else:
                    text = "".join(parts)
                    dg = digest_text(text)
                    reassembly["artifact_digests"][aname] = dg
                    reassembly["previews"][aname] = (
                        text if len(text) <= 120 else text[:117] + "...")
                    if dg != spec.get("artifact_digest") \
                            or dg != first_verdict.get(aname):
                        reassembly["problems"].append(
                            f"工件 {aname}: 重组摘要与首次裁决不符")
                        ok_all = False
            reassembly["ok"] = ok_all and bool(
                record.get("input_artifacts"))
            if not reassembly["ok"]:
                problems.extend(reassembly["problems"])

        verifiable = (
            gen_present
            and catalog_match
            and evidence_ok
            and reassembly["ok"]
            and record.get("reassembly_check", {}).get("verified") is True
        )
        return {
            "found": True,
            "compaction_id": cid,
            "record_file": os.path.basename(path),
            "record": record,
            "verification": {
                "verifiable": verifiable,
                "checked_against_generation": gen,
                "generation_present": gen_present,
                "catalog_matches_record": catalog_match,
                "current_active_generation": self.active_generation(),
                "new_segments": new_results,
                "replaced_segments": replaced_results,
                "reassembly": reassembly,
                "problems": problems,
            },
        }

    @staticmethod
    def _record_matches_catalog(record: dict, cat: dict) -> bool:
        """发布代次目录的归属必须与记录逐项一致。"""
        if cat.get("generation") != record.get("published_generation"):
            return False
        if cat.get("compaction_id") != record.get("compaction_id"):
            return False
        cat_art = cat.get("artifacts", {})
        rec_art = {a["name"]: a for a in record.get("input_artifacts", [])}
        if set(cat_art.keys()) != set(rec_art.keys()):
            return False
        for aname, spec in cat_art.items():
            r = rec_art[aname]
            if spec.get("fragment_digests") != r.get("fragment_digests"):
                return False
        cat_ent = {e["digest"]: e for e in cat.get("entries", [])}
        rec_digests = {rr["digest"]
                       for s in record.get("new_segments", [])
                       for rr in s.get("records", [])}
        if set(cat_ent.keys()) != rec_digests:
            return False
        for seg in record.get("new_segments", []):
            for rr in seg.get("records", []):
                e = cat_ent[rr["digest"]]
                if e.get("segment") != seg["segment"] \
                        or e.get("offset") != rr["offset"] \
                        or e.get("length") != rr["length"]:
                    return False
        return True

    # -- 调试/观察 ---------------------------------------------------------
    def list_segments(self) -> List[dict]:
        out = []
        for name in sorted(os.listdir(self.seg_dir)):
            if name.startswith(_SEGMENT_PREFIX):
                out.append({"name": name,
                            "size": os.path.getsize(self._seg_path(name))})
        return out

    def list_trash(self) -> List[str]:
        return sorted(os.listdir(self.trash_dir))


def fragments_of(art: Artifact) -> List[Fragment]:
    return art.fragments


def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in name)[:80]


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
