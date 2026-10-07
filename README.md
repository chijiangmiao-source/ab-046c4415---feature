# 地面成像站 · 共享片段紧凑存储

档案员在把可重复引用的短文本片段整理成紧凑存储前，必须确认**任何断电**都
不会让已登记工件指向错误偏移、或指向被提前清除的旧段。本项目实现了一套
崩溃安全的“段文件 + 代次目录 + 原子切换 + 重开收敛 + 幂等重传”服务，
并提供真实 API 联调的网页与 Compose 单次校验服务 `verify`。

## 崩溃安全协议

磁盘布局（数据卷 `/data`）：

```
data/
  segments/seg-<内容指纹16位>   # 不可变段：定长记录 [4B长度][UTF-8文本][32B sha256]
  generations/gen-000007.json  # 完整索引（代次目录）：工件→片段摘要序列、片段→(段,偏移)
  active -> generations/gen-000006.json   # 唯一活动目录，rename(2) 原子切换
  pending/<compaction_id>/manifest.json   # 在途作业清单：声明其新段在开关切换前不得清扫
  records/record-<compaction_id>.json     # 不可变整理归属证据：仅随阶段 E 裁决固化，只建不改
  trash/                       # 旧段清扫暂存区（仅留最近一代供观察）
```

一次整理（`compaction_id` 为稳定标识）严格分阶段：

| 阶段 | 动作 | 断电后重开的裁决 |
|---|---|---|
| A | 校验旧活动目录可重组全部工件 | 旧目录不动 |
| B | 写作业清单 + 新段临时文件 `fsync` 后 `rename` 入位 | 段不可变、无目录引用；重传按内容寻址复用，不新建 |
| C | 完整新索引写临时文件 `fsync` 后 `rename` 入位 | 目录已完整可重组 ⇒ **前滚**完成切换；否则保持旧目录、悬挂目录留证 |
| D | 符号链接 `active` 经 `rename(2)` **原子切换**到新代次 | 切换点只有“旧”或“新”，不存在半个目录 |
| E | 新目录验证可重新拼出**全部**工件后，旧段才移入 `trash` | 切换后/清扫前断电 ⇒ 重开补完清扫；在途作业的段凭清单保留 |
| F | 与 E 的裁决一并固化该标识的**不可变归属证据** | C/D/F 阶段断电 ⇒ 重开前滚并补固化；B 阶段断电/被拒不产生记录 |

阶段 F 的归属证据（`records/record-<compaction_id>.json`）只在**完整新目录已能
重组全部工件、且 `active` 原子切换成功后**写入，内容是该次发布的首次裁决快照：

- 发布代次 `generation`、目录创建/证据固化时间；
- **输入工件摘要**：每份工件的片段摘要序列、字节数、重组全文摘要与预览；
- 该次活动目录**接管的每个新段**：段名、大小、段文件内容摘要，以及段内每个
  片段的偏移、摘要、长度、预览；
- **被替代旧段的稳定清单**：段名、最后所属代次、固化时/当前去向（segments、
  trash、已物理清除）与段内容摘要——它们当时可安全退出，是因为完整新目录已
  重组全部工件且开关已切换；
- 首次裁决的**重组核验结果**（逐工件重组摘要）。

记录**只建一次、绝不改写**：后续整理、旧段移入 trash、显式 `recover`、相同
请求重传都不影响已发布记录。历史标识的查询只依据记录自身登记的
(段,偏移) 与磁盘段体（含 trash）重新复核，**绝不回退显示当前活动目录的段**：

- 标识从未发布（含中断、拒绝、只写在途段的作业）⇒ 明确**未找到（404）**；
- 记录所列段缺失或其摘要无法复核 ⇒ **该记录不可验证**，页面与接口给出
  `verifiable:false` 及具体 `problems`，并仍展示首次裁决登记的历史段集合。

关键不变量：

- **先持久化新段与完整索引，再原子切换唯一目录代次**；
- 旧段**只能**在新目录可重组全部工件之后被清扫（先移入 `trash`）；
- 重开后收敛为一份完整目录：前滚（C 已完成）或保留旧目录（B 阶段崩溃），
  悬挂的半成品段/临时链接被清理；
- 重传同一 `compaction_id` 与同一内容：**不新建段、不换代次、结果不变**；
- `compaction_id` 复用但 **工件集合不同 / 片段摘要不符 / 新段缺失**：
  保留原活动目录，返回**首个拒因**（HTTP 409，worker 退出码 2）。

跨进程安全：所有恢复/整理在数据目录内一把 `flock` 下进行，API 服务与
工作子进程并发访问同一数据卷不会互相穿插。

## HTTP API

- `GET  /healthz`（路径可用 `HEALTH_PATH` 配置）— 健康入口
- `GET  /` — 演练网页（真实 API 联调，5s 轮询状态）
- `GET  /api/status` — 活动代次、每份工件重组摘要、片段(段,偏移)索引、恢复裁决、段/trash、已发布整理记录摘要
- `POST /api/recover` — 显式重开收敛，返回同一状态快照
- `GET  /api/compactions` — 已发布整理记录（不可变归属证据）清单及当前可复核性
- `GET  /api/compactions/<compaction_id>` — 追溯某标识的首次裁决：发布代次、输入工件摘要、每个新段内容摘要、被替代段稳定清单与重组核验结果；不存在返 404，记录所列段缺失/摘要无法复核时返 200 且 `verifiable:false` 并列明 `problems`
- `POST /api/compact` — 请求体：
  ```json
  {
    "compaction_id": "drill-001",
    "crash": "after_segments | after_catalog | after_switch | during_segments | null",
    "artifacts": [
      {"name": "全景", "fragments": ["卫星过境-", "多光谱扫描"]},
      {"name": "局部", "fragments": ["多光谱扫描", "雷达回波"]}
    ]
  }
  ```
  工件数量 2–8；片段按顺序组成工件，可跨工件重复引用（存储层去重入段）。
  `crash` 由独立工作子进程 `os._exit(1)` 模拟真实断电，API 进程仍存活。

## 运行

宿主端口与健康入口由 Compose 配置（`.env` 或环境变量）：

```bash
cp .env.example .env          # HOST_PORT=8080, HEALTH_PATH=/healthz
docker compose up -d api      # 网页与 API: http://localhost:${HOST_PORT}
docker compose run --rm verify
```

`verify` 是**单次服务**，依次核对并以退出码结束（0 通过）：

1. 构建检查（模块编译、导入）；
2. `pytest` 全套代码测试；
3. 对四个中断点做“断电(退出码 1) → 重开收敛 → 重传不新建段/结果不变”；
4. 对运行中的 `api` 做真实 HTTP 冒烟（健康入口、建演练、压缩、断电、
   重开、幂等重传、409 拒绝裁决、状态接口）。

本地无 Docker 时：

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest tests -q
IMAGING_DATA_DIR=./data PORT=8080 .venv/bin/python -m app.server
API_BASE=http://127.0.0.1:8080 .venv/bin/python scripts/verify.py
# 或直接用工作进程 CLI 演练断电：
.venv/bin/python -m app.worker --data ./data compact --input req.json --crash after_switch
.venv/bin/python -m app.worker --data ./data recover
# 列出/追溯不可变整理归属证据（退出码 0 可验证；4 不可验证；5 未找到）
.venv/bin/python -m app.worker --data ./data records
.venv/bin/python -m app.worker --data ./data records --id drill-001
```

## 目录

- `app/storage.py` — 崩溃安全存储引擎（段格式、目录、恢复、清扫、拒因、不可变整理记录）
- `app/worker.py` — 整理工作进程 CLI（`--crash` 注入断电，退出码裁决；`records` 追溯）
- `app/server.py` — Flask API（向子进程派发整理，崩溃只杀工作进程）
- `app/static/index.html` — 演练编排、实况页面与整理记录选择/追溯视图
- `tests/` — pytest：引擎、断电子进程矩阵、HTTP API、不可变归属证据（含真实服务端到端）
- `scripts/verify.py` — Compose `verify` 单次服务入口
