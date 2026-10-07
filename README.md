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
- `GET  /api/status` — 活动代次、每份工件重组摘要、片段(段,偏移)索引、恢复裁决、段/trash
- `POST /api/recover` — 显式重开收敛，返回同一状态快照
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
```

## 目录

- `app/storage.py` — 崩溃安全存储引擎（段格式、目录、恢复、清扫、拒因）
- `app/worker.py` — 整理工作进程 CLI（`--crash` 注入断电，退出码裁决）
- `app/server.py` — Flask API（向子进程派发整理，崩溃只杀工作进程）
- `app/static/index.html` — 演练编排与实况页面
- `tests/` — pytest：引擎、断电子进程矩阵、HTTP API（含真实服务端到端）
- `scripts/verify.py` — Compose `verify` 单次服务入口
