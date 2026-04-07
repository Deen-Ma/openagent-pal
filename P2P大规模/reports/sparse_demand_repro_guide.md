# Sparse-Demand 实验复现指南

本指南对应当前仓库中已完成的 two-stage sparse-demand 实验链路，目标是便于后续重复实验与论文产物再生成。

## 1) 环境前提

- Python 3.10+（本仓库依赖 `matplotlib/pandas/numpy/pytest`）
- 可访问实验服务器（如需远端运行）
- Docker 与 `docker compose` 可用（远端）

## 2) 关键配置文件

- `configs/workload_char_public_discovery_n100.yaml`  
  旧版 probabilistic 对照配置
- `configs/workload_char_public_discovery_two_stage_n100.yaml`  
  two-stage deterministic 单轮配置
- `configs/workload_char_public_discovery_two_stage_n100_evidence_pack.yaml`  
  证据包配置（multi-seed + constraint sweep）

## 3) 关键脚本

- `scripts/discovery_workload_char_probe.py`  
  单轮编排（bootstrap + runtime + 聚合）
- `scripts/discovery_workload_evidence_pack.py`  
  5 轮 evidence pack 编排与跨 run 聚合
- `scripts/discovery_workload_aggregate.py`  
  单轮聚合（summary、per-request、plot-ready）
- `scripts/generate_sparse_demand_paper_pack.py`  
  论文可用产物导出（主表/图/中英文摘要/LaTeX）

## 4) 复现命令

### 4.1 运行 two-stage 证据包

```bash
python3 scripts/discovery_workload_evidence_pack.py \
  --config configs/workload_char_public_discovery_two_stage_n100_evidence_pack.yaml \
  --output-root results/workload_char_public_discovery_two_stage_n100_evidence_pack_<timestamp>
```

### 4.2 基于已完成 evidence pack 生成论文产物

```bash
python3 scripts/generate_sparse_demand_paper_pack.py \
  --repo-root . \
  --evidence-pack-root results/workload_char_public_discovery_two_stage_n100_evidence_pack_<timestamp> \
  --output-dir reports/sparse_demand_paper_pack_<date>
```

## 5) 核心输出检查

论文产物目录中应至少包含：

- `sparse_demand_main_table.csv/.md/.tex`
- `sparse_demand_funnel_medium_seed20260402.(png|pdf)`
- `sparse_demand_multiseed_stability.(png|pdf)`
- `sparse_demand_constraint_sweep.(png|pdf)`
- `sparse_demand_results_summary_cn.md`
- `sparse_demand_results_summary_en.md`
- `sparse_demand_section_snippet.tex`
- `sparse_demand_metric_definitions.md`

## 6) 口径说明

- 本链路主分析为 `visible -> coarse_match -> full_match`
- `actual_response` 为次要观察，不作为 sparse-demand 成立的必要条件
- 不修改已有结果口径，只做结果复用与论文化整理
