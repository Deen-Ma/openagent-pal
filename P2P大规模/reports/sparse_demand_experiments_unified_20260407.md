# Sparse-Demand 实验统一报告（截至 2026-04-07）

## 1. 目标与结论先看

本报告统一整理当前仓库内已完成的 sparse-demand 相关实验，重点回答四件事：

1. 是否已有 deterministic two-stage 版本。
2. 是否已有 multi-seed 稳定性证据。
3. 是否已有 constraint sweep（loose/medium/tight）单调性证据。
4. 是否已有 full-match availability 指标并可用于论证。

结论：以上四项均已具备，且在最新完整 evidence pack 中都已落盘。

## 2. 已纳入的完成批次（仅完整完成）

| 阶段 | 实验/批次 | 状态 | 说明 |
| --- | --- | --- | --- |
| A | `workload_char_public_discovery_n100_20260402T0935Z` | completed | 旧版 probabilistic baseline（非 two-stage） |
| B1 | `workload_char_public_discovery_two_stage_n100_20260402T151615Z` | completed | two-stage deterministic 首轮 |
| B2 | `workload_char_public_discovery_two_stage_n100_resp60_20260403T064741Z` | completed | two-stage，窗口调整后的复测 |
| C | `workload_char_public_discovery_two_stage_n100_evidence_pack_20260406T012900Z` | completed | 5 轮证据包（3 seeds + loose/medium/tight） |

说明：本报告按你的要求，只纳入“最终完整完成”批次，不包含中断/未完成尝试目录。

## 3. 方法演进与关键结果

### 3.1 阶段 A：旧版 probabilistic baseline（对照）

配置特征：`pull_policy=probabilistic`，`pull_probability=0.3`。  
核心结果（n=100, requests=200, seed=20260402）：

- `visible_mean=100.0`
- `eligible_mean=24.98`，`p_elig=0.2498`
- `pull_mean=7.645`，`p_pull=0.07645`
- `respond_mean=1.385`，`p_resp=0.01385`

解读：pulling 强烈受概率策略主导，不适合作为 sparse-demand 结构证据主线。

### 3.2 阶段 B：two-stage deterministic（主方法）

方法特征：

- declaration 做 coarse match；
- payload 做 full match；
- `pull_policy=deterministic_coarse_match`（coarse 命中即拉取）；
- response 仅作次要观察。

#### B1: two_stage_n100（原始窗口）

- `visible_mean=100.0`
- `coarse_mean=7.715`，`p_coarse=0.07715`
- `full_mean=1.525`，`p_full=0.01525`
- `actual_pull_mean=7.715`，`p_pull=0.07715`
- `actual_response_mean=0.265`，`p_resp=0.00265`
- `coarse_to_full=0.216211`
- `zero_responder_rate=0.76`
- `first_response_available_rate=0.24`

#### B2: two_stage_n100_resp60（窗口扩展复测）

- `visible/coarse/full/pull/resp` 主均值与 B1 保持一致：
  - `visible_mean=100.0`
  - `coarse_mean=7.715`
  - `full_mean=1.525`
  - `actual_pull_mean=7.715`
  - `actual_response_mean=0.265`
- `coarse_to_full=0.216211`
- `zero_responder_rate=0.78`
- `first_response_available_rate=0.22`

解读：本阶段已形成自然漏斗 `visible -> coarse -> full -> pull -> response`，且 pull 与 coarse 基本对齐，符合 deterministic two-stage 设计意图。

## 4. 阶段 C：Evidence Pack（核心证据）

批次：`workload_char_public_discovery_two_stage_n100_evidence_pack_20260406T012900Z`  
逻辑 run 共 5 轮（全部成功）：

1. `medium@20260402`
2. `medium@20260412`
3. `medium@20260422`
4. `loose@20260402`
5. `tight@20260402`

### 4.1 Multi-seed 稳定性（仅 medium）

| seed | visible_mean | coarse_mean | full_mean | p_coarse_mean | p_full_mean | coarse_to_full_mean | full_match_available_rate | zero_full_match_rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20260402 | 100.0 | 8.02 | 1.995 | 0.0802 | 0.01995 | 0.261841 | 0.99 | 0.01 |
| 20260412 | 100.0 | 8.13 | 1.84 | 0.0813 | 0.0184 | 0.237807 | 1.0 | 0.0 |
| 20260422 | 100.0 | 8.635 | 2.15 | 0.08635 | 0.0215 | 0.263828 | 0.995 | 0.005 |

跨 seed 聚合统计：

- `p_coarse_mean`: mean `0.082617`, std `0.002678`, cv `0.032412`
- `p_full_mean`: mean `0.01995`, std `0.001266`, cv `0.063437`
- `coarse_to_full_mean`: mean `0.254492`, std `0.011826`, cv `0.046469`
- `full_match_available_rate`: mean `0.995`, std `0.004082`, cv `0.004103`

解读：multi-seed 下 coarse 与 full 的比例量级稳定，支持“不是偶然抽样”的论点。

### 4.2 Constraint sweep 单调性（固定 seed=20260402）

| mode | visible_mean | coarse_mean | full_mean | p_coarse_mean | p_full_mean | coarse_to_full_mean | full_match_available_rate | zero_full_match_rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| loose | 100.0 | 8.02 | 5.475 | 0.0802 | 0.05475 | 0.681812 | 1.0 | 0.0 |
| medium | 100.0 | 8.02 | 1.995 | 0.0802 | 0.01995 | 0.261841 | 0.99 | 0.01 |
| tight | 100.0 | 8.02 | 1.185 | 0.0802 | 0.01185 | 0.160747 | 0.985 | 0.015 |

单调性检查（聚合器输出）：

- `p_full_monotonic = True`
- `coarse_to_full_monotonic = True`

解读：在 declaration 不变的前提下，约束收紧时 `p_full` 与 `coarse_to_full` 系统下降，满足预期。

### 4.3 Full-match availability（已具备）

Evidence pack 中已稳定输出 `full_match_available_rate` 与 `zero_full_match_rate`：

- medium 三个 seed：`0.99 / 1.0 / 0.995`
- sweep 三模式：`1.0 (loose), 0.99 (medium), 0.985 (tight)`

这说明请求在 full 级别总体可满足，但随着约束收紧，可满足性按预期下降。

## 5. 总体判断（sparse-demand 证据强度）

基于当前完成批次，sparse-demand 的证据链已完整：

1. two-stage deterministic 机制已建立，且 pulling 不再由概率按钮主导。
2. multi-seed 下 `p_coarse/p_full/coarse_to_full` 波动小，稳健性成立。
3. constraint tightening 下 `p_full/coarse_to_full` 单调下降，结构因果关系清晰。
4. full-match availability 已纳入主汇总，可直接用于论文/报告图表与叙事。

同时，`actual_response` 仍可记录并对照，但不是本轮 sparse-demand 结论的必要条件。

## 6. 数据源索引（可追溯）

### 阶段 A/B 单轮 summary

- `results/workload_char_public_discovery_n100_20260402T0935Z/results/run01/summaries/summary.json`
- `results/workload_char_public_discovery_two_stage_n100_20260402T151615Z/results/run01/summaries/summary.json`
- `results/workload_char_public_discovery_two_stage_n100_resp60_20260403T064741Z/results/run01/summaries/summary.json`

### 阶段 C evidence pack 聚合

- `results/workload_char_public_discovery_two_stage_n100_evidence_pack_20260406T012900Z/aggregate/run_matrix.csv`
- `results/workload_char_public_discovery_two_stage_n100_evidence_pack_20260406T012900Z/aggregate/multi_seed_summary.csv`
- `results/workload_char_public_discovery_two_stage_n100_evidence_pack_20260406T012900Z/aggregate/constraint_sweep_summary.csv`
- `results/workload_char_public_discovery_two_stage_n100_evidence_pack_20260406T012900Z/aggregate/evidence_pack_summary.json`
- `results/workload_char_public_discovery_two_stage_n100_evidence_pack_20260406T012900Z/aggregate/evidence_pack_summary.md`
