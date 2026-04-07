# Sparse-Demand Results Summary (CN)

## Overview

我们基于已完成的 two-stage deterministic evidence pack（100 节点、200 requests）整理了 sparse-demand 证据。结果显示，在 medium 设定下，三个 seed 的 p_coarse、p_full 与 coarse_to_full 波动较小；在固定 seed 下，constraint 从 loose 到 tight 时，p_full 与 coarse_to_full 均单调下降。同时，full-match availability 始终维持在高位，说明需求稀疏并不等于请求无解。

## Analysis

从稳定性看，multi-seed 的 p_coarse_mean、p_full_mean、coarse_to_full_mean 的变异系数分别为 0.032412、0.063437、0.046469，说明 coarse 与 full 两层比例在不同随机种子下保持同一量级，不属于单次抽样偶然。进一步看约束收紧实验，p_full 与 coarse_to_full 在 loose→medium→tight 上严格下降，monotonicity 检查结果为 p_full=True、coarse_to_full=True。这验证了 two-stage 结构中“声明层可见性稳定、细约束决定最终匹配稀疏度”的机制。与此同时，full_match_available_rate 在各组中保持接近 1，对应 zero_full_match_rate 维持低位，说明虽然 full match 相对 coarse 显著收缩，但绝大多数请求仍至少存在可行提供者。因此，当前结果已经形成完整证据链：two-stage deterministic 已建立且不再由 probabilistic pull 主导，稀疏性来源于能力分布与约束结构本身，可直接支撑论文中的 Workload Characterization / Sparse-Demand Motivation 小节。
