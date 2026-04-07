# Sparse-Demand Metric Definitions

- `visible`: Number of nodes that received the request declaration on the public topic.
- `coarse_match`: Number of nodes satisfying declaration-level coarse criteria (`required_family` + `coarse_domain`).
- `full_match`: Number of nodes satisfying payload-level full constraints (tools/trust/latency and configured specialization rule).
- `p_coarse`: `coarse_match / visible`.
- `p_full`: `full_match / visible`.
- `coarse_to_full`: `full_match / max(coarse_match, 1)`.
- `full_match_available_rate`: Fraction of requests with `full_match >= 1`.
- `zero_full_match_rate`: Fraction of requests with `full_match == 0`.

These definitions follow the existing two-stage workload characterization outputs and do not change the current metric semantics.
