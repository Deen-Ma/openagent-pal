# Sparse-Demand Results Summary (EN)

## Overview

Using the completed two-stage deterministic evidence pack (100 nodes, 200 requests), we observe a consistent sparse-demand pattern. Under the medium setting, p_coarse, p_full, and coarse_to_full remain stable across three seeds. Under fixed seed with constraint tightening (loose/medium/tight), both p_full and coarse_to_full decrease monotonically. At the same time, full-match availability stays high, indicating that sparsity does not imply unsatisfiable demand.

## Analysis

The multi-seed robustness statistics show low cross-seed variance in the key ratios, with coefficients of variation of 0.032412 for p_coarse_mean, 0.063437 for p_full_mean, and 0.046469 for coarse_to_full_mean. This supports that the observed sparsity is not an artifact of a single random realization. In the constraint sweep, both p_full and coarse_to_full follow a strict loose-to-tight decline, and the monotonicity checks are p_full=True and coarse_to_full=True. These results are consistent with the intended two-stage semantics: declaration-level coarse visibility remains broad, while payload-level constraints drive selective full matching. Importantly, full_match_available_rate remains close to one, with low zero_full_match_rate, meaning that the system remains largely solvable despite strong sparsity. Taken together, the current package provides a coherent and publication-ready motivation for sparse demand in the workload characterization section.
