# OpenAgent Protocol Simulation

This project implements the first-stage OpenAgent protocol-level simulation line.

## Quick start

```bash
uv sync --extra dev
uv run openagent-sim run-preset --matrix configs/default_experiment_matrix.yaml --preset smoke --backend sim --output-root results
uv run openagent-sim summarize --experiment-id openagent_large_scale_v2026_03_14_smoke --output-root results
uv run openagent-sim export-figures --experiment-id openagent_large_scale_v2026_03_14_smoke --output-root results
uv run openagent-sim render-plots --experiment-id openagent_large_scale_v2026_03_14_smoke --output-root results
uv run pytest
```

## Presets

- `smoke`: tiny functional verification matrix
- `paper_small`: paper-aligned reduced matrix for local macOS runs
- `paper_small_perturb`: paper-aligned perturbation gate (`mild_reorder`, `mild_loss`)

## Backends

- `sim`: discrete-event protocol simulation (default)
- `libp2p`: real-node orchestration backend (supports `openagent` and `baseline_full_payload_gossip`)

Example:

```bash
uv run openagent-sim run-preset --matrix configs/default_experiment_matrix.yaml --preset paper_small --backend sim --output-root results
```

Local `libp2p` 50-node baseline on MBP:

```bash
cd /Users/med/Desktop/OpenAgent/openclaw-P2P && mkdir -p .local/bin && go build -o .local/bin/openagent-node ./cmd
uv run openagent-sim run-preset --matrix configs/libp2p_mbp_50.yaml --preset libp2p_mbp50_baseline --backend libp2p --output-root results
uv run openagent-sim summarize --experiment-id openagent_libp2p_mbp50_v2026_03_16_libp2p_mbp50_baseline --output-root results
uv run openagent-sim export-figures --experiment-id openagent_libp2p_mbp50_v2026_03_16_libp2p_mbp50_baseline --output-root results
uv run openagent-sim render-plots --experiment-id openagent_libp2p_mbp50_v2026_03_16_libp2p_mbp50_baseline --output-root results
```

Local `libp2p` 50-node A/B on MBP (`openagent` vs `baseline_full_payload_gossip`):

```bash
uv run openagent-sim run-preset --matrix configs/libp2p_mbp_50.yaml --preset libp2p_mbp50_ab_normal --backend libp2p --output-root results
uv run openagent-sim summarize --experiment-id openagent_libp2p_mbp50_v2026_03_16_libp2p_mbp50_ab_normal --output-root results
uv run openagent-sim export-figures --experiment-id openagent_libp2p_mbp50_v2026_03_16_libp2p_mbp50_ab_normal --output-root results
uv run openagent-sim render-plots --experiment-id openagent_libp2p_mbp50_v2026_03_16_libp2p_mbp50_ab_normal --output-root results
```

Ubuntu server Docker orchestration (host network):

```bash
# Server path example:
cd /data/openagent

# Build/pull image with openagent-node in PATH.
docker build -t openagent-node:latest /data/openclaw-P2P

# Smoke (10 nodes):
PYTHONPATH=src uv run python -m openagent_sim run-preset \
  --matrix configs/libp2p_server_docker.yaml \
  --preset server_docker_smoke10 \
  --backend libp2p \
  --output-root /data/openagent-exp/results

# Scale (openagent only):
PYTHONPATH=src uv run python -m openagent_sim run-preset \
  --matrix configs/libp2p_server_docker.yaml \
  --preset server_docker_scale_openagent \
  --backend libp2p \
  --output-root /data/openagent-exp/results

# 1000-node bootstrap scan (B=8/10/12/16, fanout=2, seeds=1001/1002/1003):
./scripts/run_server_bootstrap_scan_1000.sh \
  configs/libp2p_server_docker_bootstrap_scan_1000.yaml \
  bootstrap_scan_1000 \
  /data/openagent-exp/results

# 1000-node staged launch (low parallelism + batch + retry + backpressure):
PYTHONPATH=src uv run python -m openagent_sim run-preset \
  --matrix configs/libp2p_server_docker_1000_staged.yaml \
  --preset server_docker_1000_staged \
  --backend libp2p \
  --output-root /data/openagent-exp/results

# Generate markdown report from summary.csv:
uv run python scripts/analyze_bootstrap_scan.py \
  --summary-csv /data/openagent-exp/results/summary/<experiment_id>/summary.csv \
  --output-md /data/openagent-exp/results/reports/<experiment_id>_report_zh.md
```

Note: current `libp2p` backend does not inject `loss/reorder/churn` netem perturbations yet; `network_profile` is baseline labeling for now.
Note: `baseline_full_payload_gossip` in `libp2p` uses repeated gossip publishes as a network-load approximation.
Note: `node_executor=docker` currently supports `docker_network_mode=host` only.
Note: `libp2p_orchestration.bootstrap_pool_size` and `bootstrap_fanout_per_node` can be overridden in preset dimensions via `bootstrap_pool_sizes` and `bootstrap_fanout_per_node`.

## Outputs

- `results/raw/<experiment_id>/<variant>/<seed>.csv`
- `results/summary/<experiment_id>/summary.csv`
- `results/plots/<experiment_id>/figure_A.csv`
- `results/plots/<experiment_id>/figure_B.csv`
- `results/plots/<experiment_id>/figure_C.csv`

## Environment Alignment

- Probe server baseline:
  - `./scripts/probe_ubuntu_env.sh .local/server-baseline.json`
- Run in local Ubuntu container:
  - `./scripts/run_ubuntu_container.sh "apt update && apt install -y python3 python3-pip"`
