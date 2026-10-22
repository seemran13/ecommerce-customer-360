#!/usr/bin/env bash
# End-to-end local verification (no Docker/JVM services required).
# Usage: bash run_local.sh 0.02   # quick dev run (default)
#        bash run_local.sh 1.0    # full synthetic dataset
set -euo pipefail
SCALE=${1:-0.02}
RUN_STAMP=$(date +%Y%m%d_%H%M%S)
DEFAULT_DATA="data_small_run_${RUN_STAMP}"
if [[ "$SCALE" == "1" || "$SCALE" == "1.0" ]]; then
  DEFAULT_DATA="data_full_run_${RUN_STAMP}"
fi
# A new output directory per run prevents the generator's NDJSON append mode from
# mixing previous test runs or overwriting the user's existing data/ directory.
D=${DATA_ROOT:-$DEFAULT_DATA}
ARTIFACTS=${ARTIFACTS_DIR:-$D/ml_artifacts}
RAG_OUT=${RAG_OUT_DIR:-$D/rag_index}
mkdir -p "$D" "$ARTIFACTS" "$RAG_OUT"

python3 data_gen/generate_data.py --out "$D" --scale "$SCALE"
python3 data_gen/inject_duplicates.py "$D" 0.005
python3 local_pipeline/cleanse_local.py "$D/weblogs" "$D/clean" "$D/mysql/products.csv"
python3 local_pipeline/abandonment_sim.py "$D/clean/events_clean.pkl" "$D"
python3 ml/train_propensity.py "$D" "$ARTIFACTS"
python3 ml/segment_kmeans.py "$D"
python3 ml/score_customers.py "$D" "$ARTIFACTS"
python3 rag/build_index.py --data "$D" --out "$RAG_OUT"
RAG_INDEX="$RAG_OUT/index.pkl" python3 rag/eval_rag.py
python3 bi/export_powerbi.py "$D"
python3 drills/reconcile.py --data "$D"
C360_DATA="$D" python3 -m pytest -q tests
printf '\nCUSTOMER360_LOCAL_RUN=PASS data=%s scale=%s artifacts=%s rag=%s\n' "$D" "$SCALE" "$ARTIFACTS" "$RAG_OUT"
