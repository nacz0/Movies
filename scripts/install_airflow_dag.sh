#!/usr/bin/env bash
# Run from WSL. Link just this DAG; leave all other DAGs untouched.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
dag_dir="${1:-/home/nacho/airflow/dags}"
source_dag="$repo_root/dags/movies_steps.py"
target_dag="$dag_dir/movies_steps.py"
mkdir -p -- "$dag_dir"
if [[ -e "$target_dag" || -L "$target_dag" ]]; then
    if [[ -L "$target_dag" && "$target_dag" -ef "$source_dag" ]]; then
        echo "Already installed: $target_dag"
        exit 0
    fi
    echo "Refusing to replace existing file: $target_dag" >&2
    exit 1
fi
ln -s -- "$source_dag" "$target_dag"
echo "Installed: $target_dag -> $source_dag"
