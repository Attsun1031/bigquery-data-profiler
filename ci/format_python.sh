#!/bin/bash
set -e

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

yapf --recursive --parallel --in-place "${PROJECT_ROOT}"/bqdataprofiler
isort "${PROJECT_ROOT}"/bqdataprofiler
