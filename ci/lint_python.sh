#!/bin/bash
set -e

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

pylint "${PROJECT_ROOT}"/bqdataprofiler
mypy "${PROJECT_ROOT}"/bqdataprofiler
