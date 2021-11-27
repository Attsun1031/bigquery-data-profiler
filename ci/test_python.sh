#!/bin/bash
set -e

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

pytest -v "${PROJECT_ROOT}"/tests
