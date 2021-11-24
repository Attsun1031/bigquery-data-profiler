#!/bin/bash
set -e

machine=$(uname)
if [ "${machine}" = "Darwin" ]
then
  READLINK=greadlink
else
  READLINK=readlink
fi

SCRIPT_DIR="$(dirname "$($READLINK -f "$0")")"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

yapf --recursive --parallel --in-place "${PROJECT_ROOT}"/bqdataprofiler
yapf --recursive --parallel --in-place "${PROJECT_ROOT}"/ci
isort "${PROJECT_ROOT}"/bqdataprofiler
