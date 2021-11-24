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

pylint "${PROJECT_ROOT}"/bqdataprofiler
mypy "${PROJECT_ROOT}"/bqdataprofiler
