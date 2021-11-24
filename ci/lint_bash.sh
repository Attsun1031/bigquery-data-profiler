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
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

shellcheck "${PROJECT_DIR}"/ci/*.sh
