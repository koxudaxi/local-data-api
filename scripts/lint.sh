#!/usr/bin/env bash
set -e

black local_data_api tests --check --skip-string-normalization
isort --recursive --check-only -w 88  --combine-as --thirdparty local_data_api local_data_api tests -m 3 -tc
mypy local_data_api --disallow-untyped-defs --ignore-missing-imports