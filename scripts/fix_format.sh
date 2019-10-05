#!/usr/bin/env bash
set -e

black local_data_api tests --skip-string-normalization
isort --recursive -w 88  --combine-as --thirdparty local_data_api local_data_api tests -m 3 -tc
