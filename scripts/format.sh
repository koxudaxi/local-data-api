#!/usr/bin/env bash
set -e

black local_data_api tests
isort --recursive local_data_api tests
