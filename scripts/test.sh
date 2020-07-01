#!/usr/bin/env bash
set -ex

pytest --cov=local_data_api --cov-report term-missing tests
