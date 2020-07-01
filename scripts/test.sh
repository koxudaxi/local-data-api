#!/usr/bin/env bash
set -e

pytest --cov=local_data_api --cov-report term-missing tests
