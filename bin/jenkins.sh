#!/usr/bin/env bash

set -euo pipefail

mkdir -p .docker || echo ".docker directory exists"
export DOCKER_CONFIG=`pwd`/.docker
docker login -u _json_key --password-stdin https://gcr.io < "${KEYFILE}"

docker pull centos:centos7

docker build -t workflow2:latest .

docker tag workflow2 gcr.io/liveramp-eng-bdi/workflow2

docker push gcr.io/liveramp-eng-bdi/workflow2
