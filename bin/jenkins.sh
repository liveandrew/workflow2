#!/usr/bin/env bash

set -euo pipefail

mkdir -p .docker || echo ".docker directory exists"
export DOCKER_CONFIG=`pwd`/.docker
docker login -u _json_key --password-stdin https://gcr.io < "${KEYFILE}"

docker pull centos:centos7

# Workflow db migration container
pushd workflow_db
docker build -t workflow2_db:latest -f Dockerfile .
docker tag workflow2_db gcr.io/liveramp-eng-bdi/workflow2_db
docker push gcr.io/liveramp-eng-bdi/workflow2_db
popd

# Workflow UI container
pushd workflow_ui
docker build -t workflow2_ui:latest -f Dockerfile .
docker tag workflow2_ui gcr.io/liveramp-eng-bdi/workflow2_ui
docker push gcr.io/liveramp-eng-bdi/workflow2_ui
popd

# Workflow monitor container
pushd workflow_monitor
docker build -t workflow2_monitor:latest -f Dockerfile .
docker tag workflow2_monitor gcr.io/liveramp-eng-bdi/workflow2_monitor
docker push gcr.io/liveramp-eng-bdi/workflow2_monitor
popd
