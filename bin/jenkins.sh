#!/usr/bin/env bash

set -euo pipefail

docker pull centos:centos7

sweep_images() {
  REPO=$1
  gcloud container images list-tags ${REPO} --limit=999999 --sort-by=TIMESTAMP --filter "NOT tags:* " --format='get(digest)' \
   | tac | tail -n +5 | awk -F: '{print $2}' \
   | xargs -I{} -n 1 gcloud container images delete ${REPO}@sha256:{} --quiet
}

# Workflow UI container
WORKFLOW_UI_REPO="gcr.io/liveramp-eng/workflow2_ui"
pushd workflow_ui
docker build -t workflow2_ui:latest -f Dockerfile .
docker tag workflow2_ui ${WORKFLOW_UI_REPO}
docker push ${WORKFLOW_UI_REPO}
popd
sweep_images ${WORKFLOW_UI_REPO}

# Workflow monitor container
WORKFLOW_MONITOR_REPO="gcr.io/liveramp-eng/workflow2_monitor"
pushd workflow_monitor
docker build -t workflow2_monitor:latest -f Dockerfile .
docker tag workflow2_monitor ${WORKFLOW_MONITOR_REPO}
docker push ${WORKFLOW_MONITOR_REPO}
popd
sweep_images ${WORKFLOW_MONITOR_REPO}