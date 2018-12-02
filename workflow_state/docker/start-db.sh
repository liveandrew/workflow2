#!/usr/bin/env bash

set -euo pipefail

DIR="${0%/*}"
export COMPOSE_PROJECT_NAME=workflow_state

docker-compose -f "$DIR/test-db-compose.yml" pull
docker-compose -f "$DIR/test-db-compose.yml" up -d

docker pull gcr.io/liveramp-eng/workflow2_db:latest
docker run --rm --name test_migrate --network=workflow_state_shared -e "DB_PORT=3306" -e "DB_USERNAME=root" -e "DB_HOSTNAME=mysql" gcr.io/liveramp-eng/workflow2_db:latest