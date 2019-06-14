#!/usr/bin/env bash

set -e

source ${BASH_SOURCE%/*}/shared.sh

docker build -t workflow2_db_migrations:latest -f container/migration/Dockerfile .
docker tag workflow2_db_migrations:latest ${MIGRATION_REPO}

docker build -t workflow2_sqldump:latest -f container/sqldump/Dockerfile .
docker tag workflow2_sqldump:latest ${SQLDUMP_REPO}
