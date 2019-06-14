#!/usr/bin/env bash

set -e

source ${BASH_SOURCE%/*}/shared.sh

docker build -t db_migrations:latest -f container/migration/Dockerfile .
docker tag db_migrations:latest ${MIGRATION_REPO}

docker build -t sqldump:latest -f container/sqldump/Dockerfile .
docker tag sqldump:latest ${SQLDUMP_REPO}
