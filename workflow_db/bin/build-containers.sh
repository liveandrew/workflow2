#!/usr/bin/env bash

set -e

source ${BASH_SOURCE%/*}/shared.sh

docker build -t db_migrations:latest -f container/migration/Dockerfile .
docker tag db_migrations ${MIGRATION_REPO}

docker build -t sqldump:latest -f container/sqldump/Dockerfile .
docker tag sqldump ${SQLDUMP_REPO}
