#!/usr/bin/env bash

set -e

source ${BASH_SOURCE%/*}/shared.sh

docker build -t workflow2_db_migrations:${VERSION} -f container/migration/Dockerfile .
docker tag workflow2_db_migrations:${VERSION} ${MIGRATION_REPO}

docker build -t workflow2_sqldump:${VERSION} -f container/sqldump/Dockerfile .
docker tag workflow2_sqldump:${VERSION} ${SQLDUMP_REPO}
