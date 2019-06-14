#!/usr/bin/env bash

set -e

source ${BASH_SOURCE%/*}/shared.sh

docker push ${MIGRATION_REPO}
docker push ${SQLDUMP_REPO}
