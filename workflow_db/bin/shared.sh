#!/usr/bin/env bash

docker_login() {
    local restore_x
    if [[ $- =~ x ]]; then
      restore_x=true
    else
      restore_x=false
    fi

    # Disable xtrace to hide password ($DOCKER_PASS).
    set +x
    echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
    if [[ ${restore_x} ]]; then
        set -x
    fi
}

docker_login

export MIGRATION_REPO=liveramp/workflow2_db_migrations:${VERSION}
export SQLDUMP_REPO=liveramp/workflow2_sqldump:${VERSION}
