#!/usr/bin/env bash

docker build -t db_migrations:latest -f container/migration/Dockerfile .
docker tag db_migrations gcr.io/liveramp-eng/workflow2/db_migrations
docker push gcr.io/liveramp-eng/workflow2/db_migrations

docker build -t sqldump:latest -f container/sqldump/Dockerfile .
docker tag sqldump gcr.io/liveramp-eng/workflow2/sqldump
docker push gcr.io/liveramp-eng/workflow2/sqldump
