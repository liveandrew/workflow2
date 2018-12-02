#!/usr/bin/env bash

# Workflow db migration container
docker build -t db_migrations:latest -f Dockerfile.migration .
docker tag db_migrations gcr.io/liveramp-eng/workflow2/db_migrations
docker push gcr.io/liveramp-eng/workflow2/db_migrations
