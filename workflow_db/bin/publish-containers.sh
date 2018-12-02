#!/usr/bin/env bash

# Workflow db migration container
docker build -t workflow2_db:latest -f Dockerfile .
docker tag workflow2_db gcr.io/liveramp-eng/workflow2_db
docker push gcr.io/liveramp-eng/workflow2_db
