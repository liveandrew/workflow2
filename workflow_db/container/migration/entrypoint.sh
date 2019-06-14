#!/usr/bin/env bash

set -euo pipefail

DIR="${0%/*}"

# wait for mysql
source "$DIR/block-on-db.sh"

# set up the database files from environment vars
cat /apps/workflow_db/config/database.yml.tpl | envsubst > /apps/workflow_db/config/database.yml

# set up the rails db
export RAILS_ENV=workflow_docker_env
cd /apps/workflow_db/

echo "Creating database..."
bundle exec rake db:create

echo "Running migrations..."
bundle exec rake db:migrate

echo "Migrations complete"
