#!/usr/bin/env bash

# set up the rails db

cd /apps/workflow_db/

sed -e "s/\${db_username}/$DB_USERNAME/" \
  -e "s/\${db_password}/$DB_PASSWORD/" \
  -e "s/\${db_address}/$DB_HOSTNAME/" \
  -e "s/\${db_address}/$DB_PORT/" \
  /apps/workflow_db/config/database.yml.tpl \
  > /apps/workflow_db/config/database.yml

export RAILS_ENV=docker_env

bundle exec rake db:create
bundle exec rake rapleaf:migrate
