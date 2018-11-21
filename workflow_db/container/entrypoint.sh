#!/usr/bin/env bash

# set up the database files from environment vars
sed -e "s/\${DB_USERNAME}/$DB_USERNAME/" \
  -e "s/\${DB_PASSWORD}/$DB_PASSWORD/" \
  -e "s/\${DB_HOSTNAME}/$DB_HOSTNAME/" \
  -e "s/\${DB_PORT}/$DB_PORT/" \
  /apps/workflow_db/config/database.yml.tpl \
  > /apps/workflow_db/config/database.yml

function mysql_conn_error {
  mysql -h $DB_HOSTNAME -P $DB_PORT -u $DB_USERNAME --password=$DB_PASSWORD -e "show databases"
  return $?
}

# wait until mysql is running
while true
do

  mysql_conn_error
  RES=$?
  echo "Got result: $RES"
  if [ "$RES" == "0" ]; then
    echo "MySQL started"
    break
  fi

  echo "Waiting for MySQL to start"
  sleep 1

done

# set up the rails db
export RAILS_ENV=docker_env
cd /apps/workflow_db/
bundle exec rake db:create
bundle exec rake db:migrate
