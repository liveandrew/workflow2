#!/usr/bin/env bash

set -e

cd /apps/workflow_db/

export RAILS_ENV=production

bundle exec rake db:create
bundle exec rake rapleaf:migrate

cd /apps/workflow_ui/

# not really a work of art, could be done as ruby or java
JETTY_ENV=$( cat config/environment.yml | grep workflow_ui_jetty_db | awk -F: '{print $2}' | xargs )
DB_NAME=$( cat config/database.yml | sed -n -e "/^$JETTY_ENV:/,\$p" | grep database | awk -F: '{print $2}' | head -1 | xargs )
USERNAME=$( cat config/database.yml | sed -n -e "/^$JETTY_ENV:/,\$p" | grep username | awk -F: '{print $2}' | head -1 | xargs )
PASSWORD=$( cat config/database.yml | sed -n -e "/^$JETTY_ENV:/,\$p" | grep password | awk -F: '{print $2}' | head -1 | xargs )
HOST=$( cat config/database.yml | sed -n -e "/^$JETTY_ENV:/,\$p" | grep host | awk -F: '{print $2}' | head -1 | xargs )

# set up jetty session db if it doesn't exist already
mysql -h $HOST -u$USERNAME -p$PASSWORD -e "create database if not exists $DB_NAME" || true

java -Djava.io.tmpdir=/var/www/tmp -Xmx12000m -Djava.net.preferIPv4Stack=true -cp workflow_ui.job.jar \
  com.liveramp.workflow_ui.WorkflowDbWebServer
