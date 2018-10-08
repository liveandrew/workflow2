#!/usr/bin/env bash

set -e

cd /apps/workflow_db/

bundle exec rake rapleaf:migrate

cd /apps/workflow_ui/

java -Djava.io.tmpdir=/var/www/tmp -Xmx12000m -Djava.net.preferIPv4Stack=true -cp workflow_ui.job.jar \
  com.liveramp.workflow_ui.WorkflowDbWebServer
