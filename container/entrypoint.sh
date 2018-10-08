#!/usr/bin/env bash

# TODO db migrate

java -Djava.io.tmpdir=/var/www/tmp -Xmx12000m -Djava.net.preferIPv4Stack=true -cp workflow_ui.job.jar \
  com.liveramp.workflow_ui.WorkflowDbWebServer

