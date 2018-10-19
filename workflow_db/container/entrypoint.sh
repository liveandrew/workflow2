#!/usr/bin/env bash

# set up the rails db

cd /apps/workflow_db/

export RAILS_ENV=production

bundle exec rake db:create
bundle exec rake rapleaf:migrate
