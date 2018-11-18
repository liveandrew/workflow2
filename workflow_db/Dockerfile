FROM centos:centos7

# workflow db

RUN mkdir -p /apps

COPY databases/workflow_db /apps/workflow_db

RUN mkdir -p /apps/workflow_db/config/

RUN yum -y install ruby ruby-devel make gcc mysql-devel gcc-c++ mysql && \
  gem install bundler && \
  cd /apps/workflow_db && bundle install

COPY container/entrypoint.sh /apps/

ENTRYPOINT /apps/entrypoint.sh
