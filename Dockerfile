FROM centos:centos7

# workflow db

RUN mkdir -p /apps

COPY workflow_db/databases/workflow_db /apps/workflow_db

RUN mkdir -p /apps/workflow_db/config/

RUN ln -sf /apps/secrets/workflow_db/database.yml /apps/workflow_db/config/database.yml

RUN yum -y install ruby ruby-devel make gcc mysql-devel gcc-c++ && \
  gem install bundler && \
  cd /apps/workflow_db && bundle install


# workflow ui

RUN yum -y install java-1.8.0-openjdk

RUN mkdir -p /apps/workflow_ui/

COPY workflow_ui/target/workflow_ui.job.jar /apps/workflow_ui/

RUN mkdir -p /apps/workflow_ui/config/

RUN ln -sf /apps/secrets/workflow_ui/database.yml /apps/workflow_ui/config/database.yml && \
    ln -sf /apps/secrets/workflow_ui/environment.yml /apps/workflow_ui/config/environment.yml

COPY container/entrypoint.sh /apps/

ENTRYPOINT /apps/entrypoint.sh