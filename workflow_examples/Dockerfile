FROM centos:centos7

RUN yum -y install java-1.8.0-openjdk

RUN mkdir -p /apps/workflow_examples/

COPY target/workflow_examples.job.jar /apps/workflow_examples/

RUN mkdir -p /apps/workflow_examples/config/

RUN ln -sf /apps/secrets/workflow_examples/database.yml /apps/workflow_examples/config/database.yml && \
    ln -sf /apps/secrets/workflow_examples/environment.yml /apps/workflow_examples/config/environment.yml
