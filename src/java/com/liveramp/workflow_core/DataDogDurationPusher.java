package com.liveramp.workflow_core;

import java.io.IOException;

import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.datadog_client.statsd.DogClient;
import com.liveramp.workflow_state.ExecutionState;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.rollback.SuccessCallback;

public class DataDogDurationPusher implements SuccessCallback {
  private static final Logger LOG = LoggerFactory.getLogger(DataDogDurationPusher.class);

  private final StatsDClient client;

  public DataDogDurationPusher(StatsDClient client) {
    this.client = client;
  }

  public static SuccessCallback production(){
    return new DataDogDurationPusher(DogClient.getProduction());
  }

  @Override
  public void onSuccess(WorkflowStatePersistence state) {
    try {

      ExecutionState executionState = state.getExecutionState();

      long duration = executionState.getEndTime() - executionState.getStartTime();
      client.gauge(
          "workflow.application.duration",
          duration,
          "application:" + executionState.getAppName()
      );

    } catch (IOException e) {
      LOG.error("Error pushing stats to datadog: " + e);
    }
  }
}
