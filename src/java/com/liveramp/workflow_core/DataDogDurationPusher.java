package com.liveramp.workflow_core;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
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
  private final List<String> customTags;

  public DataDogDurationPusher(StatsDClient client) {
    this(client, Lists.newArrayList());
  }

  //  remember, tags cost money.  custom tags should not be UUIDs or large cardinality
  public DataDogDurationPusher(StatsDClient client, List<String> customTags) {
    this.client = client;
    this.customTags = customTags;
  }

  public static SuccessCallback production(){
    return new DataDogDurationPusher(DogClient.getProduction());
  }

  @Override
  public void onSuccess(WorkflowStatePersistence state) {
    try {

      ExecutionState executionState = state.getExecutionState();

      List<String> tags = Lists.newArrayList(customTags);
      tags.add("application:" + executionState.getAppName());

      long duration = executionState.getEndTime() - executionState.getStartTime();
      client.gauge(
          "workflow.application.duration",
          duration,
          tags.toArray(new String[0])
      );

    } catch (IOException e) {
      LOG.error("Error pushing stats to datadog: " + e);
    }
  }
}
