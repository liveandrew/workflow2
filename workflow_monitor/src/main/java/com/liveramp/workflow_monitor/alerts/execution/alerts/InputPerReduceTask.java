package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.util.Properties;

import com.google.common.collect.Multimap;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.java_support.ByteUnit;
import com.liveramp.workflow_monitor.alerts.execution.JobThresholdAlert;
import com.liveramp.workflow_monitor.alerts.execution.thresholds.GreaterThan;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

import static com.liveramp.workflow_core.WorkflowConstants.WORKFLOW_ALERT_RECOMMENDATIONS;

public class InputPerReduceTask extends JobThresholdAlert {

  private static final String PROPERTIES_PREFIX = "alert." + InputPerReduceTask.class.getSimpleName();

  private static final String BYTES_PER_REDUCE_LIMIT_PROP = PROPERTIES_PREFIX+".bytes_per_reduce_limit";
  private static final String BYTES_PER_REDUCE_LIMIT_DEFAULT = Long.toString(ByteUnit.GIBIBYTES.toBytes(20));

  protected static final Multimap<String, String> REQUIRED_COUNTERS = new MultimapBuilder<String, String>()
      .put(TASK_COUNTER_GROUP, MAP_OUTPUT_MATERIALIZED_BYTES)
      .put(JOB_COUNTER_GROUP, LAUNCHED_REDUCES)
      .get();

  public static InputPerReduceTask create(Properties properties){
    return new InputPerReduceTask(
        Long.parseLong(properties.getProperty(BYTES_PER_REDUCE_LIMIT_PROP, BYTES_PER_REDUCE_LIMIT_DEFAULT))
    );
  }

  private InputPerReduceTask(long bytesPerReduce) {
    super(bytesPerReduce, WorkflowRunnerNotification.PERFORMANCE, REQUIRED_COUNTERS, new GreaterThan());
  }

  @Override
  protected Double calculateStatistic(MapreduceJob job, TwoNestedMap<String, String, Long> counters) {

    Long mapOutputBytes = counters.get(TASK_COUNTER_GROUP, MAP_OUTPUT_MATERIALIZED_BYTES);
    Long launchedReduces = counters.get(JOB_COUNTER_GROUP, LAUNCHED_REDUCES);

    if(launchedReduces != null && mapOutputBytes != null){
      return mapOutputBytes.doubleValue() / launchedReduces.doubleValue();
    }

    return null;
  }

  @Override
  protected String getMessage(double value, MapreduceJob job, TwoNestedMap<String, String, Long> counters) {

    return "Reduce tasks in this job are reading on average " +
        df.format(value / ((double)ByteUnit.GIGABYTES.toBytes(1))) + "GB post-serialization. " +
        WORKFLOW_ALERT_RECOMMENDATIONS.get("InputPerReduceTask");

  }
}
