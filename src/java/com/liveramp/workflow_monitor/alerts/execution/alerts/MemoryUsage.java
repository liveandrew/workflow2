package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.MapreduceJobAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;

public class MemoryUsage extends MapreduceJobAlertGenerator {

  private static final String SLICE_GROUP = "cascading.flow.SliceCounters";
  private static final String TASK_GROUP = "org.apache.hadoop.mapreduce.TaskCounter";
  private static final String JOB_GROUP = "org.apache.hadoop.mapreduce.JobCounter";

  private static final String END_TIME = "Process_End_Time";
  private static final String START_TIME = "Process_Begin_Time";

  private static final String MEM_BYTES = "PHYSICAL_MEMORY_BYTES";
  private static final String LAUNCHED_MAPS = "TOTAL_LAUNCHED_MAPS";
  private static final String LAUNCHED_REDUCES = "TOTAL_LAUNCHED_REDUCES";

  private static final String MB_MAPS = "MB_MILLIS_MAPS";
  private static final String MB_REDUCES = "MB_MILLIS_REDUCES";

  public MemoryUsage() {
    super(new MultimapBuilder<String, String>()
        .put(SLICE_GROUP, END_TIME)
        .put(SLICE_GROUP, START_TIME)
        .put(TASK_GROUP, MEM_BYTES)
        .put(JOB_GROUP, LAUNCHED_MAPS)
        .put(JOB_GROUP, LAUNCHED_REDUCES)
        .put(JOB_GROUP, MB_MAPS)
        .put(JOB_GROUP, MB_REDUCES)
        .get());
  }

  @Override
  public List<AlertMessage> generateAlerts(MapreduceJob job, TwoNestedMap<String, String, Long> counters) throws IOException {


    Long end = counters.get(SLICE_GROUP, END_TIME);
    Long begin = counters.get(SLICE_GROUP, START_TIME);

    Long wallTimeSum = end - begin;

    Long memoryBytes = counters.get(TASK_GROUP, MEM_BYTES);


    Long launchedMaps = counters.get(JOB_GROUP, LAUNCHED_MAPS);
    Long launchedReduces = counters.get(JOB_GROUP, LAUNCHED_REDUCES);

    Long totalTasks = launchedMaps + launchedReduces;

    Double averageTaskMemory = memoryBytes.doubleValue() / totalTasks.doubleValue();

    Long occupiedMemory = (long)(wallTimeSum * averageTaskMemory);

    Long mapAllocatedMem = counters.get(JOB_GROUP, MB_MAPS);
    Long reduceAllocatedMem = counters.get(JOB_GROUP, MB_REDUCES);

    Long allocatedMem = mapAllocatedMem + reduceAllocatedMem;

    System.out.println();
    System.out.println(job.getJobIdentifier() + " " + job.getJobName());
    System.out.println(occupiedMemory.doubleValue() / allocatedMem.doubleValue());

    return Lists.newArrayList();
  }
}

