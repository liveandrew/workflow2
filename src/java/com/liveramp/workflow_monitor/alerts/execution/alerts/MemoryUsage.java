package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.MapreduceJobAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;

public class MemoryUsage implements MapreduceJobAlertGenerator {

  @Override
  public List<AlertMessage> generateAlerts(MapreduceJob job, TwoNestedMap<String, String, Long> counters) throws IOException {


    Long end = counters.get("cascading.flow.SliceCounters", "Process_End_Time");
    Long begin = counters.get("cascading.flow.SliceCounters", "Process_Begin_Time");

    Long wallTimeSum = end - begin;

    Long memoryBytes = counters.get("org.apache.hadoop.mapreduce.TaskCounter", "PHYSICAL_MEMORY_BYTES");

    Long launchedMaps = counters.get("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_MAPS");
    Long launchedReduces = counters.get("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_REDUCES");

    Long totalTasks = launchedMaps + launchedReduces;

    Double averageTaskMemory = memoryBytes.doubleValue() / totalTasks.doubleValue();

    Long occupiedMemory =  (long) (wallTimeSum * averageTaskMemory);

    Long mapAllocatedMem = counters.get("org.apache.hadoop.mapreduce.JobCounter", "MB_MILLIS_MAPS");
    Long reduceAllocatedMem = counters.get("org.apache.hadoop.mapreduce.JobCounter", "MB_MILLIS_REDUCES");

    Long allocatedMem = mapAllocatedMem + reduceAllocatedMem;

    System.out.println();
    System.out.println(job.getJobIdentifier()+" "+job.getJobName());
    System.out.println(occupiedMemory.doubleValue() / allocatedMem.doubleValue());

  }
}

