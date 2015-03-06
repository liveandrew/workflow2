package com.rapleaf.cascading_ext.workflow2.stats;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.timgroup.statsd.StatsDClient;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import cascading.flow.SliceCounters;
import cascading.flow.StepCounters;

import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.support.Rap;

public class StatsDRecorder implements StepStatsRecorder {

  private StatsDClient client;
  private static final Logger LOG = LoggerFactory.getLogger(StatsDRecorder.class);

  private static final int TIME_MS_SCALE = 100 * 1000;
  private static final int BYTES_SCALE = 1000 * 1000;
  private static final int RECORDS_SCALE = 100 * 1000;
  private static final Map<String, Integer> COUNTER_SCALES = ImmutableMap.<String, Integer>builder()
      .put("COMMITTED_HEAP_BYTES", BYTES_SCALE)
      .put("PHYSICAL_MEMORY_BYTES", BYTES_SCALE)
      .put("MAP_OUTPUT_BYTES", BYTES_SCALE)
      .put("VIRTUAL_MEMORY_BYTES", BYTES_SCALE)

      .put("MAP_INPUT_RECORDS", RECORDS_SCALE)
      .put("MAP_OUTPUT_RECORDS", RECORDS_SCALE)
      .put("REDUCE_INPUT_RECORDS", RECORDS_SCALE)
      .put("REDUCE_OUTPUT_RECORDS", RECORDS_SCALE)

      .put("Tuples_Read", RECORDS_SCALE)
      .put("Tuples_Written", RECORDS_SCALE)

      .put("CPU_MILLISECONDS", TIME_MS_SCALE)
      .put("SLOTS_MILLIS_MAPS", TIME_MS_SCALE)
      .put("SLOTS_MILLIS_REDUCES", TIME_MS_SCALE)
      .put("FALLOW_SLOTS_MILLIS_MAPS", TIME_MS_SCALE)
      .put("FALLOW_SLOTS_MILLIS_REDUCES", TIME_MS_SCALE)
      .build();

  public StatsDRecorder(StatsDClient client) {
    this.client = client;
  }

  @Override
  public void stop() {
    client.stop();
  }

  @Override
  public void recordStats(Step step, Step.StepTimer timer) {
    try {
      String bucket = step.getCheckpointToken().replaceAll("__", "."); //Change MSA checkpoint notation to form buckets with substeps of MSAs
      client.count(bucket + ".countRuns", 1);

      long milliTime = timer.getEventEndTime() - timer.getEventStartTime();
      int secondTime = Rap.safeLongToInt(TimeUnit.MILLISECONDS.toSeconds(milliTime));
      client.recordExecutionTime(bucket + ".realTime", secondTime);

      List<Class<? extends Enum<?>>> counterGroups = Lists.newArrayList(JobCounter.class, SliceCounters.class, StepCounters.class, TaskCounter.class);

      for (Class<? extends Enum<?>> counterGroup : counterGroups) {
        String groupName = counterGroup.getName();
        for (Enum<?> enumValue : counterGroup.getEnumConstants()) {
          String counterName = enumValue.name();
          record(step, bucket, groupName, counterName);
        }
      }
    } catch (Exception e) {
      LOG.error("There was an error while recording stats:\n" + e);
    }
  }

  private void record(Step step, String bucket, String groupName, String counterName) {

    int valToCommit = 0;
    for (NestedCounter nestedCounter : step.getCounters()) {
      if (groupName.equals(nestedCounter.getCounter().getGroup()) &&
          counterName.equals(nestedCounter.getCounter().getName())) {
        valToCommit += value(nestedCounter, counterName);
      }
    }

    if(valToCommit != 0) {
      client.count(bucket + "." + counterName, valToCommit);
      LOG.info("Recording " + groupName + ":" + counterName + "  :  " + valToCommit);
    }

  }

  private int value(NestedCounter nestedCounter, String counterName) {
    final Integer objScale = COUNTER_SCALES.get(counterName);
    final int scale = objScale == null ? 1 : objScale;
    final long value = nestedCounter.getCounter().getValue();
    return new Long(value / (long) scale).intValue();
  }
}