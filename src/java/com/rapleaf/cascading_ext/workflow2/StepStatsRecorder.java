package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.SliceCounters;
import cascading.flow.StepCounters;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.support.Rap;
import com.timgroup.statsd.StatsDClient;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: pwestling
 * Date: 11/5/13
 * Time: 3:41 PM
 * To change this template use File | Settings | File Templates.
 */
public interface StepStatsRecorder {

  public void recordStats(Step step, Step.StepTimer timer);

  public void stop();
}

class StatsDRecorder implements StepStatsRecorder {

  private StatsDClient client;
  private static final Logger LOG = Logger.getLogger(StatsDRecorder.class);

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
    for (NestedCounter nestedCounter : step.getCounters()) {
      if (groupName.equals(nestedCounter.getCounter().getGroup()) &&
          counterName.equals(nestedCounter.getCounter().getName())) {
        int value = value(nestedCounter, counterName);
        client.count(bucket + "." + counterName, value);
        LOG.info("Recording " + groupName + ":" + counterName + "  :  " + value);
        return;
      }
    }
  }

  private int value(NestedCounter nestedCounter, String counterName) {
    final Integer objScale = COUNTER_SCALES.get(counterName);
    final int scale = objScale == null ? 1 : objScale;
    final long value = nestedCounter.getCounter().getValue();
    return new Long(value / (long) scale).intValue();
  }
}

class MockStatsRecorder implements StepStatsRecorder {
  private static final Logger LOG = Logger.getLogger(MockStatsRecorder.class);

  @Override
  public void recordStats(Step step, Step.StepTimer timer) {
    for (NestedCounter nestedCounter : step.getCounters()) {
      LOG.info(nestedCounter.getCounter().getGroup() + " : " + nestedCounter.getCounter().getName() + " : " + nestedCounter.getCounter().getValue());
    }
  }

  @Override
  public void stop() {
    //  empty
  }
}
