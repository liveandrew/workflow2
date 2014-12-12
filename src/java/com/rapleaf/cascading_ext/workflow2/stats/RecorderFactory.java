package com.rapleaf.cascading_ext.workflow2.stats;

import com.timgroup.statsd.NonBlockingStatsDClient;
import org.apache.log4j.Logger;

public interface RecorderFactory {

  public StepStatsRecorder makeRecorder(String workflowName);

  public static class Mock implements RecorderFactory {

    @Override
    public StepStatsRecorder makeRecorder(String workflowName) {
      return new MockStatsRecorder();
    }
  }

  public static class StatsD implements RecorderFactory {
    private static final Logger LOG = Logger.getLogger(RecorderFactory.class);

    public StepStatsRecorder makeRecorder(String workflowName) {
      try {
        return new StatsDRecorder(new NonBlockingStatsDClient("workflow." + workflowName, "pglibertyc6", 8125));
      } catch (Exception e) {
        //  whatever
        LOG.info("Exception creating stats recorder", e);
      }
      return new MockStatsRecorder();
    }
  }

}
