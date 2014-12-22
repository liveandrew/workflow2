package com.rapleaf.cascading_ext.workflow2.stats;

import org.apache.log4j.Logger;

import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.workflow2.Step;

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