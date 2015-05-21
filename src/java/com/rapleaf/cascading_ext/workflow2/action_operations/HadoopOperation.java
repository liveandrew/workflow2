package com.rapleaf.cascading_ext.workflow2.action_operations;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.counters.Counter;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.java_support.event_timer.FixedTimedEvent;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.workflow2.ActionOperation;
import com.rapleaf.cascading_ext.RunnableJob;
import com.rapleaf.cascading_ext.workflow2.Step;

public class HadoopOperation implements ActionOperation {
  private static Logger LOG = LoggerFactory.getLogger(HadoopOperation.class);

  private final RunnableJob runnableJob;

  private JobConf conf = null;
  private RunningJob runningJob = null;
  private Long startTime = null;
  private Long finishTime = null;

  public HadoopOperation(RunnableJob job) {
    this.runnableJob = job;
  }

  @Override
  public void complete() {
    try {

      markStarted();
      conf = runnableJob.configure();
      JobClient jobClient = new JobClient(conf);

      runningJob = jobClient.submitJob(conf);
      runningJob.waitForCompletion();

      LOG.info(com.liveramp.cascading_ext.counters.Counters.prettyCountersString(runningJob));
      if (!runningJob.isSuccessful()) {
        throw new RuntimeException("Job " + getName() + " failed!: " + runningJob.getFailureInfo());
      }
      runnableJob.complete(runningJob);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      markFinished();
    }
  }

  @Override
  public String getName() {
    return conf.getJobName();
  }

  @Override
  public List<RunningJob> listJobs() {

    List<RunningJob> jobs = Lists.newArrayList();
    if(runningJob != null){
      jobs.add(runningJob);
    }

    return jobs;
  }

  @Override
  public ThreeNestedMap<String, String, String, Long> getJobCounters() {
    ThreeNestedMap<String, String, String, Long> toRet = new ThreeNestedMap<String, String, String, Long>();
    toRet.put(runningJob.getID().toString(), com.liveramp.cascading_ext.counters.Counters.getCounterMap(runningJob));
    return toRet;
  }

  private void markStarted() {
    startTime = System.currentTimeMillis();
  }

  private void markFinished() {
    finishTime = System.currentTimeMillis();
  }
}
