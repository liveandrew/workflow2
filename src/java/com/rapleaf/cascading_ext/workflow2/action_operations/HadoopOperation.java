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
import com.liveramp.cascading_tools.jobs.ActionOperation;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.rapleaf.cascading_ext.RunnableJob;
import com.rapleaf.cascading_ext.counters.NestedCounter;

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

  @Override
  public void timeOperation(String checkpointToken, List<NestedCounter> nestedCounters) {
    Counters counterGroups;

    try {
      counterGroups = runningJob.getCounters();
    } catch (NullPointerException e) {
      counterGroups = new Counters();
    } catch (IOException e) {
      counterGroups = new Counters();
    }

    for (Counters.Group counterGroup : counterGroups) {
      final String groupName = counterGroup.getName();

      for (Counters.Counter c : counterGroup) {
        if (c.getValue() > 0) {
          Counter singleValueCounter = new Counter(groupName, c.getName(), c.getValue());
          nestedCounters.add(new NestedCounter(singleValueCounter));
        }
      }
    }

  }

  private void markStarted() {
    startTime = System.currentTimeMillis();
  }

  private void markFinished() {
    finishTime = System.currentTimeMillis();
  }
}
