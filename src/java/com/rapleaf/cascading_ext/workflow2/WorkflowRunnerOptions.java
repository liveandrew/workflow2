package com.rapleaf.cascading_ext.workflow2;

import org.apache.log4j.Logger;

import com.liveramp.java_support.alerts_handler.LoggingAlertsHandler;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.registry.MockRegistry;
import com.rapleaf.cascading_ext.workflow2.registry.ZkRegistry;
import com.rapleaf.cascading_ext.workflow2.stats.RecorderFactory;
import com.rapleaf.support.Rap;

//  TODO this should get renamed ProductionWorkflowOptions at some point.  Goal is that
//  this is instantiated in only production
public class WorkflowRunnerOptions extends WorkflowOptions<WorkflowRunnerOptions> {
  private static final Logger LOG = Logger.getLogger(WorkflowRunnerOptions.class);

  public WorkflowRunnerOptions() {

    //  TODO get rid of this block once we don't use this classs in test (few OL things still do
    if(Rap.getTestMode()){
      LOG.warn("Should not use WorkflowRunnerOptions in test!  Use TestWorkflowOptions");

      setMaxConcurrentSteps(1);
      setAlertsHandler(new LoggingAlertsHandler());
      setEnabledNotifications(WorkflowRunnerNotificationSet.all());
      setStatsRecorder(new RecorderFactory.Mock());
      setLockProvider(null);
      setStorage(new ContextStorage.None());
      setRegistry(new MockRegistry());
    }

    else {
      setMaxConcurrentSteps(Integer.MAX_VALUE);
      setAlertsHandler(new LoggingAlertsHandler());
      setEnabledNotifications(WorkflowRunnerNotificationSet.all());
      setStatsRecorder(new RecorderFactory.StatsD());
      setLockProvider(null);
      setStorage(new ContextStorage.None());
      setRegistry(new ZkRegistry());
    }
  }

}
