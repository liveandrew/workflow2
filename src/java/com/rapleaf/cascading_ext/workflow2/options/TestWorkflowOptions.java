package com.rapleaf.cascading_ext.workflow2.options;

import com.liveramp.java_support.alerts_handler.LoggingAlertsHandler;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnerNotificationSet;
import com.rapleaf.cascading_ext.workflow2.registry.MockRegistry;
import com.rapleaf.cascading_ext.workflow2.stats.RecorderFactory;
import com.rapleaf.support.Rap;

public class TestWorkflowOptions extends WorkflowOptions<TestWorkflowOptions> {

  public TestWorkflowOptions() {
    Rap.assertTest();

    setMaxConcurrentSteps(1);
    setAlertsHandler(new LoggingAlertsHandler());
    setEnabledNotifications(WorkflowRunnerNotificationSet.all());
    setStatsRecorder(new RecorderFactory.Mock());
    setLockProvider(null);
    setStorage(new ContextStorage.None());
    setRegistry(new MockRegistry());
    setStepPollInterval(100);
  }

}
