package com.rapleaf.cascading_ext.workflow2.options;

import com.liveramp.java_support.alerts_handler.LoggingAlertsHandler;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnerNotificationSet;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilters;
import com.rapleaf.support.Rap;

public class TestWorkflowOptions extends WorkflowOptions {

  public TestWorkflowOptions() {
    Rap.assertTest();

    setMaxConcurrentSteps(1);
    setAlertsHandler(new LoggingAlertsHandler());
    setEnabledNotifications(WorkflowRunnerNotificationSet.all());
    setLockProvider(null);
    setStorage(new ContextStorage.None());
    setStepPollInterval(100);
    setCounterFilter(CounterFilters.defaultCounters()); //  TODO we can probably switch this to all user counters by default
    //  after making sure it's not going to nuke the DB

  }

}
