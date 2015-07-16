package com.rapleaf.cascading_ext.workflow2.options;

import com.liveramp.cascading_ext.megadesk.MockStoreReaderLockProvider;
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
    setLockProvider(new MockStoreReaderLockProvider());
    setStorage(new ContextStorage.None());
    setStepPollInterval(100);
    setCounterFilter(CounterFilters.all());

  }

}
