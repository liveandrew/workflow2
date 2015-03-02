package com.rapleaf.cascading_ext.workflow2.options;

import com.liveramp.cascading_ext.util.NestedProperties;
import com.liveramp.java_support.alerts_handler.LoggingAlertsHandler;
import com.rapleaf.cascading_ext.test.CommonTestUtil;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnerNotificationSet;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilters;
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
    NestedProperties defaultProperties = new NestedProperties(null, CommonTestUtil.getFinalWorkflowJobPropertiesForTests());
    setWorkflowDefaultProperties(new NestedProperties(
        defaultProperties, CommonTestUtil.getOverridableWorkflowJobPropertiesForTests())
    );
    setCounterFilter(CounterFilters.defaultCounters()); //  TODO we can probably switch this to all user counters by default
                                                      //  after making sure it's not going to nuke the DB
  }

}
