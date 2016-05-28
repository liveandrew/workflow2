package com.rapleaf.cascading_ext.workflow2.options;

import java.util.Collections;

import org.apache.hadoop.mapreduce.MRJobConfig;

import com.liveramp.cascading_ext.megadesk.MockStoreReaderLockProvider;
import com.liveramp.cascading_ext.resource.ResourceManagers;
import com.liveramp.java_support.alerts_handler.LoggingAlertsHandler;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.TrackerURLBuilder;
import com.rapleaf.cascading_ext.workflow2.WorkflowNotificationLevel;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilters;
import com.rapleaf.support.Rap;

public class TestWorkflowOptions extends WorkflowOptions {

  public TestWorkflowOptions() {
    Rap.assertTest();

    setMaxConcurrentSteps(1);
    setAlertsHandler(new LoggingAlertsHandler());
    setNotificationLevel(WorkflowNotificationLevel.DEBUG);
    setLockProvider(new MockStoreReaderLockProvider());
    setStorage(new ContextStorage.None());
    setStepPollInterval(100);
    setCounterFilter(CounterFilters.all());
    setUrlBuilder(new TrackerURLBuilder.None());
    setHostnameProvider(new FixedHostnameProvider());
    setResourceManager(ResourceManagers.notImplemented());

    addWorkflowProperties(Collections.<Object, Object>singletonMap(MRJobConfig.QUEUE_NAME, "test"));
  }

}
