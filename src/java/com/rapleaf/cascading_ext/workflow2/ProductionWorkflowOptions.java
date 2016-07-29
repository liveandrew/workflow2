package com.rapleaf.cascading_ext.workflow2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.megadesk.MockStoreReaderLockProvider;
import com.liveramp.cascading_ext.resource.ResourceManagers;
import com.liveramp.java_support.alerts_handler.LoggingAlertsHandler;
import com.liveramp.workflow_core.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilters;
import com.rapleaf.cascading_ext.workflow2.options.DefaultHostnameProvider;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.support.Rap;

@Deprecated // use WorkflowOptions.production()
public class ProductionWorkflowOptions extends WorkflowOptions {
  private static final Logger LOG = LoggerFactory.getLogger(ProductionWorkflowOptions.class);

  public ProductionWorkflowOptions() {

    Rap.assertProduction();

    setMaxConcurrentSteps(Integer.MAX_VALUE);
    setAlertsHandler(new LoggingAlertsHandler());
    setNotificationLevel(WorkflowNotificationLevel.DEBUG);
    setLockProvider(new MockStoreReaderLockProvider());
    setStorage(new ContextStorage.None());
    setStepPollInterval(6000);  // be nice to production DB
    setCounterFilter(CounterFilters.all());
    setUrlBuilder(new DbTrackerURLBuilder(WORKFLOW_UI_URL));
    setHostnameProvider(new DefaultHostnameProvider());
    setResourceManager(ResourceManagers.notImplemented());

  }
}
