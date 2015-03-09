package com.rapleaf.cascading_ext.workflow2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.alerts_handler.LoggingAlertsHandler;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilters;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.support.Rap;

public class ProductionWorkflowOptions extends WorkflowOptions<ProductionWorkflowOptions> {
  private static final Logger LOG = LoggerFactory.getLogger(ProductionWorkflowOptions.class);

  public ProductionWorkflowOptions() {

    Rap.assertProduction();

    setMaxConcurrentSteps(Integer.MAX_VALUE);
    setAlertsHandler(new LoggingAlertsHandler());
    setEnabledNotifications(WorkflowRunnerNotificationSet.all());
    setLockProvider(null);
    setStorage(new ContextStorage.None());
    setStepPollInterval(3000);  // be nice to production DB
    setCounterFilter(CounterFilters.defaultCounters()); //  TODO we can probably switch this to all user counters by default
                                                      //  after making sure it's not going to nuke the DB

  }

}
