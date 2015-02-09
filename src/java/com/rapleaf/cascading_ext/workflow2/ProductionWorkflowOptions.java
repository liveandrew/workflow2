package com.rapleaf.cascading_ext.workflow2;

import org.apache.log4j.Logger;

import com.liveramp.java_support.alerts_handler.LoggingAlertsHandler;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.registry.ZkRegistry;
import com.rapleaf.cascading_ext.workflow2.stats.RecorderFactory;
import com.rapleaf.support.Rap;

public class ProductionWorkflowOptions extends WorkflowOptions<ProductionWorkflowOptions> {
  private static final Logger LOG = Logger.getLogger(ProductionWorkflowOptions.class);

  public ProductionWorkflowOptions() {

    Rap.assertProduction();

    setMaxConcurrentSteps(Integer.MAX_VALUE);
    setAlertsHandler(new LoggingAlertsHandler());
    setEnabledNotifications(WorkflowRunnerNotificationSet.all());
    setStatsRecorder(new RecorderFactory.StatsD());
    setLockProvider(null);
    setStorage(new ContextStorage.None());
    setRegistry(new ZkRegistry());
    setStepPollInterval(3000);  // be nice to production DB

  }

}
