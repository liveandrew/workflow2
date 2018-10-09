package com.liveramp.workflow_monitor.alerts.execution;

import org.apache.log4j.Level;
import org.junit.Before;

import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.rapleaf.java_support.CommonJUnit4TestCase;

public abstract class WorkflowMonitorTestCase extends CommonJUnit4TestCase {

  public WorkflowMonitorTestCase() {
    super(Level.ALL);
  }

  @Before
  public void deleteFixtures() throws Exception {
    new DatabasesImpl().getWorkflowDb().deleteAll();
  }

}
