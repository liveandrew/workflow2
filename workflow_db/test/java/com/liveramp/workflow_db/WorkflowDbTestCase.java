package com.liveramp.workflow_db;

import org.apache.log4j.Level;
import org.junit.Before;

import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.rapleaf.java_support.CommonJUnit4TestCase;

public class WorkflowDbTestCase extends CommonJUnit4TestCase {

  public WorkflowDbTestCase() {
    super(Level.ALL);
  }

  public WorkflowDbTestCase(Level logLevel) {
    super(logLevel);
  }

  @Before
  public void deleteFixtures() throws Exception {
    new DatabasesImpl().getWorkflowDb().deleteAll();
  }
}
