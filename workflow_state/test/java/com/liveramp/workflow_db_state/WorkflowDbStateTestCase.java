package com.liveramp.workflow_db_state;

import org.apache.log4j.Level;
import org.junit.Before;

import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.rapleaf.java_support.CommonJUnit4TestCase;

public class WorkflowDbStateTestCase extends CommonJUnit4TestCase {

  public WorkflowDbStateTestCase() {
    super(Level.ALL);
  }

  public WorkflowDbStateTestCase(Level logLevel) {
    super(logLevel);
  }

  @Before
  public void deleteFixtures() throws Exception {
    new DatabasesImpl().getWorkflowDb().deleteAll();
  }
}
