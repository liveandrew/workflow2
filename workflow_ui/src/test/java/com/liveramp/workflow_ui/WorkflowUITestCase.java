package com.liveramp.workflow_ui;

import org.apache.log4j.Level;
import org.junit.Before;

import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.java_support.CommonJUnit4TestCase;

/**
 * Unit test for simple App.
 */
public abstract class WorkflowUITestCase extends CommonJUnit4TestCase {

  public WorkflowUITestCase() {
    super(Level.ALL);
  }

  @Before
  public void setUp() throws Exception {
    new DatabasesImpl().getRlDb().deleteAll();
    new com.liveramp.databases.workflow_db.DatabasesImpl().getWorkflowDb().deleteAll();
  }
}
