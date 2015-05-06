package com.rapleaf.cascading_ext.workflow2;

import org.apache.log4j.Level;
import org.junit.Before;

import com.rapleaf.cascading_ext.workflow2.test.BaseWorkflowTestCase;
import com.rapleaf.db_schemas.DatabasesImpl;

public class WorkflowTestCase extends BaseWorkflowTestCase {
  public WorkflowTestCase() {
    super(Level.ALL, "workflow");
  }

  @Before
  public void deleteFixtures() throws Exception {
    new DatabasesImpl().getRlDb().deleteAll();
  }
}
