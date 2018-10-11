package com.liveramp.resource_db_manager;

import org.apache.log4j.Level;
import org.junit.Before;

import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.rapleaf.java_support.CommonJUnit4TestCase;

public class ResourceDbManagerTestCase extends CommonJUnit4TestCase {

  public ResourceDbManagerTestCase() {
    super(Level.ALL);
  }

  @Before
  public void deleteFixtures() throws Exception {
    new DatabasesImpl().getWorkflowDb().deleteAll();
  }

}
