package com.liveramp.workflow2.workflow_hadoop;

import org.apache.log4j.Level;
import org.junit.Before;

import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.rapleaf.cascading_ext.test.HadoopCommonJunit4TestCase;

public class ResourcefulTestCase extends HadoopCommonJunit4TestCase {
  public ResourcefulTestCase() {
    super(Level.ALL, "resourceful");
  }

  @Before
  public void deleteFixtures() throws Exception {
    new DatabasesImpl().getWorkflowDb().deleteAll();
  }
}
