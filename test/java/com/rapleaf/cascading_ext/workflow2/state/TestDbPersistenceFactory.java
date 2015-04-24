package com.rapleaf.cascading_ext.workflow2.state;

import java.util.List;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.Application;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.AttemptStatus;
import com.rapleaf.db_schemas.rldb.workflow.DbPersistence;
import com.rapleaf.db_schemas.rldb.workflow.StepStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowExecutionStatus;
import com.rapleaf.support.collections.Accessors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDbPersistenceFactory extends CascadingExtTestCase {

  @Before
  public void setUp() throws Exception {
    new DatabasesImpl().getRlDb().deleteAll();
  }

  @Test
  public void testAutoCleanup() throws Exception {

    IRlDb rldb = new DatabasesImpl().getRlDb();
    rldb.disableCaching();

    WorkflowExecution ex = rldb.workflowExecutions().create(
        "Workflow", WorkflowExecutionStatus.INCOMPLETE.ordinal()
    );

    long currentTime = System.currentTimeMillis();

    WorkflowAttempt workflowAttempt = rldb.workflowAttempts().create((int)ex.getId(), "bpodgursky", "default", "default", "localhost")
        .setStatus(AttemptStatus.RUNNING.ordinal())
        .setLastHeartbeat(currentTime - (DbPersistence.HEARTBEAT_INTERVAL * DbPersistence.NUM_HEARTBEAT_TIMEOUTS * 2));
    workflowAttempt.save();

    StepAttempt stepAttempt = rldb.stepAttempts().create((int)workflowAttempt.getId(), "step1", StepStatus.RUNNING.ordinal(), Object.class.getName());

    WorkflowRunner workflowRunner = new WorkflowRunner("Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions(),
        Sets.newHashSet(new Step(new NoOpAction("step1"))));
    workflowRunner.run();

    assertEquals(AttemptStatus.FAILED.ordinal(),
        rldb.workflowAttempts().find(workflowAttempt.getId()).getStatus().intValue());

    assertEquals(StepStatus.FAILED.ordinal(),
        rldb.stepAttempts().find(stepAttempt.getId()).getStepStatus());

  }

  @Test
  public void testApplicationCreation() throws Exception {


    IRlDb rldb = new DatabasesImpl().getRlDb();
    rldb.disableCaching();

    WorkflowRunner workflowRunner = new WorkflowRunner("Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions(),
        Sets.newHashSet(new Step(new NoOpAction("step1"))));
    workflowRunner.run();

    assertEquals(1, rldb.applications().findByName("Workflow").size());

    WorkflowRunner workflowRunner2 = new WorkflowRunner("Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions(),
        Sets.newHashSet(new Step(new NoOpAction("step1"))));
    workflowRunner2.run();

    List<Application> applications = rldb.applications().findByName("Workflow");
    assertEquals(1, applications.size());

    Application app = Accessors.only(applications);
    assertEquals(2, app.getWorkflowExecution().size());

  }

  @Test
  public void testFailOnCleanup() throws Exception {

    IRlDb rldb = new DatabasesImpl().getRlDb();
    rldb.disableCaching();

    WorkflowExecution ex = rldb.workflowExecutions().create(
        "Workflow", WorkflowExecutionStatus.INCOMPLETE.ordinal()
    );

    long currentTime = System.currentTimeMillis();

    WorkflowAttempt workflowAttempt = rldb.workflowAttempts().create((int)ex.getId(), "bpodgursky", "default", "default", "localhost")
        .setStatus(AttemptStatus.RUNNING.ordinal())
        .setLastHeartbeat(currentTime - (DbPersistence.HEARTBEAT_INTERVAL * 2));
    workflowAttempt.save();

    rldb.stepAttempts().create((int)workflowAttempt.getId(), "step1", StepStatus.RUNNING.ordinal(), Object.class.getName());

    Exception exception = getException(new Runnable2() {
      @Override
      public void run() throws Exception {
        new WorkflowRunner("Workflow",
            new DbPersistenceFactory(),
            new TestWorkflowOptions(),
            Sets.newHashSet(new Step(new NoOpAction("step1"))));
      }
    });

    assertTrue(exception.getCause().getMessage().startsWith("Cannot start, a previous attempt is still alive!"));

  }
}