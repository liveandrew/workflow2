package com.rapleaf.cascading_ext.workflow2.state;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.rapleaf.cascading_ext.workflow2.FailingAction;
import com.rapleaf.cascading_ext.workflow2.IncrementAction;
import com.rapleaf.cascading_ext.workflow2.IncrementAction2;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

public class TestHdfsCheckpointPersistence extends WorkflowTestCase {

  private final String checkpointDir = getTestRoot() + "/checkpoints";

  @Before
  public void prepare() throws Exception {
    IncrementAction.counter = 0;
  }

  @Test
  public void testWritesCheckpoints() throws Exception {
    Step first = new Step(new IncrementAction("first"));
    Step second = new Step(new FailingAction("second"), first);

    try {
      new WorkflowRunner("test",
          new HdfsCheckpointPersistence(checkpointDir),
          new TestWorkflowOptions(),
          second).run();
      fail("should have failed!");
    } catch (Exception e) {
      // expected
    }

    assertEquals(1, IncrementAction.counter);
    assertTrue(getFS().exists(new Path(checkpointDir + "/first")));
  }

  @Test
  public void testResume() throws Exception {
    HdfsCheckpointPersistence persistenceFactory = new HdfsCheckpointPersistence(checkpointDir);

    AtomicInteger counter = new AtomicInteger();
    Step first = new Step(new IncrementAction2("first", counter));
    Step second = new Step(new FailingAction("second"), first);

    WorkflowRunner runner1 = new WorkflowRunner("test",
        persistenceFactory,
        new TestWorkflowOptions(),
        Sets.newHashSet(second)
    );

    try {
      runner1.run();
    } catch (Exception e) {
      //  fine
    }

    assertEquals(1L,
        runner1.getPersistence().getExecutionId()
    );

    //  resume

    counter = new AtomicInteger();
    first = new Step(new IncrementAction2("first", counter));
    second = new Step(new IncrementAction2("second", counter), first);

    InitializedWorkflow initialized = persistenceFactory.initialize(
        "test",
        new TestWorkflowOptions()
    );

    assertEquals(1L, initialized.getInitializedPersistence().getExecutionId());

    new WorkflowRunner(
        initialized,
        Sets.newHashSet(second)
    ).run();

    assertEquals(1L, initialized.getInitializedPersistence().getExecutionId());
    assertEquals(1, counter.get());


    //  complete, run a new execution

    counter = new AtomicInteger();
    first = new Step(new IncrementAction2("first", counter));
    second = new Step(new IncrementAction2("second", counter), first);

    WorkflowRunner runner2 = new WorkflowRunner("test",
        persistenceFactory,
        new TestWorkflowOptions(),
        Sets.newHashSet(second)
    );
    runner2.run();

    assertEquals(2L, runner2.getPersistence().getExecutionId());
    assertEquals(2, counter.get());

  }

}