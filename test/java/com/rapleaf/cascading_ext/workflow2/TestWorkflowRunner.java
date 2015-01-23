package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStoreImpl;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;
import com.rapleaf.cascading_ext.workflow2.context.HdfsContextStorage;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.DbPersistenceFactory;
import com.rapleaf.cascading_ext.workflow2.state.HdfsCheckpointPersistence;
import com.rapleaf.db_schemas.rldb.workflow.StepStatus;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowStatePersistence;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.formats.test.TupleDataStoreHelper;
import com.rapleaf.support.event_timer.TimedEvent;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

public class TestWorkflowRunner extends CascadingExtTestCase {

  private WorkflowPersistenceFactory hdfsPersistenceFactory = new HdfsCheckpointPersistence(getTestRoot() + "/hdfs_root");

  private WorkflowPersistenceFactory dbPersistenceFactory = new DbPersistenceFactory();

  @Before
  public void prepare() throws Exception {
    IncrementAction.counter = 0;
    new DatabasesImpl().getRlDb().deleteAll();
  }

  private WorkflowRunner buildWfr(WorkflowPersistenceFactory persistence, Step tail) {
    return buildWfr(persistence, Sets.newHashSet(tail));
  }

  private WorkflowRunner buildWfr(WorkflowPersistenceFactory persistence, Set<Step> tailSteps) {
    return buildWfr(persistence, new TestWorkflowOptions(), tailSteps);
  }

  private WorkflowRunner buildWfr(WorkflowPersistenceFactory persistence, WorkflowOptions opts, Set<Step> tailSteps) {
    return new WorkflowRunner("Test Workflow", persistence, opts, tailSteps);
  }


  @Test
  public void testSimple1() throws Exception {
    testSimple(hdfsPersistenceFactory);
  }

  @Test
  public void testSimple2() throws Exception {
    testSimple(dbPersistenceFactory);
  }


  public void testSimple(WorkflowPersistenceFactory persistence) throws Exception {
    Step first = new Step(new IncrementAction("first"));
    Step second = new Step(new IncrementAction("second"), first);

    buildWfr(persistence, second).run();

    assertEquals(2, IncrementAction.counter);
  }

  @Test
  public void testFullRestart1() throws IOException {
    testFullRestart(hdfsPersistenceFactory);
  }

  @Test
  public void testFullRestart2() throws IOException {
    testFullRestart(dbPersistenceFactory);
  }

  public void testFullRestart(WorkflowPersistenceFactory persistence) throws IOException {

    //  test a full restart if interrupted by a failure

    AtomicInteger int1 = new AtomicInteger(0);
    AtomicInteger int2 = new AtomicInteger(0);

    Step one = new Step(new IncrementAction2("one", int1));
    Step two = new Step(new FailingAction("two"), one);
    Step three = new Step(new IncrementAction2("three", int2), two);

    WorkflowRunner run = new WorkflowRunner("Test Workflow", persistence, new TestWorkflowOptions(), Sets.newHashSet(three));

    try {
      run.run();
      fail();
    } catch (Exception e) {
      //  no-op
    }

    assertEquals(1, int1.intValue());
    assertEquals(0, int2.intValue());

    one = new Step(new IncrementAction2("one", int1));
    two = new Step(new NoOpAction("two"), one);
    three = new Step(new IncrementAction2("three", int2), two);

    run = new WorkflowRunner("Test Workflow", persistence, new TestWorkflowOptions(), Sets.newHashSet(three));
    run.run();

    assertEquals(1, int1.intValue());
    assertEquals(1, int2.intValue());

  }

  @Test
  public void testLoneMultiStepAction1() throws Exception {
    testLoneMultiStepAction(hdfsPersistenceFactory);
  }

  @Test
  public void testLoneMultiStepAction2() throws Exception {
    testLoneMultiStepAction(dbPersistenceFactory);
  }

  public void testLoneMultiStepAction(WorkflowPersistenceFactory factory) throws Exception {
    // lone multi
    Step s = new Step(new MultiStepAction("lone", Arrays.asList(new Step(
        new IncrementAction("blah")))));

    buildWfr(factory, s).run();

    assertEquals(1, IncrementAction.counter);
  }

  @Test
  public void testMultiInTheMiddle1() throws Exception {
    testMultiIntheMiddle(hdfsPersistenceFactory);
  }

  @Test
  public void testMultiInTheMiddle2() throws Exception {
    testMultiIntheMiddle(dbPersistenceFactory);
  }

  public void testMultiIntheMiddle(WorkflowPersistenceFactory factory) throws IOException {
    Step s = new Step(new IncrementAction("first"));
    s = new Step(new MultiStepAction("lone", Arrays.asList(new Step(new IncrementAction("blah")))),
        s);
    s = new Step(new IncrementAction("last"), s);

    buildWfr(factory, s).run();

    assertEquals(3, IncrementAction.counter);
  }

  @Test
  public void testMultiAtTheEnd() throws Exception {
    testMultiAtEnd(hdfsPersistenceFactory);
  }

  @Test
  public void testMultiAtTheEnd2() throws Exception {
    testMultiAtEnd(dbPersistenceFactory);
  }


  public void testMultiAtEnd(WorkflowPersistenceFactory factory) throws IOException {
    Step s = new Step(new IncrementAction("first"));
    s = new Step(new MultiStepAction("lone", Arrays.asList(new Step(new IncrementAction("blah")))),
        s);

    buildWfr(factory, s).run();

    assertEquals(2, IncrementAction.counter);
  }

  private static Thread run(final WorkflowRunner runner, final Wrapper<Exception> exception) {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          runner.run();
        } catch (Exception e) {
          exception.setVal(e);
        }
      }
    });
  }

  @Test
  public void testFailThenShutdown1() throws InterruptedException, IOException {
    testFailThenShutdown(hdfsPersistenceFactory);
  }

  @Test
  public void testFailThenShutdown2() throws InterruptedException, IOException {
    testFailThenShutdown(dbPersistenceFactory);
  }

  public void testFailThenShutdown(WorkflowPersistenceFactory factory) throws InterruptedException, IOException {

    Semaphore semaphore = new Semaphore(0);
    Semaphore semaphore2 = new Semaphore(0);
    AtomicBoolean didExecute = new AtomicBoolean();

    Step fail = new Step(new DelayedFailingAction("fail", semaphore));
    Step unlockFail = new Step(new UnlockWaitAction("unlock", semaphore, semaphore2));
    Step last = new Step(new FlipAction("after", didExecute), unlockFail);

    Wrapper<Exception> exception = new Wrapper<Exception>();
    WorkflowRunner run = new WorkflowRunner("Test Workflow", factory, new TestWorkflowOptions().setMaxConcurrentSteps(2),
        Sets.newHashSet(fail, last));

    WorkflowStatePersistence persistence = run.getPersistence();

    Thread t = run(run, exception);
    t.start();

    Thread.sleep(500);
    persistence.markShutdownRequested("Shutdown Requested");
    semaphore2.release();

    t.join();

    Exception failure = exception.getVal();
    assertTrue(failure.getMessage().contains("(1/1) Step fail failed with exception: failed on purpose"));

    assertEquals("Shutdown Requested", persistence.getShutdownRequest());
    assertFalse(didExecute.get());
    assertTrue(persistence.getStepStatuses().get("fail").getStatus() == StepStatus.FAILED);
    assertTrue(persistence.getStepStatuses().get("unlock").getStatus() == StepStatus.COMPLETED);
    assertTrue(persistence.getStepStatuses().get("after").getStatus() == StepStatus.WAITING);

  }

  @Test
  public void testShutdownThenFail1() throws InterruptedException, IOException {
    testShutdownThenFail(hdfsPersistenceFactory);
  }

  @Test
  public void testShutdownThenFail2() throws InterruptedException, IOException {
    testShutdownThenFail(dbPersistenceFactory);
  }

  public void testShutdownThenFail(WorkflowPersistenceFactory factory) throws InterruptedException, IOException {

    Semaphore semaphore = new Semaphore(0);
    AtomicBoolean didExecute = new AtomicBoolean(false);


    Step fail = new Step(new DelayedFailingAction("fail", semaphore));
    Step after = new Step(new FlipAction("after", didExecute), fail);

    Wrapper<Exception> exception = new Wrapper<Exception>();
    WorkflowRunner run = new WorkflowRunner("Test Workflow", factory, new TestWorkflowOptions(), Sets.newHashSet(after));
    WorkflowStatePersistence persistence = run.getPersistence();

    Thread t = run(run, exception);
    t.start();

    Thread.sleep(500);
    persistence.markShutdownRequested("Shutdown Requested");

    semaphore.release();

    t.join();

    Exception failure = exception.getVal();
    assertTrue(failure.getMessage().contains("(1/1) Step fail failed with exception: failed on purpose"));

    assertEquals("Shutdown Requested", persistence.getShutdownRequest());
    assertFalse(didExecute.get());
    assertTrue(persistence.getStepStatuses().get("fail").getStatus() == StepStatus.FAILED);
    assertTrue(persistence.getStepStatuses().get("after").getStatus() == StepStatus.WAITING);
  }

  @Test
  public void testShutdown1() throws InterruptedException, IOException {
    testShutdown(hdfsPersistenceFactory);
  }

  @Test
  public void testShutdown12() throws InterruptedException, IOException {
    testShutdown(dbPersistenceFactory);
  }

  public void testShutdown(WorkflowPersistenceFactory factory) throws InterruptedException, IOException {

    Semaphore semaphore = new Semaphore(0);
    AtomicInteger preCounter = new AtomicInteger(0);
    AtomicInteger postConter = new AtomicInteger(0);

    Step pre = new Step(new IncrementAction2("pre", preCounter));
    Step step = new Step(new LockedAction("wait", semaphore), pre);
    Step after = new Step(new IncrementAction2("after", postConter), step);

    Wrapper<Exception> exception = new Wrapper<Exception>();
    WorkflowRunner run = new WorkflowRunner("Test Workflow", factory, new TestWorkflowOptions(), Sets.newHashSet(after));

    WorkflowStatePersistence peristence = run.getPersistence();

    Thread t = run(run, exception);
    t.start();

    Thread.sleep(500);
    peristence.markShutdownRequested("Shutdown Requested");

    semaphore.release();

    t.join();

    Exception failure = exception.getVal();

    assertEquals("Shutdown requested: Test Workflow. Reason: Shutdown Requested", failure.getMessage());

    assertEquals("Shutdown Requested", peristence.getShutdownRequest());
    assertEquals(1, preCounter.get());
    assertEquals(0, postConter.get());
    assertTrue(peristence.getStepStatuses().get("pre").getStatus() == StepStatus.COMPLETED);
    assertTrue(peristence.getStepStatuses().get("wait").getStatus() == StepStatus.COMPLETED);
    assertTrue(peristence.getStepStatuses().get("after").getStatus() == StepStatus.WAITING);

    //  restart

    run = new WorkflowRunner("Test Workflow", factory, new TestWorkflowOptions(), Sets.newHashSet(after));
    peristence = run.getPersistence();

    t = run(run, exception);

    t.start();
    semaphore.release();
    t.join();

    assertEquals(null, peristence.getShutdownRequest());
    assertEquals(1, preCounter.get());
    assertEquals(1, postConter.get());
    assertTrue(peristence.getStepStatuses().get("pre").getStatus() == StepStatus.SKIPPED);
    assertTrue(peristence.getStepStatuses().get("wait").getStatus() == StepStatus.SKIPPED);
    assertTrue(peristence.getStepStatuses().get("after").getStatus() == StepStatus.COMPLETED);

  }

  private static class Wrapper<T> {
    private T val;

    public T getVal() {

      if (val == null) {
        org.junit.Assert.fail("Expected value to be set!");
      }

      return val;
    }

    public void setVal(T val) {
      this.val = val;
    }
  }

  @Test
  public void testMultiInMultiEnd1() throws Exception {
    testMultiInMultiEnd(hdfsPersistenceFactory);
  }

  @Test
  public void testMultiInMultiEnd2() throws Exception {
    testMultiInMultiEnd(dbPersistenceFactory);
  }

  public void testMultiInMultiEnd(WorkflowPersistenceFactory factory) throws IOException {
    Step s = new Step(new IncrementAction("first"));
    // please, never do this in real code
    s = new Step(new MultiStepAction("depth 1", Arrays.asList(new Step(new MultiStepAction(
        "depth 2", Arrays.asList(new Step(new IncrementAction("blah"))))))), s);
    s = new Step(new IncrementAction("last"), s);

    buildWfr(factory, s).run();

    assertEquals(3, IncrementAction.counter);
  }

  @Test
  public void testMulitInMultiMiddle1() throws Exception {
    testMultiInMultiMiddle(hdfsPersistenceFactory);
  }

  @Test
  public void testMulitInMultiMiddle2() throws Exception {
    testMultiInMultiMiddle(dbPersistenceFactory);
  }

  public void testMultiInMultiMiddle(WorkflowPersistenceFactory factory) throws IOException {
    Step b = new Step(new IncrementAction("b"));
    Step innermost = new Step(new MultiStepAction("innermost", Arrays.asList(new Step(
        new IncrementAction("c")))), b);
    Step d = new Step(new IncrementAction("d"), b);

    Step a = new Step(new IncrementAction("a"));

    Step outer = new Step(new MultiStepAction("outer", Arrays.asList(b, innermost, d)), a);

    buildWfr(factory, outer).run();

    assertEquals(4, IncrementAction.counter);
  }

  @Test
  public void testDuplicateCheckpoints1() throws Exception {
    testDuplicateCheckpoints(hdfsPersistenceFactory);
  }

  @Test
  public void testDuplicateCheckpoints2() throws Exception {
    testDuplicateCheckpoints(dbPersistenceFactory);
  }

  public void testDuplicateCheckpoints(WorkflowPersistenceFactory factory) throws IOException {
    try {

      HashSet<Step> tails = Sets.newHashSet(
          new Step(new IncrementAction("a")),
          new Step(new IncrementAction("a")));

      buildWfr(factory, tails).run();

      fail("should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testTimingMultiStep1() throws Exception {
    testTimingMultiStep(hdfsPersistenceFactory);
  }

  @Test
  public void testTimingMultiStep2() throws Exception {
    testTimingMultiStep(dbPersistenceFactory);
  }

  public void testTimingMultiStep(WorkflowPersistenceFactory factory) throws Exception {

    Step bottom1 = new Step(new IncrementAction("bottom1"));
    Step bottom2 = new Step(new IncrementAction("bottom2"));

    Step multiMiddle = new Step(new MultiStepAction("middle", Arrays.asList(bottom1, bottom2)));
    Step flatMiddle = new Step(new IncrementAction("flatMiddle"));

    Step top = new Step(new MultiStepAction("Tom's first test dude", Arrays.asList(multiMiddle, flatMiddle)));

    WorkflowRunner testWorkflow = buildWfr(factory, top);
    testWorkflow.run();

    assertTrue(testWorkflow.getTimer() != null);

    // Goal here is to detect whether nested MultiTimedEvents ever have "-1"s in their timing and to FAIL if this occurs.

    // Assert that none of the timer.EventStartTime values are -1
    assertTrue(testWorkflow.getTimer().getEventStartTime() != -1);

    TimedEvent middleTimer = multiMiddle.getTimer();
    TimedEvent flatMiddleTimer = flatMiddle.getTimer();

    //    System.out.println("CHILDREN:");
    assertTrue(middleTimer.getEventStartTime() != -1);
    assertTrue(flatMiddleTimer.getEventStartTime() != -1);

    TimedEvent bottom1Timer = bottom1.getTimer();
    TimedEvent bottom2Timer = bottom1.getTimer();

    //    System.out.println("SUBCHILDREN:");
    assertTrue(bottom1Timer.getEventStartTime() != -1);
    assertTrue(bottom2Timer.getEventStartTime() != -1);
  }

  @Test
  public void testSandboxDir1() throws Exception {
    testSandboxDir(hdfsPersistenceFactory);
  }

  @Test
  public void testSandboxDir2() throws Exception {
    testSandboxDir(dbPersistenceFactory);
  }

  public void testSandboxDir(WorkflowPersistenceFactory persistence) throws Exception {
    try {
      WorkflowRunner wfr = buildWfr(persistence,
          Sets.newHashSet(fakeStep("a", "/fake/EVIL/../path"), fakeStep("b", "/path/of/fakeness"))
      );
      wfr.setSandboxDir("//fake/path");
      wfr.run();
      fail("There was an invalid path!");
    } catch (IOException e) {
      // expected
    }

    try {
      WorkflowRunner wfr = buildWfr(persistence,
          Sets.newHashSet(fakeStep("a", "/fake/EVIL/../path"),
              fakeStep("b", "/fake/./path")));

      wfr.setSandboxDir("//fake/path");
      wfr.run();
      // expected
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  public Step fakeStep(String checkpointToken, final String fakePath) {
    DataStore dataStore = new DataStore() {
      private String path = fakePath;

      @Override
      public String getName() {
        return "fakeDataStore";
      }

      @Override
      public Tap getTap() {
        return null;
      }

      @Override
      public String getPath() {
        return path;
      }

      @Override
      public String getRelPath() {
        return "." + path;
      }
    };
    Action action = new IncrementAction(checkpointToken);
    action.creates(dataStore);
    action.createsTemporary(dataStore);
    return new Step(action);
  }

  @Test
  public void testPathNesting1() throws IOException, ClassNotFoundException {
    testPathNesting(hdfsPersistenceFactory);
  }

  @Test
  public void testPathNesting2() throws IOException, ClassNotFoundException {
    testPathNesting(dbPersistenceFactory);
  }

  public void testPathNesting(WorkflowPersistenceFactory factory) throws IOException, ClassNotFoundException {

    String tmpRoot = getTestRoot() + "/tmp-dir";

    Step step = new Step(new ParentResource("parent-step", tmpRoot));

    HdfsContextStorage storage = new HdfsContextStorage(getTestRoot() + "/context");

    TestWorkflowOptions options = new TestWorkflowOptions()
        .setStorage(storage);

    WorkflowRunner wfr = new WorkflowRunner(
        "test workflow",
        factory,
        options,
        Sets.newHashSet(step)
    );
    wfr.run();
    WorkflowStatePersistence persistence = wfr.getPersistence();

    Resource<Integer> resMock1 = new Resource<Integer>("resource", new ActionId("parent-step")
        .setParentPrefix(""));
    Resource<Integer> resMock2 = new Resource<Integer>("output", new ActionId("consume-resource")
        .setParentPrefix("parent-step__"));

    assertEquals(1, storage.get(resMock1).intValue());
    assertEquals(1, storage.get(resMock2).intValue());

    Assert.assertEquals(StepStatus.COMPLETED, persistence.getState("parent-step__set-resource").getStatus());
    Assert.assertEquals(StepStatus.COMPLETED, persistence.getState("parent-step__consume-resource").getStatus());

    TupleDataStore store = new TupleDataStoreImpl("store", tmpRoot + "/parent-step-tmp-stores/consume-resource-tmp-stores/", "tup_out", new Fields("string"));
    List<Tuple> tups = HRap.getAllTuples(store.getTap());

    assertCollectionEquivalent(Sets.newHashSet(tups), Lists.<Tuple>newArrayList(new Tuple(1)));

  }

  public static class ParentResource extends MultiStepAction {

    public ParentResource(String checkpointToken, String tmpRoot) throws IOException {
      super(checkpointToken, tmpRoot);

      Resource<Integer> res = resource("resource");

      Step set = new Step(new SetResource(
          "set-resource",
          res
      ));

      Step get = new Step(new ConsumeResource(
          "consume-resource",
          getTmpRoot(),
          res),
          set
      );

      setSubStepsFromTail(get);

    }

  }

  public static class SetResource extends Action {

    private final Resource<Integer> res;

    public SetResource(String checkpointToken,
                       Resource<Integer> res1) {
      super(checkpointToken);
      this.res = res1;
      creates(res1);
    }

    @Override
    protected void execute() throws Exception {
      set(res, 1);
    }
  }

  public static class ConsumeResource extends Action {

    private final Resource<Integer> res;

    private final Resource<Integer> resOut;
    private final TupleDataStore tupOut;

    public ConsumeResource(String checkpointToken,
                           String tmpRoot,
                           Resource<Integer> res1) throws IOException {
      super(checkpointToken, tmpRoot);
      this.res = res1;
      uses(res);

      this.tupOut = builder().getTupleDataStore("tup_out", new Fields("string"));
      this.resOut = resource("output");
      creates(resOut);
    }

    @Override
    protected void execute() throws Exception {
      Integer val = get(res);
      set(resOut, val);

      TupleDataStoreHelper.writeToStore(tupOut,
          new Tuple(val)
      );
    }
  }


}
