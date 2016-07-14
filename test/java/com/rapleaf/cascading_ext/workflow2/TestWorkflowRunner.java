package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.flow.JobRecordListener;
import com.liveramp.cascading_ext.tap.NullTap;
import com.liveramp.cascading_tools.properties.PropertiesUtil;
import com.liveramp.commons.Accessors;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertMessage;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.MailBuffer;
import com.liveramp.java_support.alerts_handler.MailOptions;
import com.liveramp.java_support.alerts_handler.configs.DefaultAlertMessageConfig;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipient;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.liveramp.java_support.alerts_handler.recipients.RecipientListBuilder;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.java_support.workflow.ActionId;
import com.liveramp.workflow.test.MonitoredPersistenceFactory;
import com.liveramp.workflow_state.DbPersistence;
import com.liveramp.workflow_state.InitializedDbPersistence;
import com.liveramp.workflow_state.MapReduceJob;
import com.liveramp.workflow_state.StepState;
import com.liveramp.workflow_state.StepStatus;
import com.liveramp.workflow_state.WorkflowQueries;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.liveramp.workflow_state.controller.ApplicationController;
import com.liveramp.workflow_state.controller.ExecutionController;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStoreImpl;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;
import com.rapleaf.cascading_ext.workflow2.action.PersistNewVersion;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.DbPersistenceFactory;
import com.rapleaf.cascading_ext.workflow2.state.HdfsCheckpointPersistence;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.cascading_ext.workflow2.state.MultiShutdownHook;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.Application;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.StepDependency;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.formats.test.TupleDataStoreHelper;
import com.rapleaf.jack.queries.QueryOrder;
import com.rapleaf.types.new_person_data.PIN;
import com.liveramp.workflow.types.WorkflowAttemptStatus;
import com.liveramp.workflow.types.WorkflowExecutionStatus;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

//  TODO don't modify this class -- modify com.liverammp.workflow.TestWorkflowRunner.  getting swept after migration
public class TestWorkflowRunner extends WorkflowTestCase {

  private WorkflowPersistenceFactory hdfsPersistenceFactory = new HdfsCheckpointPersistence(getTestRoot() + "/hdfs_root");

  private WorkflowPersistenceFactory dbPersistenceFactory = new DbPersistenceFactory();

  @Before
  public void prepare() throws Exception {
    IncrementAction.counter = 0;
    new DatabasesImpl().getRlDb().deleteAll();
  }

  private WorkflowRunner buildWfr(WorkflowPersistenceFactory persistence, Step tail) throws IOException {
    return buildWfr(persistence, Sets.newHashSet(tail));
  }

  private WorkflowRunner buildWfr(WorkflowPersistenceFactory persistence, Set<Step> tailSteps) throws IOException {
    return buildWfr(persistence, new TestWorkflowOptions(), tailSteps);
  }

  private WorkflowRunner buildWfr(WorkflowPersistenceFactory persistence, WorkflowOptions opts, Set<Step> tailSteps) throws IOException {
    return new WorkflowRunner("Test Workflow", persistence, opts, tailSteps);
  }

  private InitializedWorkflow buildWf(WorkflowPersistenceFactory persistence, WorkflowOptions opts) throws IOException {
    return persistence.initialize("Test Workflow", opts);
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
  public void testRetainPool() throws IOException {

    assertEquals("root.dev-tools.some_pool", WorkflowPersistenceFactory
        .findDefaultValue(new TestWorkflowOptions().addWorkflowProperties(PropertiesUtil.teamPool(TeamList.DEV_TOOLS, "some_pool")).getWorkflowJobProperties(),
            "mapreduce.job.queuename",
            "default"
        )
    );
  }

  @Test
  public void testDescription() throws IOException {

    Step first = new Step(new IncrementAction("first"));

    WorkflowRunner wr = new WorkflowRunner("test",
        new DbPersistenceFactory(),
        new TestWorkflowOptions().setDescription("description1"),
        first
    );
    wr.run();

    IRlDb rldb = new DatabasesImpl().getRlDb();

    assertEquals("description1", rldb.workflowAttempts().find(wr.getPersistence().getAttemptId()).getDescription());
  }


  @Test
  public void testInitialStatus() throws IOException {
    IRlDb rldb = new DatabasesImpl().getRlDb();

    Step first = new Step(new IncrementAction("first"));
    WorkflowAttempt attempt = WorkflowQueries.getLatestAttempt(rldb.workflowExecutions().find(buildWfr(dbPersistenceFactory, first).getPersistence().getExecutionId()));
    assertEquals(WorkflowAttemptStatus.INITIALIZING.ordinal(), attempt.getStatus().intValue());

  }

  @Test
  public void testFullRestart1() throws IOException {
    testFullRestart(hdfsPersistenceFactory);
  }

  @Test
  public void testFailReturnsChain() throws IOException {

    //  TODO the case which this tries to catch hangs forever.  figure out a timer or something
    //  instead of just having the test hang

    Step zero = new Step(new NoOpAction("succeed"));
    Step one = new Step(new FailingAction("fail"));
    Step two = new Step(new NoOpAction("after"), zero, one);
    Step three = new Step(new NoOpAction("later"), two);

    try {
      execute(three);
      fail();
    } catch (Exception e) {
      // fine
    }
  }

  @Test
  public void testNotificationDestinations() throws IOException {
    IRlDb rldb = new DatabasesImpl().getRlDb();
    rldb.disableCaching();

    MailBuffer providedBuffer = new MailBuffer.ListBuffer();

    Step one = new Step(new FailingAction("fail"));

    WorkflowRunner wr = new WorkflowRunner("test", new DbPersistenceFactory(), new TestWorkflowOptions()
        .setNotificationLevel(WorkflowNotificationLevel.ERROR)
        .setAlertsHandler(AlertsHandlers.builder(TeamList.DEV_TOOLS).setTestMailBuffer(providedBuffer).build()),
        one);

    ApplicationController.addConfiguredNotifications(rldb,
        "test",
        "test@gmail.com",
        WorkflowNotificationLevel.ERROR
    );

    ExecutionController.addConfiguredNotifications(rldb,
        wr.getPersistence().getExecutionId(),
        "ben@gmail.com",
        WorkflowNotificationLevel.ERROR
    );

    try {
      wr.run();
      fail();
    } catch (Exception e) {
      // fine
    }

    DbPersistence origPersistence = (DbPersistence)wr.getPersistence();
    DbPersistence newPersistence = DbPersistence.queryPersistence(origPersistence.getAttemptId(), rldb);

    for (AlertsHandler alertsHandler : newPersistence.getRecipients(WorkflowRunnerNotification.DIED_UNCLEAN)) {
      alertsHandler.sendAlert("Died Unclean", "Test", AlertRecipients.engineering(AlertSeverity.ERROR));
    }

    Multimap<String, String> recipientToSubjects = HashMultimap.create();

    for (MailOptions mailOptions : newPersistence.getTestMailBuffer().get()) {
      for (String to : mailOptions.getToEmails()) {
        recipientToSubjects.put(to, mailOptions.getSubject());
      }
    }

    for (MailOptions mailOptions : Iterables.concat(providedBuffer.get(), origPersistence.getTestMailBuffer().get())) {
      for (String to : mailOptions.getToEmails()) {
        recipientToSubjects.put(to, mailOptions.getSubject());
      }
    }

    assertEquals(3, recipientToSubjects.keySet().size());

    assertCollectionEquivalent(recipientToSubjects.get("ben@gmail.com"), Lists.newArrayList("Died Unclean", "[ERROR] [WORKFLOW] Failed: test"));
    assertCollectionEquivalent(recipientToSubjects.get("test@gmail.com"), Lists.newArrayList("Died Unclean", "[ERROR] [WORKFLOW] Failed: test"));
    assertCollectionEquivalent(recipientToSubjects.get("dev-tools+error@liveramp.com"), Lists.newArrayList("Died Unclean", "[ERROR] [WORKFLOW] Failed: test"));

  }

  @Test
  public void testKeepRunning() throws Exception {

    final Semaphore sem = new Semaphore(0);
    final Semaphore sem2 = new Semaphore(0);

    Step one = new Step(new DelayedFailingAction("fail", sem2));
    Step two = new Step(new UnlockWaitAction("wait", sem2, sem));
    Step three = new Step(new NoOpAction("proceed"), two);

    final List<String> messages = Lists.newArrayList();

    WorkflowRunner runner = new WorkflowRunner("Test Workflow", new DbPersistenceFactory(), new TestWorkflowOptions()
        .setNotificationLevel(WorkflowNotificationLevel.ERROR)
        .setMaxConcurrentSteps(2)
        .setAlertsHandler(new AlertsHandler() {

          @Override
          public void sendAlert(AlertMessage contents, AlertRecipient recipient, AlertRecipient... ad) {
            messages.add(contents.getSubject(new DefaultAlertMessageConfig(true, Lists.newArrayList("TAG"))));
            sem.release();
          }

          @Override
          public void sendAlert(String subject, String body, AlertRecipient recipient, AlertRecipient... additionalRecipients) {
          }

          @Override
          public void sendAlert(String subject, Throwable t, AlertRecipient recipient, AlertRecipient... additionalRecipients) {
          }

          @Override
          public void sendAlert(String subject, String body, Throwable t, AlertRecipient recipient, AlertRecipient... additionalRecipients) {
          }

          @Override
          public RecipientListBuilder resolveRecipients(List<AlertRecipient> recipients) {
            return new RecipientListBuilder();
          }

        }), Sets.newHashSet(one, three));

    try {
      runner.run();
      fail();
    } catch (Exception e) {
      // fine
    }


    assertEquals(StepStatus.COMPLETED, runner.getPersistence().getStatus("proceed"));
    assertEquals(StepStatus.COMPLETED, runner.getPersistence().getStatus("wait"));
    assertEquals(StepStatus.FAILED, runner.getPersistence().getStatus("fail"));

    assertCollectionEquivalent(Lists.newArrayList(
        "[ERROR] [TAG] [WORKFLOW] Step has failed in: Test Workflow",
        "[ERROR] [TAG] [WORKFLOW] Failed: Test Workflow"),
        messages
    );

  }


  private static class EmptyMSA extends MultiStepAction {

    public EmptyMSA(String checkpointToken, String tmpRoot) {
      super(checkpointToken, tmpRoot);

      setSubSteps(Sets.<Step>newHashSet());
    }
  }

  @Test
  public void testEmptyMSA() throws Exception {

    Step step1 = new Step(new EmptyMSA("empty-msa",
        getTestRoot() + "/tmp"
    ));

    Step step2 = new Step(new NoOpAction("dep"),
        step1
    );

    execute(step2);

    StepDependency dep = Accessors.only(new DatabasesImpl().getRlDb().stepDependencies().findAll());

    StepAttempt attempt1 = dep.getStepAttempt();
    StepAttempt attempt2 = dep.getDependencyAttempt();

    assertEquals("dep", attempt1.getStepToken());
    assertEquals("empty-msa__empty-msa-placeholder", attempt2.getStepToken());

  }

  @Test
  public void testNotificationLevels() throws Exception {

    runFlow(WorkflowNotificationLevel.DEBUG, new NoOpAction("step"), Lists.newArrayList(
        "[WORKFLOW] Started: Test workflow",
        "[WORKFLOW] Succeeded: Test workflow"
    ));

    runFlow(WorkflowNotificationLevel.ERROR, new NoOpAction("step"), Lists.<String>newArrayList());

    runFlow(WorkflowNotificationLevel.INFO, new NoOpAction("step"), Lists.newArrayList(
        "[WORKFLOW] Succeeded: Test workflow"
    ));

    runFlow(WorkflowNotificationLevel.ERROR, new FailingAction("step"), Lists.newArrayList(
        "[ERROR] [WORKFLOW] Failed: Test workflow"
    ));

    runFlow(WorkflowNotificationLevel.DEBUG, new FailingAction("step"), Lists.newArrayList(
        "[WORKFLOW] Started: Test workflow",
        "[ERROR] [WORKFLOW] Failed: Test workflow"
    ));

  }

  private void runFlow(Set<WorkflowRunnerNotification> level, Action toRun, List<String> expectedAlerts) throws IOException {

    SubjectAlertHandler handler = new SubjectAlertHandler();

    Step step = new Step(toRun);

    try {
      execute(step, new TestWorkflowOptions()
          .setNotificationLevel(level)
          .setAlertsHandler(handler)
      );
    } catch (Exception e) {
      //  fine
    }

    assertCollectionEquivalent(expectedAlerts, handler.getSubjects());

  }


  private static class SubjectAlertHandler implements AlertsHandler {

    private final Set<String> subjects = Sets.newHashSet();

    public Set<String> getSubjects() {
      return subjects;
    }

    @Override
    public void sendAlert(AlertMessage contents, AlertRecipient recipient, AlertRecipient... ad) {
      subjects.add(contents.getSubject(new DefaultAlertMessageConfig(false, Lists.<String>newArrayList())));
    }

    @Override
    public void sendAlert(String subject, String body, AlertRecipient recipient, AlertRecipient... additionalRecipients) {
      subjects.add(subject);
    }

    @Override
    public void sendAlert(String subject, Throwable t, AlertRecipient recipient, AlertRecipient... additionalRecipients) {
      subjects.add(subject);
    }

    @Override
    public void sendAlert(String subject, String body, Throwable t, AlertRecipient recipient, AlertRecipient... additionalRecipients) {
      subjects.add(subject);
    }

    @Override
    public RecipientListBuilder resolveRecipients(List<AlertRecipient> recipients) {
      return new RecipientListBuilder();
    }
  }


  @Test
  public void testSingleAlert() throws Exception {

    Step one = new Step(new FailingAction("fail"));

    final List<String> messages = Lists.newArrayList();

    WorkflowRunner runner = new WorkflowRunner("Test Workflow", new DbPersistenceFactory(), new TestWorkflowOptions()
        .setNotificationLevel(WorkflowNotificationLevel.ERROR)
        .setAlertsHandler(new AlertsHandler() {

          @Override
          public void sendAlert(AlertMessage contents, AlertRecipient recipient, AlertRecipient... ad) {
            messages.add(contents.getSubject(new DefaultAlertMessageConfig(true, Lists.newArrayList("TAG"))));
          }

          @Override
          public void sendAlert(String subject, String body, AlertRecipient recipient, AlertRecipient... additionalRecipients) {
          }

          @Override
          public void sendAlert(String subject, Throwable t, AlertRecipient recipient, AlertRecipient... additionalRecipients) {
          }

          @Override
          public void sendAlert(String subject, String body, Throwable t, AlertRecipient recipient, AlertRecipient... additionalRecipients) {
          }

          @Override
          public RecipientListBuilder resolveRecipients(List<AlertRecipient> recipients) {
            return new RecipientListBuilder();
          }

        }), one);

    try {
      runner.run();
      fail();
    } catch (Exception e) {
      // fine
    }

    assertCollectionEquivalent(Lists.newArrayList(
        "[ERROR] [TAG] [WORKFLOW] Failed: Test Workflow"),
        messages
    );

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
    Step s = new Step(new MultiStepAction("lone", getTestRoot(), Arrays.asList(new Step(
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
    s = new Step(new MultiStepAction("lone", getTestRoot(), Arrays.asList(new Step(new IncrementAction("blah")))),
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
    s = new Step(new MultiStepAction("lone", getTestRoot(), Arrays.asList(new Step(new IncrementAction("blah")))),
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

    Wrapper<Exception> exception = new Wrapper<>();
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
    assertTrue(persistence.getStatus("fail") == StepStatus.FAILED);
    assertTrue(persistence.getStatus("unlock") == StepStatus.COMPLETED);
    assertTrue(persistence.getStatus("after") == StepStatus.WAITING);

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
    assertTrue(persistence.getStatus("fail") == StepStatus.FAILED);
    assertTrue(persistence.getStatus("after") == StepStatus.WAITING);
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
    assertTrue(peristence.getStatus("pre") == StepStatus.COMPLETED);
    assertTrue(peristence.getStatus("wait") == StepStatus.COMPLETED);
    assertTrue(peristence.getStatus("after") == StepStatus.WAITING);

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
    assertTrue(peristence.getStatus("pre") == StepStatus.SKIPPED);
    assertTrue(peristence.getStatus("wait") == StepStatus.SKIPPED);
    assertTrue(peristence.getStatus("after") == StepStatus.COMPLETED);

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
    s = new Step(new MultiStepAction("depth 1", getTestRoot(), Arrays.asList(new Step(new MultiStepAction(
        "depth 2", getTestRoot(), Arrays.asList(new Step(new IncrementAction("blah"))))))), s);
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
    Step innermost = new Step(new MultiStepAction("innermost", getTestRoot(), Arrays.asList(new Step(
        new IncrementAction("c")))), b);
    Step d = new Step(new IncrementAction("d"), b);

    Step a = new Step(new IncrementAction("a"));

    Step outer = new Step(new MultiStepAction("outer", getTestRoot(), Arrays.asList(b, innermost, d)), a);

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
  public void testSandboxDir1() throws Exception {
    testSandboxDir(hdfsPersistenceFactory);
  }

  @Test
  public void testSandboxDir2() throws Exception {
    testSandboxDir(dbPersistenceFactory);
  }

  private void testSandboxDir(WorkflowPersistenceFactory persistence) throws Exception {

    InitializedWorkflow wf = buildWf(
        persistence,
        new TestWorkflowOptions().setSandboxDir("//fake/path")
    );

    try {

      new WorkflowRunner(
          wf,
          Sets.newHashSet(fakeStep("a", "/fake/EVIL/../path"), fakeStep("b", "/path/of/fakeness"))
      ).run();

      fail("There was an invalid path!");
    } catch (Exception e) {
      //  so we can try again in this jvm
      wf.getInitializedPersistence().markWorkflowStopped();
    }

    WorkflowRunner wfr = buildWfr(persistence,
        new TestWorkflowOptions().setSandboxDir("//fake/path"),
        Sets.newHashSet(fakeStep("a", "/fake/EVIL/../path"),
            fakeStep("b", "/fake/./path")));
    wfr.run();

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

    WorkflowOptions options = new TestWorkflowOptions()
        .setStorage(storage);

    WorkflowRunner wfr = new WorkflowRunner(
        "test workflow",
        factory,
        options,
        Sets.newHashSet(step)
    );
    wfr.run();
    WorkflowStatePersistence persistence = wfr.getPersistence();

    OldResource<Integer> resMock1 = new OldResource<Integer>("resource", new ActionId("parent-step")
        .setParentPrefix(""));
    OldResource<Integer> resMock2 = new OldResource<Integer>("output", new ActionId("consume-resource")
        .setParentPrefix("parent-step__"));

    assertEquals(1, storage.get(resMock1).intValue());
    assertEquals(1, storage.get(resMock2).intValue());

    Assert.assertEquals(StepStatus.COMPLETED, persistence.getStatus("parent-step__set-resource"));
    Assert.assertEquals(StepStatus.COMPLETED, persistence.getStatus("parent-step__consume-resource"));

    TupleDataStore store = new TupleDataStoreImpl("store", tmpRoot + "/parent-step-tmp-stores/consume-resource-tmp-stores/", "tup_out", new Fields("string"));
    List<Tuple> tups = HRap.getAllTuples(store.getTap());

    assertCollectionEquivalent(Sets.newHashSet(tups), Lists.<Tuple>newArrayList(new Tuple(1)));

  }

  @Test
  public void integrationTestCancelComplete() throws Exception {
    IRlDb rldb = new DatabasesImpl().getRlDb();

    AtomicInteger step1Count = new AtomicInteger(0);
    AtomicInteger step2Count = new AtomicInteger(0);

    Step step1 = new Step(new IncrementAction2("step1", step1Count));
    Step step2 = new Step(new IncrementAction2("step2", step2Count), step1);

    WorkflowRunner testWorkflow = buildWfr(dbPersistenceFactory, step2);
    testWorkflow.run();

    WorkflowStatePersistence origPersistence = testWorkflow.getPersistence();

    List<Application> applications = rldb.applications().query().name("Test Workflow").find();
    assertEquals(1, applications.size());

    assertEquals(WorkflowExecutionStatus.COMPLETE.ordinal(), rldb.workflowExecutions().query()
        .applicationId(Accessors.only(applications).getIntId())
        .find().iterator().next().getStatus());

    origPersistence.markStepReverted("step1");

    assertEquals(WorkflowExecutionStatus.INCOMPLETE.ordinal(), rldb.workflowExecutions().query()
        .applicationId(Accessors.only(applications).getIntId())
        .find().iterator().next().getStatus());


    step1 = new Step(new IncrementAction2("step1", step1Count));
    step2 = new Step(new IncrementAction2("step2", step2Count), step1);

    testWorkflow = buildWfr(dbPersistenceFactory, step2);
    testWorkflow.run();

    assertEquals(2, step1Count.get());
    assertEquals(1, step2Count.get());
    assertEquals(StepStatus.REVERTED, origPersistence.getStatus("step1"));

  }

  @Test
  public void integrationTestCancelTwoDeep() throws Exception {
    IRlDb rldb = new DatabasesImpl().getRlDb();
    rldb.disableCaching();

    //  complete, fail, wait
    //  skip, complete, fail
    //  cancel 1
    //  make sure it runs 1 again

    AtomicInteger step1Count = new AtomicInteger(0);
    AtomicInteger step2Count = new AtomicInteger(0);
    AtomicInteger step3Count = new AtomicInteger(0);

    Step step1 = new Step(new IncrementAction2("step1", step1Count));
    Step step2 = new Step(new FailingAction("step2"), step1);
    Step step3 = new Step(new IncrementAction2("step3", step3Count), step2);

    WorkflowRunner testWorkflow = buildWfr(dbPersistenceFactory, step3);

    try {
      testWorkflow.run();
    } catch (Exception e) {
      //  fine
    }

    WorkflowStatePersistence pers1 = testWorkflow.getPersistence();

    step1 = new Step(new IncrementAction2("step1", step1Count));
    step2 = new Step(new IncrementAction2("step2", step2Count), step1);
    step3 = new Step(new IncrementAction2("step3", step3Count), step2);

    testWorkflow = buildWfr(dbPersistenceFactory, step3);
    DbPersistence pers2 = (DbPersistence)testWorkflow.getPersistence();

    try {
      testWorkflow.run();
    } catch (Exception e) {
      //  fine
    }

    Assert.assertEquals(WorkflowExecutionStatus.COMPLETE, getExecutionStatus(rldb, pers2));
    assertEquals(WorkflowAttemptStatus.FINISHED, pers2.getStatus());

    pers1.markStepReverted("step1");

    Assert.assertEquals(WorkflowExecutionStatus.INCOMPLETE, getExecutionStatus(rldb, pers2));
    assertEquals(WorkflowAttemptStatus.FINISHED, pers2.getStatus());

    step1 = new Step(new IncrementAction2("step1", step1Count));
    step2 = new Step(new IncrementAction2("step2", step2Count), step1);
    step3 = new Step(new IncrementAction2("step3", step3Count), step2);

    testWorkflow = buildWfr(dbPersistenceFactory, step3);
    testWorkflow.run();

    Assert.assertEquals(WorkflowExecutionStatus.COMPLETE, getExecutionStatus(rldb, pers2));
    assertEquals(WorkflowAttemptStatus.FINISHED, pers2.getStatus());

    assertEquals(2, step1Count.get());
    assertEquals(1, step2Count.get());
    assertEquals(1, step3Count.get());

  }

  public static WorkflowExecutionStatus getExecutionStatus(IRlDb rldb, WorkflowStatePersistence persistence) throws IOException {
    return WorkflowExecutionStatus.findByValue(rldb.workflowExecutions().find(persistence.getExecutionId()).getStatus());
  }

  @Test
  public void testCancelWorkflow() throws IOException {

    final IRlDb rldb = new DatabasesImpl().getRlDb();
    rldb.disableCaching();

    AtomicInteger step1Count = new AtomicInteger(0);
    AtomicInteger step2Count = new AtomicInteger(0);

    Step step1 = new Step(new IncrementAction2("step1", step1Count));
    Step step2 = new Step(new FailingAction("step2"), step1);

    final WorkflowRunner testWorkflow = buildWfr(dbPersistenceFactory, step2);

    getException(new Runnable2() {
      @Override
      public void run() throws Exception {
        testWorkflow.run();
      }
    });

    WorkflowStatePersistence persistence = testWorkflow.getPersistence();

    ApplicationController.cancelLatestExecution(rldb, "Test Workflow", null);
    WorkflowExecution ex = Accessors.first(rldb.workflowExecutions().findAll());

    assertEquals(WorkflowExecutionStatus.CANCELLED.ordinal(), ex.getStatus());
    assertEquals(StepStatus.REVERTED.ordinal(), persistence.getStatus("step1").ordinal());
    assertEquals(StepStatus.FAILED.ordinal(), persistence.getStatus("step2").ordinal());

    //  restart workflow
    step1 = new Step(new IncrementAction2("step1", step1Count));
    step2 = new Step(new IncrementAction2("step2", step2Count), step1);

    WorkflowRunner restartedWorkflow = buildWfr(dbPersistenceFactory, step2);
    restartedWorkflow.run();
    WorkflowStatePersistence newPeristence = restartedWorkflow.getPersistence();

    //  nothing changed for first execution
    List<WorkflowExecution> executions = rldb.workflowExecutions().query().order(QueryOrder.ASC).find();
    ex = Accessors.first(executions);
    assertEquals(WorkflowExecutionStatus.CANCELLED.ordinal(), ex.getStatus());
    assertEquals(StepStatus.REVERTED.ordinal(), persistence.getStatus("step1").ordinal());
    assertEquals(StepStatus.FAILED.ordinal(), persistence.getStatus("step2").ordinal());

    //  second one is complete
    final WorkflowExecution ex2 = Accessors.second(executions);
    assertEquals(WorkflowExecutionStatus.COMPLETE.ordinal(), ex2.getStatus());
    assertEquals(StepStatus.COMPLETED.ordinal(), newPeristence.getStatus("step1").ordinal());
    assertEquals(StepStatus.COMPLETED.ordinal(), newPeristence.getStatus("step2").ordinal());

    assertEquals(2, step1Count.get());
    assertEquals(1, step2Count.get());

    //  run a third time
    step1 = new Step(new IncrementAction2("step1", step1Count));
    step2 = new Step(new IncrementAction2("step2", step2Count), step1);

    restartedWorkflow = buildWfr(dbPersistenceFactory, step2);
    restartedWorkflow.run();

    Exception cancelNonLatest = getException(new Runnable2() {
      @Override
      public void run() throws Exception {
        ExecutionController.cancelExecution(rldb, ex2);
      }
    });

    assertTrue(cancelNonLatest.getMessage().startsWith("Cannot revert steps or cancel execution"));

  }

  @Test
  public void testCancelApplication() throws Exception {

    IRlDb rldb = new DatabasesImpl().getRlDb();

    Step step1 = new Step(new NoOpAction("step1"));

    WorkflowRunner restartedWorkflow = new WorkflowRunner("HUMAN_INTERVENTION",
        dbPersistenceFactory,
        new TestWorkflowOptions().setAppType(AppType.HUMAN_INTERVENTION),
        Sets.newHashSet(step1)
    );

    restartedWorkflow.run();

    ApplicationController.cancelLatestExecution(rldb, AppType.HUMAN_INTERVENTION, null);
  }

  @Test
  public void integrationTestCancelIncomplete() throws Exception {

    AtomicInteger step1Count = new AtomicInteger(0);
    AtomicInteger step2Count = new AtomicInteger(0);

    Step step1 = new Step(new IncrementAction2("step1", step1Count));
    Step step2 = new Step(new FailingAction("step2"), step1);

    WorkflowRunner testWorkflow = buildWfr(dbPersistenceFactory, step2);

    try {
      testWorkflow.run();
    } catch (Exception e) {
      //  fine
    }

    testWorkflow.getPersistence().markStepReverted("step1");


    step1 = new Step(new IncrementAction2("step1", step1Count));
    step2 = new Step(new IncrementAction2("step2", step2Count), step1);

    testWorkflow = buildWfr(dbPersistenceFactory, step2);
    testWorkflow.run();

    assertEquals(2, step1Count.get());
    assertEquals(1, step2Count.get());

  }


  @Test
  public void integrationTestCancelFailure() throws Exception {

    Step step1 = new Step(new NoOpAction("step1"));
    Step step2 = new Step(new NoOpAction("step2"), step1);

    WorkflowRunner firstRun = buildWfr(dbPersistenceFactory, step2);
    firstRun.run();

    step1 = new Step(new NoOpAction("step1"));
    step2 = new Step(new NoOpAction("step2"), step1);

    WorkflowRunner secondRun = buildWfr(dbPersistenceFactory, step2);
    secondRun.run();

    assertEquals(StepStatus.COMPLETED, secondRun.getPersistence().getStatus("step1"));

    try {
      firstRun.getPersistence().markStepReverted("step1");
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Cannot revert steps or cancel execution"));
    }

  }

  public static class ParentResource extends MultiStepAction {

    public ParentResource(String checkpointToken, String tmpRoot) throws IOException {
      super(checkpointToken, tmpRoot);

      OldResource<Integer> res = resource("resource");

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

  @Test
  public void testCounters() throws IOException {

    TupleDataStore input = builder().getTupleDataStore("input",
        new Fields("string")
    );

    TupleDataStoreHelper.writeToStore(input,
        new Tuple("1")
    );

    TupleDataStore output = builder().getTupleDataStore("output",
        new Fields("string")
    );

    Step step = new Step(new IncrementCounter("step1", getTestRoot(), input, output));
    Step step2 = new Step(new IncrementCounter2("step2", getTestRoot(), input, output), step);

    WorkflowRunner runner = new WorkflowRunner("Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions(),
        Sets.newHashSet(step, step2)
    );

    runner.run();

    TwoNestedMap<String, String, Long> counters = runner.getPersistence().getFlatCounters();

    assertEquals(1L, counters.get("CUSTOM_COUNTER", "NAME").longValue());

    assertEquals(1L, counters.get("CUSTOM_COUNTER2", "NAME").longValue());

    TaskCounter mapIn = TaskCounter.MAP_INPUT_RECORDS;
    assertEquals(2L, counters.get(mapIn.getClass().getName(), mapIn.name()).longValue());

  }

  @Test
  public void testCountersOnRestart() throws IOException {


    TupleDataStore input = builder().getTupleDataStore("input",
        new Fields("string")
    );

    TupleDataStoreHelper.writeToStore(input,
        new Tuple("1")
    );

    TupleDataStore output = builder().getTupleDataStore("output",
        new Fields("string")
    );

    Step step = new Step(new IncrementCounter("step1", getTestRoot(), input, output));
    Step step2 = new Step(new FailingAction("step2"), step);

    WorkflowRunner runner = new WorkflowRunner("Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions(),
        Sets.newHashSet(step2)
    );

    try {
      runner.run();
    } catch (Exception e) {
      // expected
    }

    step = new Step(new IncrementCounter("step1", getTestRoot(), input, output));
    step2 = new Step(new IncrementCounter2("step2", getTestRoot(), input, output), step);

    runner = new WorkflowRunner("Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions(),
        Sets.newHashSet(step2)
    );

    runner.run();

    //  test flat

    TwoNestedMap<String, String, Long> counters = runner.getPersistence().getFlatCounters();

    assertEquals(1L, counters.get("CUSTOM_COUNTER", "NAME").longValue());

    assertEquals(1L, counters.get("CUSTOM_COUNTER2", "NAME").longValue());

    TaskCounter mapIn = TaskCounter.MAP_INPUT_RECORDS;
    assertEquals(2L, counters.get(mapIn.getClass().getName(), mapIn.name()).longValue());

    //  test by step

    ThreeNestedMap<String, String, String, Long> countersByStep = runner.getPersistence().getCountersByStep();
    assertEquals(1L, countersByStep.get("step1__step", mapIn.getClass().getName(), mapIn.name()).longValue());


  }

  @Test
  public void testDatastoreDeps() throws Exception {

    BucketDataStore<PIN> store1 = builder().getBucketDataStore("store1", PIN.class);
    BucketDataStore<PIN> store2 = builder().getBucketDataStore("store2", PIN.class);
    VersionedBucketDataStore<PIN> store1P = builder().getVersionedBucketDataStore("stoer1p", PIN.class);
    BucketDataStore<PIN> store3 = builder().getBucketDataStore("store3", PIN.class);

    Step step1 = new Step(new CopyStore("step1", store1, store2));
    Step step2 = new Step(new PersistNewVersion<>("step2", store2, store1P), step1);
    new WorkflowRunner("workflow1", new DbPersistenceFactory(), new TestWorkflowOptions()
        .addWorkflowProperties(PropertiesUtil.teamPool(TeamList.DISTRIBUTION, "default")), step2).run();

    Step step3 = new Step(new CopyStore("step2", store1P.getLatestVersion(), store3));
    new WorkflowRunner("workflow2", new DbPersistenceFactory(), new TestWorkflowOptions()
        .addWorkflowProperties(PropertiesUtil.teamPool(TeamList.APEX, "default")), step3).run();

  }

  @Test
  public void testTaskStatistics() throws Exception {

    TupleDataStore input = builder().getTupleDataStore("input",
        new Fields("string")
    );

    TupleDataStoreHelper.writeToStore(input,
        new Tuple("1")
    );

    WorkflowRunner stat = execute(new StatAction("stat", input));

    StepState state = stat.getPersistence().getStepStates().get("stat");
    MapReduceJob job = Accessors.only(state.getMrJobsByID().values());

    //  unfortunately we can't get real values locally, but prove not fail and not null
    assertEquals(0L, job.getTaskSummary().getAvgMapDuration().longValue());
    assertNotNull(job.getTaskSummary().getTaskFailures());

  }

  @Test
  public void testSimultaneousConcurrent() throws IOException {

    //  simple -- confirm we are not calling getStepStatuses for each step

    Set<Step> heads = Sets.newHashSet();
    for (int i = 0; i < 50; i++) {
      heads.add(new Step(new DelayAction("step-" + i)));
    }

    Step tail = new Step(new NoOpAction("tail"), heads);

    WorkflowRunner runner = new WorkflowRunner(
        TestWorkflowRunner.class.getName(),
        new MonitoredPersistenceFactory(new DbPersistenceFactory()),
        new TestWorkflowOptions()
            .setStepPollInterval(2000)
            //  concurrency 200
            .setMaxConcurrentSteps(200),
        tail
    );
    runner.run();

    MonitoredPersistenceFactory.MonitoredPersistence monitoredPersistence =
        (MonitoredPersistenceFactory.MonitoredPersistence)runner.getPersistence();

    //  three times through the loop, once checking if we had step failures
    assertEquals(7, monitoredPersistence.getGetStepStatusCalls());

  }


  @Test
  public void testInitializeBeforePrepare() throws IOException {

    AtomicLong workflowID = new AtomicLong();

    //  initialize the workflow with a name and scope to get an execution ID
    InitializedWorkflow workflow = new DbPersistenceFactory().initialize(
        new TestWorkflowOptions()
            .setUniqueIdentifier("1")
            .setAppType(AppType.HUMAN_INTERVENTION)
    );

    //  verify that it shows up as running after initialization
    assertTrue(ApplicationController.isRunning(new DatabasesImpl().getRlDb(), AppType.HUMAN_INTERVENTION, "1"));

    //  prove we can use it to do stuff when creating steps
    Step step = new Step(new SetStep(
        "step",
        workflowID,
        workflow.getInitializedPersistence().getExecutionId())
    );

    //  then run the workflow
    WorkflowRunner runner = new WorkflowRunner(workflow, Sets.<Step>newHashSet(step));
    runner.run();

    assertEquals(runner.getPersistence().getExecutionId(), workflowID.get());

  }

  @Test
  public void testStepCreationFailure() throws IOException, InterruptedException {

    //  initialize the workflow with a name and scope to get an execution ID
    InitializedWorkflow<InitializedDbPersistence> workflow = new DbPersistenceFactory().initialize(
        "Test Workflow",
        new TestWorkflowOptions()
            .setUniqueIdentifier("1")
    );

    //  pretend we failed before getting to new WorkflowRunner().run()
    //  we can't really test jvm shutdown behavior, so run the shutdown hook manually
    MultiShutdownHook hook = workflow.getShutdownHook();
    hook.start();
    hook.join();

    InitializedDbPersistence dbInitialized = workflow.getInitializedPersistence();

    //  verify that the shutdown hook failed the attempt
    assertEquals(WorkflowExecutionStatus.INCOMPLETE.ordinal(), dbInitialized.getExecution().getStatus());
    assertEquals(WorkflowAttemptStatus.FAILED.ordinal(), dbInitialized.getAttempt().getStatus().intValue());

    //  try again
    workflow = new DbPersistenceFactory().initialize(
        "Test Workflow",
        new TestWorkflowOptions()
            .setUniqueIdentifier("1")
    );

    Step step = new Step(new NoOpAction("no-op"));

    WorkflowRunner runner = new WorkflowRunner(workflow, Sets.<Step>newHashSet(step));
    runner.run();

    dbInitialized = workflow.getInitializedPersistence();

    //  verify that the shutdown hook failed the attempt
    assertEquals(WorkflowExecutionStatus.COMPLETE.ordinal(), dbInitialized.getExecution().getStatus());
    assertEquals(WorkflowAttemptStatus.FINISHED.ordinal(), dbInitialized.getAttempt().getStatus().intValue());
    assertEquals(StepStatus.COMPLETED, runner.getPersistence().getStepStatuses().get("no-op"));

  }


  private static class SetStep extends Action {

    private final AtomicLong value;
    private final long executionID;

    public SetStep(String checkpointToken,
                   AtomicLong value,
                   long executionID) {
      super(checkpointToken);
      this.value = value;
      this.executionID = executionID;
    }

    @Override
    protected void execute() throws Exception {
      this.value.set(executionID);
    }
  }

  public static class DelayAction extends Action {

    public DelayAction(String checkpointToken) {
      super(checkpointToken);
    }

    @Override
    protected void execute() throws Exception {
      Thread.sleep(1000);
    }
  }

  public static class StatAction extends Action {

    private final TupleDataStore input;

    public StatAction(String checkpointToken, TupleDataStore input) {
      super(checkpointToken);
      this.input = input;
    }

    @Override
    protected void execute() throws Exception {
      completeWithProgress(buildFlow().connect(
          input.getTap(),
          new NullTap(),
          new Each(new Pipe("pipe"), new TempDelay())
      ));
    }

    private static class TempDelay extends BaseOperation implements Filter {

      @Override
      public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          //  wait
        }
        return false;
      }
    }
  }

  public static class CopyStore extends Action {

    private final DataStore input;
    private final DataStore output;

    public CopyStore(String checkpointToken,
                     DataStore input,
                     DataStore output) {
      super(checkpointToken);
      this.input = input;
      this.output = output;

      readsFrom(input);
      creates(output);
    }

    @Override
    protected void execute() throws Exception {
      completeWithProgress(buildFlow().connect(input.getTap(), output.getTap(), new Pipe("flow")));
    }
  }


  private static void hideComplete(Flow f) {
    f.complete();
  }

  public static class IncrementCounter2 extends Action {

    private final TupleDataStore in;
    private final TupleDataStore out;

    public IncrementCounter2(String checkpointToken, String tmpRoot,
                             TupleDataStore in,
                             TupleDataStore out) {
      super(checkpointToken, tmpRoot);

      this.in = in;
      this.out = out;

      readsFrom(in);
      creates(out);

    }

    @Override
    protected void execute() throws Exception {

      Pipe pipe = new Pipe("input");
      pipe = new Each(pipe, new Count());

      Flow flow = flowConnector().connect(in.getTap(), out.getTap(), pipe);
      flow.addStepListener(new JobRecordListener(getPersister(), true));
      hideComplete(flow);

      //  make sure counter from previous step is accessible
      assertEquals(1L, (long)getFlatCounters().get("CUSTOM_COUNTER", "NAME"));

    }

    private static class Count extends BaseOperation implements Filter {
      @Override
      public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        flowProcess.increment("CUSTOM_COUNTER2", "NAME", 1);
        flowProcess.increment("OTHER_COUNTER2", "NAME", 1);
        return false;
      }
    }
  }


  public static class IncrementCounter extends CascadingAction2 {

    public IncrementCounter(String checkpointToken, String tmpRoot,
                            TupleDataStore in,
                            TupleDataStore out) {
      super(checkpointToken, tmpRoot);

      Pipe pipe = bindSource("input", in);
      pipe = new Each(pipe, new Count());
      complete("step", pipe, out);

    }

    private static class Count extends BaseOperation implements Filter {
      @Override
      public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        flowProcess.increment("CUSTOM_COUNTER", "NAME", 1);
        flowProcess.increment("OTHER_COUNTER", "NAME", 1);
        return false;
      }
    }
  }


  public static class SetResource extends Action {

    private final OldResource<Integer> res;

    public SetResource(String checkpointToken,
                       OldResource<Integer> res1) {
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

    private final OldResource<Integer> res;

    private final OldResource<Integer> resOut;
    private final TupleDataStore tupOut;

    public ConsumeResource(String checkpointToken,
                           String tmpRoot,
                           OldResource<Integer> res1) throws IOException {
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
