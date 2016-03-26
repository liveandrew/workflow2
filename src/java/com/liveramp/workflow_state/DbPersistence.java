package com.liveramp.workflow_state;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.Accessors;
import com.liveramp.commons.collections.map.MapBuilder;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.state.LaunchedJob;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.MailBuffer;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.java_support.workflow.TaskSummary;
import com.liveramp.workflow_state.json.WorkflowJSON;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.ConfiguredNotification;
import com.rapleaf.db_schemas.rldb.models.MapreduceCounter;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.StepAttemptDatastore;
import com.rapleaf.db_schemas.rldb.models.StepDependency;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttemptDatastore;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.util.JackUtil;

public class DbPersistence implements WorkflowStatePersistence {
  private static final Logger LOG = LoggerFactory.getLogger(DbPersistence.class);

  public static final long HEARTBEAT_INTERVAL = 15 * 1000; //  15s
  public static final int NUM_HEARTBEAT_TIMEOUTS = 4;  //  if an attempt misses 4 heartbeats, assume it is dead

  private final IRlDb rldb;
  private final long workflowAttemptId;

  private final Thread heartbeatThread;
  private final AlertsHandler providedHandler;
  private final MailBuffer testMailBuffer;

  //  TODO I don't like the the boolean or alerts handler which are only sometimes set.
  //  this should get split into separate interfaces for run persistence, query persistence, and controller persistence.
  //  ideally, WorkflowOptions could get helpers added so that people don't actually pass in an AlertsHandler, they pass in
  //  a config or builder which the persistence can either directly instantiate (hdfs) or store configuration  of in the db so it can
  //  construct it later when it has to send messages.

  public static DbPersistence runPersistence(long workflowAttemptId,
                                             AlertsHandler providedHandler) {
    return new DbPersistence(workflowAttemptId, new DatabasesImpl().getRlDb(), true, providedHandler);
  }

  public static DbPersistence queryPersistence(long workflowAttemptId, IRlDb rldb) {
    return new DbPersistence(workflowAttemptId, rldb, false, null);
  }

  private DbPersistence(long workflowAttemptId, IRlDb rldb, boolean runMode, AlertsHandler providedHandler) {
    this.rldb = rldb;
    this.rldb.disableCaching();
    this.workflowAttemptId = workflowAttemptId;
    this.testMailBuffer = new MailBuffer.ListBuffer();

    if (runMode) {
      this.heartbeatThread = new Thread(new Heartbeat());
      this.heartbeatThread.setDaemon(true);
      this.heartbeatThread.start();
      this.providedHandler = providedHandler;
    } else {
      this.heartbeatThread = null;
      this.providedHandler = null;
    }
  }

  private StepAttempt getStep(String stepToken) throws IOException {
    return Accessors.only(rldb.stepAttempts().query()
        .workflowAttemptId((int)workflowAttemptId)
        .stepToken(stepToken)
        .find());
  }

  private WorkflowAttempt getAttempt() throws IOException {
    return rldb.workflowAttempts().find(workflowAttemptId);
  }

  private WorkflowExecution getExecution() throws IOException {
    return getAttempt().getWorkflowExecution();
  }

  private synchronized void save(WorkflowExecution execution) throws IOException {
    rldb.workflowExecutions().save(execution);
  }

  private synchronized void save(WorkflowAttempt attempt) throws IOException {
    rldb.workflowAttempts().save(attempt);
  }

  private synchronized void update(StepAttempt attempt, MapBuilder<StepAttempt._Fields, Object> valuesBuilder) throws IOException {

    Map<StepAttempt._Fields, Object> values = valuesBuilder.get();

    if (values.containsKey(StepAttempt._Fields.step_status)) {
      StepStatus newStatus = StepStatus.findByValue((Integer)values.get(StepAttempt._Fields.step_status));
      StepStatus current = StepStatus.findByValue(attempt.getStepStatus());

      if (!StepStatus.VALID_TRANSITIONS.get(current).contains(newStatus)) {
        throw new RuntimeException("Cannot move step " + attempt + "from status " + current + " to " + newStatus + "!");
      }

    }

    for (Map.Entry<StepAttempt._Fields, Object> value : values.entrySet()) {
      attempt.setField(value.getKey(), value.getValue());
    }

    rldb.stepAttempts().save(attempt);

  }

  private MapBuilder<StepAttempt._Fields, Object> stepFields() {
    return new MapBuilder<StepAttempt._Fields, Object>();
  }

  @Override
  public synchronized void markStepRunning(String stepToken) throws IOException {
    LOG.info("Marking step " + stepToken + " as running");

    update(getStep(stepToken), stepFields()
        .put(StepAttempt._Fields.step_status, StepStatus.RUNNING.ordinal())
        .put(StepAttempt._Fields.start_time, System.currentTimeMillis())
    );

  }

  @Override
  public synchronized void markStepFailed(String stepToken, Throwable t) throws IOException {
    LOG.info("Marking step " + stepToken + " as failed");

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);

    StepAttempt step = getStep(stepToken);

    update(step, stepFields()
        .put(StepAttempt._Fields.failure_cause, StringUtils.substring(t.getMessage(), 0, 255))
        .put(StepAttempt._Fields.failure_trace, StringUtils.substring(sw.toString(), 0, 10000))
        .put(StepAttempt._Fields.step_status, StepStatus.FAILED.ordinal())
        .put(StepAttempt._Fields.end_time, System.currentTimeMillis())
    );

    save(getAttempt()
        .setStatus(AttemptStatus.FAIL_PENDING.ordinal())
    );

  }

  @Override
  public synchronized void markStepCompleted(String stepToken) throws IOException {
    LOG.info("Marking step " + stepToken + " as completed");

    update(getStep(stepToken), stepFields()
        .put(StepAttempt._Fields.step_status, StepStatus.COMPLETED.ordinal())
        .put(StepAttempt._Fields.end_time, System.currentTimeMillis()));

  }

  @Override
  public synchronized void markStepReverted(String stepToken) throws IOException {
    LOG.info("Marking step " + stepToken + " as reverted");

    WorkflowExecution execution = getExecution();

    //  verify this is the latest execution
    //  verify workflow attempt not running
    //  can't cancel attempt already cancelled or finished
    Assertions.assertCanRevert(rldb, execution);

    update(getStep(stepToken), stepFields()
        .put(StepAttempt._Fields.step_status, StepStatus.REVERTED.ordinal())
    );

    //  set execution to not complete
    save(execution.setStatus(WorkflowExecutionStatus.INCOMPLETE.ordinal()));

  }

  @Override
  public synchronized void markStepStatusMessage(String stepToken, String newMessage) throws IOException {
    LOG.info("Marking step status message: " + stepToken + " message " + newMessage);

    update(getStep(stepToken), stepFields()
        .put(StepAttempt._Fields.status_message, StringUtils.substring(newMessage, 0, 255))
    );

  }

  @Override
  public synchronized void markStepRunningJob(String stepToken, String jobId, String jobName, String trackingURL) throws IOException {

    StepAttempt step = getStep(stepToken);

    List<MapreduceJob> saved = rldb.mapreduceJobs().query()
        .stepAttemptId((int)step.getId())
        .jobIdentifier(jobId)
        .find();

    if (saved.isEmpty()) {
      LOG.info("Marking step " + stepToken + " as running job " + jobId);
      MapreduceJob job = rldb.mapreduceJobs().create(
          jobId,
          jobName,
          trackingURL
      );
      job.setStepAttemptId((int)step.getId()).save();
    }

  }

  @Override
  public synchronized void markJobCounters(String stepToken, String jobId, TwoNestedMap<String, String, Long> values) throws IOException {

    //  if job failed in setup, don't try to get the job, won't exist.  nothing to record.
    if (values.isEmpty()) {
      return;
    }

    StepAttempt step = getStep(stepToken);
    MapreduceJob job = getMapreduceJob(jobId, step);

    for (TwoNestedMap.Entry<String, String, Long> value : values) {
      rldb.mapreduceCounters().create(
          (int)job.getId(),
          value.getK1(),
          value.getK2(),
          value.getValue()
      );
    }

  }

  @Override
  public synchronized void markJobTaskInfo(String stepToken, String jobId, TaskSummary info) throws IOException {

    try {

      StepAttempt step = getStep(stepToken);
      MapreduceJob job = getMapreduceJob(jobId, step);

      job
          //  map
          .setAvgMapDuration(info.getAvgMapDuration())
          .setMedianMapDuration(info.getMedianMapDuration())
          .setMaxMapDuration(info.getMaxMapDuration())
          .setMinMapDuration(info.getMinMapDuration())
          .setStdevMapDuration(info.getMapDurationStDev())
          //  reduce
          .setAvgReduceDuration(info.getAvgReduceDuration())
          .setMedianReduceDuration(info.getMedianReduceDuration())
          .setMaxReduceDuration(info.getMaxReduceDuration())
          .setMinReduceDuration(info.getMinReduceDuration())
          .setStdevReduceDuration(info.getReduceDurationStDev())

          .save();

    }
    //  don't want to leave this in forever, debugging transient error on setup failure  (figure out why jobID is here which isn't recorded earlier)
    catch (Exception e) {
      throw new RuntimeException("Error recording job task info for jobID " + jobId, e);
    }
  }

  private MapreduceJob getMapreduceJob(String jobId, StepAttempt step) throws IOException {
    return Accessors.only(rldb.mapreduceJobs().query()
        .stepAttemptId((int)step.getId())
        .jobIdentifier(jobId)
        .find());
  }


  @Override
  public synchronized void markWorkflowStarted() throws IOException {

    LOG.info("Starting attempt: " + getAttempt());
    save(getAttempt()
        .setStatus(AttemptStatus.RUNNING.ordinal())
        .setStartTime(System.currentTimeMillis())
    );

  }

  @Override
  public synchronized void markPool(String pool) throws IOException {

    WorkflowAttempt attempt = getAttempt();
    Assertions.assertLive(attempt);

    save(getExecution()
        .setPoolOverride(pool)
    );

  }

  @Override
  public synchronized void markPriority(String priority) throws IOException {

    WorkflowAttempt attempt = getAttempt();
    Assertions.assertLive(attempt);

    LOG.info("Setting priority: " + priority);
    save(attempt
        .setPriority(priority)
    );

  }

  @Override
  public synchronized void markShutdownRequested(String providedReason) throws IOException {

    WorkflowAttempt attempt = getAttempt();
    Assertions.assertLive(attempt);

    String reason = WorkflowJSON.getShutdownReason(providedReason);
    LOG.info("Processing shutdown request: " + reason);

    attempt.setShutdownReason(reason);

    //  don't override fail pending (is there a better way?)
    if (attempt.getStatus() != AttemptStatus.FAIL_PENDING.ordinal()) {
      attempt.setStatus(AttemptStatus.SHUTDOWN_PENDING.ordinal());
    }

    save(attempt);

  }

  //  this method is carefully not synchronized, because we don't want a deadlock with the heartbeat waiting to heartbeat.
  @Override
  public void markWorkflowStopped() throws IOException {

    synchronized (this) {
      LOG.info("Marking workflow stopped");

      if (WorkflowQueries.workflowComplete(getExecution())) {
        LOG.info("Marking execution as complete");
        save(getExecution()
            .setStatus(WorkflowExecutionStatus.COMPLETE.ordinal())
            .setEndTime(System.currentTimeMillis())
        );
      }

      WorkflowAttempt attempt = getAttempt()
          .setEndTime(System.currentTimeMillis());

      LOG.info("Stopping attempt: " + attempt);
      if (attempt.getStatus() == AttemptStatus.FAIL_PENDING.ordinal()) {
        attempt.setStatus(AttemptStatus.FAILED.ordinal());
      } else if (attempt.getStatus() == AttemptStatus.SHUTDOWN_PENDING.ordinal()) {
        attempt.setStatus(AttemptStatus.SHUTDOWN.ordinal());
      } else if (attempt.getStatus() == AttemptStatus.FAILED.ordinal()) {
        LOG.info("Attempt was already stopped (via shutdown hook probably)");
      } else {
        attempt.setStatus(AttemptStatus.FINISHED.ordinal());
      }

      LOG.info("Marking status of attempt " + attempt.getId() + " as " + attempt.getStatus());

      save(attempt);

    }

    heartbeatThread.interrupt();
    try {
      heartbeatThread.join();
    } catch (InterruptedException e) {
      LOG.error("Failed to interrupt heartbeat thread!");
    }

  }

  @Override
  public synchronized StepStatus getStatus(String stepToken) throws IOException {
    return StepStatus.findByValue(getStep(stepToken).getStepStatus());
  }

  @Override
  public synchronized Map<String, StepState> getStepStatuses() throws IOException {
    return getStates(null);
  }

  private synchronized Map<String, StepState> getStates(String specificToken) throws IOException {

    long workflowAttemptId = getAttemptId();

    List<StepAttempt.Attributes> attempts = WorkflowQueries.getStepAttempts(rldb,
        workflowAttemptId,
        specificToken
    );

    Map<Long, StepAttempt.Attributes> attemptsById = Maps.newHashMap();
    for (StepAttempt.Attributes attempt : attempts) {
      attemptsById.put(attempt.getId(), attempt);
    }

    List<MapreduceJob.Attributes> mapreduceJobs = WorkflowQueries.getMapreduceJobs(rldb,
        attemptsById.keySet()
    );

    Set<Long> jobIds = Sets.newHashSet();
    Multimap<Long, MapreduceJob.Attributes> jobsByStepId = HashMultimap.create();
    for (MapreduceJob.Attributes mapreduceJob : mapreduceJobs) {
      jobsByStepId.put((long)mapreduceJob.getStepAttemptId(), mapreduceJob);
      jobIds.add(mapreduceJob.getId());
    }

    List<MapreduceCounter.Attributes> counters = WorkflowQueries.getMapreduceCounters(rldb,
        jobIds
    );

    Multimap<Long, MapreduceCounter.Attributes> countersByJobId = HashMultimap.create();
    for (MapreduceCounter.Attributes counter : counters) {
      countersByJobId.put((long)counter.getMapreduceJobId(), counter);
    }

    List<StepAttemptDatastore.Attributes> storeUsages = WorkflowQueries.getStepAttemptDatastores(rldb,
        attemptsById.keySet()
    );

    Set<Long> allStores = Sets.newHashSet();
    for (StepAttemptDatastore.Attributes storeUsage : storeUsages) {
      allStores.add((long)storeUsage.getWorkflowAttemptDatastoreId());
    }

    Map<Long, WorkflowAttemptDatastore.Attributes> storesById = JackUtil.attributesById(WorkflowQueries.getWorkflowAttemptDatastores(rldb, allStores, null));
    TwoNestedMap<String, DSAction, WorkflowAttemptDatastore.Attributes> stepToDatastoreUsages = new TwoNestedMap<>();
    for (StepAttemptDatastore.Attributes usage : storeUsages) {
      stepToDatastoreUsages.put(
          attemptsById.get((long)usage.getStepAttemptId()).getStepToken(),
          DSAction.findByValue(usage.getDsAction()),
          storesById.get((long)usage.getWorkflowAttemptDatastoreId())
      );

    }

    Multimap<String, String> stepToDependencies = HashMultimap.create();
    for (StepDependency.Attributes dependency : WorkflowQueries.getStepDependencies(rldb, attemptsById.keySet())) {
      stepToDependencies.put(
          attemptsById.get((long)dependency.getStepAttemptId()).getStepToken(),
          attemptsById.get((long)dependency.getDependencyAttemptId()).getStepToken()
      );
    }

    Map<String, StepState> states = Maps.newHashMap();

    for (StepAttempt.Attributes attempt : attempts) {

      Multimap<DSAction, DataStoreInfo> infoMap = HashMultimap.create();
      for (Map.Entry<DSAction, WorkflowAttemptDatastore.Attributes> entry : stepToDatastoreUsages.get(attempt.getStepToken()).entrySet()) {
        WorkflowAttemptDatastore.Attributes value = entry.getValue();
        infoMap.put(entry.getKey(), new DataStoreInfo(value.getName(), value.getClassName(), value.getPath(), value.getIntId()));
      }

      String token = attempt.getStepToken();
      StepState state = new StepState(
          token,
          StepStatus.findByValue(attempt.getStepStatus()),
          attempt.getActionClass(),
          Sets.newHashSet(stepToDependencies.get(token)),
          infoMap)
          .setFailureMessage(attempt.getFailureCause())
          .setFailureTrace(attempt.getFailureTrace())
          .setStatusMessage(attempt.getStatusMessage());

      if (attempt.getStartTime() != null) {
        state.setStartTimestamp(attempt.getStartTime());
      }

      if (attempt.getEndTime() != null) {
        state.setEndTimestamp(attempt.getEndTime());
      }

      for (MapreduceJob.Attributes job : jobsByStepId.get(attempt.getId())) {

        List<MapReduceJob.Counter> counterList = Lists.newArrayList();

        for (MapreduceCounter.Attributes attributes : countersByJobId.get(job.getId())) {
          counterList.add(new MapReduceJob.Counter(attributes.getGroup(), attributes.getName(), attributes.getValue()));
        }

        LaunchedJob launched = new LaunchedJob(job.getJobIdentifier(), job.getJobName(), job.getTrackingUrl());
        state.addMrjob(new MapReduceJob(launched,
            new TaskSummary(
                job.getAvgMapDuration(),
                job.getMedianMapDuration(),
                job.getMaxMapDuration(),
                job.getMinMapDuration(),
                job.getStdevMapDuration(),
                job.getAvgReduceDuration(),
                job.getMedianReduceDuration(),
                job.getMaxReduceDuration(),
                job.getMinReduceDuration(),
                job.getStdevReduceDuration()
            ),
            counterList
        ));

      }

      states.put(token, state);

    }

    return states;
  }

  @Override
  public synchronized String getShutdownRequest() throws IOException {
    return getAttempt().getShutdownReason();
  }

  @Override
  public synchronized String getPriority() throws IOException {
    return getAttempt().getPriority();
  }

  @Override
  public synchronized String getPool() throws IOException {
    return WorkflowQueries.getPool(getAttempt(), getExecution());
  }

  @Override
  public synchronized String getName() throws IOException {
    return getExecution().getApplication().getName();
  }

  @Override
  public synchronized String getScopeIdentifier() throws IOException {
    return getExecution().getScopeIdentifier();
  }

  @Override
  public synchronized String getId() throws IOException {
    return Long.toString(getAttempt().getId());
  }

  @Override
  public synchronized AttemptStatus getStatus() throws IOException {
    return AttemptStatus.findByValue(getAttempt().getStatus());
  }

  @Override
  public synchronized List<AlertsHandler> getRecipients(WorkflowRunnerNotification notification) throws IOException {

    List<ConfiguredNotification.Attributes> allNotifications = Lists.newArrayList();

    allNotifications.addAll(WorkflowQueries.getAttemptNotifications(rldb,
        notification,
        workflowAttemptId
    ));

    allNotifications.addAll(WorkflowQueries.getExecutionNotifications(rldb,
        getExecutionId(),
        notification
    ));

    allNotifications.addAll(WorkflowQueries.getApplicationNotifications(rldb,
        getExecution().getApplicationId().longValue(),
        notification
    ));

    Set<String> emailsToAlert = Sets.newHashSet();
    boolean useProvided = false;

    for (ConfiguredNotification.Attributes allNotification : allNotifications) {
      Boolean isProvided = allNotification.isProvidedAlertsHandler();
      if (isProvided != null && isProvided) {
        useProvided = true;
      }

      if (allNotification.getEmail() != null) {
        emailsToAlert.add(allNotification.getEmail());
      }
    }

    List<AlertsHandler> handlers = Lists.newArrayList();
    if (useProvided) {

      //  if they used other constructor (see note up top about how to fix this... maybe)
      if (providedHandler == null) {
        throw new RuntimeException("Provided alerts handler not available for notification " + notification);
      }

      handlers.add(providedHandler);
    }

    for (String email : emailsToAlert) {

      handlers.add(AlertsHandlers.builder(TeamList.NULL)
          .setEngineeringRecipient(AlertRecipients.of(email))
          .setTestMailBuffer(testMailBuffer)
          .build());

    }

    return handlers;

  }

  @Override
  public synchronized ThreeNestedMap<String, String, String, Long> getCountersByStep() throws IOException {
    return WorkflowQueries.getCountersByStep(rldb, getExecutionId());
  }

  @Override
  public synchronized TwoNestedMap<String, String, Long> getFlatCounters() throws IOException {
    return WorkflowQueries.getFlatCounters(rldb, getExecutionId());
  }

  @Override
  public synchronized long getExecutionId() throws IOException {
    return getExecution().getId();
  }

  @Override
  public synchronized long getAttemptId() throws IOException {
    return workflowAttemptId;
  }

  //  for testing

  public MailBuffer getTestMailBuffer() {
    return testMailBuffer;
  }

  private synchronized void heartbeat() {
    try {
      save(getAttempt()
          .setLastHeartbeat(System.currentTimeMillis())
      );
    } catch (IOException e) {
      try {
        markShutdownRequested("Requesting shutdown because heartbeat thread lost database connection: " + e.getMessage());
      } catch (IOException e1) {
        //  fine
      }
      throw new RuntimeException(e);
    }
  }

  private class Heartbeat implements Runnable {
    @Override
    public void run() {
      //noinspection InfiniteLoopStatement
      while (!Thread.interrupted()) {
        try {
          Thread.sleep(HEARTBEAT_INTERVAL);
          heartbeat();
        } catch (InterruptedException e) {
          LOG.info("Heartbeat thread killed");
          return;
        }
      }
    }
  }
}
