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
import com.liveramp.commons.state.TaskFailure;
import com.liveramp.commons.state.TaskSummary;
import com.liveramp.db_utils.BaseJackUtil;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.MailBuffer;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.workflow_state.json.WorkflowJSON;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.ConfiguredNotification;
import com.rapleaf.db_schemas.rldb.models.MapreduceCounter;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.models.MapreduceJobTaskException;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.StepAttemptDatastore;
import com.rapleaf.db_schemas.rldb.models.StepDependency;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttemptDatastore;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;

public class DbPersistence implements WorkflowStatePersistence {
  private static final Logger LOG = LoggerFactory.getLogger(DbPersistence.class);

  public static final long HEARTBEAT_INTERVAL = 15 * 1000; //  15s
  public static final int NUM_HEARTBEAT_TIMEOUTS = 4;  //  if an attempt misses 4 heartbeats, assume it is dead

  private final MailBuffer testMailBuffer;
  private final InitializedDbPersistence init;
  private final Object lock;

  public static DbPersistence queryPersistence(long workflowAttemptId, IRlDb rldb) {
    return new DbPersistence(new InitializedDbPersistence(workflowAttemptId, rldb, false, null));
  }

  public DbPersistence(InitializedDbPersistence initialization) {

    this.testMailBuffer = new MailBuffer.ListBuffer();
    this.init = initialization;
    this.lock = init.getLock();

  }

  private StepAttempt getStep(String stepToken) throws IOException {
    synchronized (lock) {
      return Accessors.only(init.getDb().stepAttempts().query()
          .workflowAttemptId((int)init.getAttemptId())
          .stepToken(stepToken)
          .find());
    }
  }

  private void update(StepAttempt attempt, MapBuilder<StepAttempt._Fields, Object> valuesBuilder) throws IOException {
    synchronized (lock) {

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

      init.getDb().stepAttempts().save(attempt);

    }
  }

  private MapBuilder<StepAttempt._Fields, Object> stepFields() {
    return new MapBuilder<StepAttempt._Fields, Object>();
  }

  @Override
  public void markStepRunning(String stepToken) throws IOException {
    synchronized (lock) {

      LOG.info("Marking step " + stepToken + " as running");
      update(getStep(stepToken), stepFields()
          .put(StepAttempt._Fields.step_status, StepStatus.RUNNING.ordinal())
          .put(StepAttempt._Fields.start_time, System.currentTimeMillis())
      );

    }
  }

  @Override
  public void markStepFailed(String stepToken, Throwable t) throws IOException {
    synchronized (lock) {

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

      init.save(init.getAttempt().setStatus(AttemptStatus.FAIL_PENDING.ordinal()));

    }
  }

  @Override
  public void markStepCompleted(String stepToken) throws IOException {
    synchronized (lock) {

      LOG.info("Marking step " + stepToken + " as completed");

      update(getStep(stepToken), stepFields()
          .put(StepAttempt._Fields.step_status, StepStatus.COMPLETED.ordinal())
          .put(StepAttempt._Fields.end_time, System.currentTimeMillis()));

    }
  }

  @Override
  public void markStepReverted(String stepToken) throws IOException {
    synchronized (lock) {

      LOG.info("Marking step " + stepToken + " as reverted");

      WorkflowExecution execution = init.getExecution();

      //  verify this is the latest execution
      //  verify workflow attempt not running
      //  can't cancel attempt already cancelled or finished
      Assertions.assertCanRevert(init.getDb(), execution);

      update(getStep(stepToken), stepFields()
          .put(StepAttempt._Fields.step_status, StepStatus.REVERTED.ordinal())
      );

      //  set execution to not complete
      init.save(execution.setStatus(WorkflowExecutionStatus.INCOMPLETE.ordinal()));

    }
  }

  @Override
  public void markStepStatusMessage(String stepToken, String newMessage) throws IOException {
    synchronized (lock) {

      LOG.info("Marking step status message: " + stepToken + " message " + newMessage);

      update(getStep(stepToken), stepFields()
          .put(StepAttempt._Fields.status_message, StringUtils.substring(newMessage, 0, 255))
      );

    }
  }

  @Override
  public void markStepRunningJob(String stepToken, String jobId, String jobName, String trackingURL) throws IOException {
    synchronized (lock) {

      StepAttempt step = getStep(stepToken);
      IRlDb conn = init.getDb();

      List<MapreduceJob> saved = conn.mapreduceJobs().query()
          .stepAttemptId((int)step.getId())
          .jobIdentifier(jobId)
          .find();

      if (saved.isEmpty()) {
        LOG.info("Marking step " + stepToken + " as running job " + jobId);
        MapreduceJob job = conn.mapreduceJobs().create(
            jobId,
            jobName,
            trackingURL
        );
        job.setStepAttemptId((int)step.getId()).save();
      }

    }
  }

  @Override
  public void markJobCounters(String stepToken, String jobId, TwoNestedMap<String, String, Long> values) throws IOException {
    synchronized (lock) {

      //  if job failed in setup, don't try to get the job, won't exist.  nothing to record.
      if (values.isEmpty()) {
        return;
      }

      StepAttempt step = getStep(stepToken);
      MapreduceJob job = getMapreduceJob(jobId, step);
      IRlDb conn = init.getDb();

      for (TwoNestedMap.Entry<String, String, Long> value : values) {
        conn.mapreduceCounters().create(
            (int)job.getId(),
            value.getK1(),
            value.getK2(),
            value.getValue()
        );
      }

    }
  }

  @Override
  public void markJobTaskInfo(String stepToken, String jobId, TaskSummary info) throws IOException {
    synchronized (lock) {

      try {

        StepAttempt step = getStep(stepToken);
        MapreduceJob job = getMapreduceJob(jobId, step);
        IRlDb conn = init.getDb();

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

        for (TaskFailure taskFailure : info.getTaskFailures()) {
          conn.mapreduceJobTaskExceptions().create(
              (int)job.getId(),
              taskFailure.getTaskAttemptID(),
              taskFailure.getError());
        }
      }
      //  don't want to leave this in forever, debugging transient error on setup failure  (figure out why jobID is here which isn't recorded earlier)
      catch (Exception e) {
        throw new RuntimeException("Error recording job task info for jobID " + jobId, e);
      }
    }
  }

  private MapreduceJob getMapreduceJob(String jobId, StepAttempt step) throws IOException {
    IRlDb conn = init.getDb();
    return Accessors.only(conn.mapreduceJobs().query()
        .stepAttemptId((int)step.getId())
        .jobIdentifier(jobId)
        .find());
  }

  @Override
  public void markWorkflowStarted() throws IOException {
    synchronized (lock) {

      LOG.info("Starting attempt: " + init.getAttempt());
      init.save(init.getAttempt()
          .setStatus(AttemptStatus.RUNNING.ordinal())
          .setStartTime(System.currentTimeMillis())
      );

    }
  }

  @Override
  public void markPool(String pool) throws IOException {
    synchronized (lock) {
      WorkflowAttempt attempt = init.getAttempt();
      Assertions.assertLive(attempt);

      init.save(init.getExecution()
          .setPoolOverride(pool)
      );

    }
  }

  @Override
  public void markPriority(String priority) throws IOException {
    synchronized (lock) {

      WorkflowAttempt attempt = init.getAttempt();
      Assertions.assertLive(attempt);

      LOG.info("Setting priority: " + priority);
      init.save(attempt
          .setPriority(priority)
      );

    }
  }

  @Override
  public void markShutdownRequested(String providedReason) throws IOException {
    synchronized (lock) {
      WorkflowAttempt attempt = init.getAttempt();
      Assertions.assertLive(attempt);

      String reason = WorkflowJSON.getShutdownReason(providedReason);
      LOG.info("Processing shutdown request: " + reason);

      attempt.setShutdownReason(reason);

      //  don't override fail pending (is there a better way?)
      if (attempt.getStatus() != AttemptStatus.FAIL_PENDING.ordinal()) {
        attempt.setStatus(AttemptStatus.SHUTDOWN_PENDING.ordinal());
      }

      init.save(attempt);

    }
  }

  @Override
  public void markWorkflowStopped() throws IOException {
    init.markWorkflowStopped();
  }

  @Override
  public StepStatus getStatus(String stepToken) throws IOException {
    synchronized (lock) {
      return WorkflowQueries.getStepStatuses(init.getDb(), init.getAttemptId(), stepToken).get(stepToken);
    }
  }

  @Override
  public Map<String, StepState> getStepStates() throws IOException {
    synchronized (lock) {
      return getStates();
    }
  }

  @Override
  public Map<String, StepStatus> getStepStatuses() throws IOException {
    return WorkflowQueries.getStepStatuses(init.getDb(), init.getAttemptId(), null);
  }

  private Map<String, StepState> getStates() throws IOException {
    synchronized (lock) {

      Map<Long, StepAttempt.Attributes> attemptsById = Maps.newHashMap();
      IRlDb conn = init.getDb();

      List<StepAttempt.Attributes> attempts = WorkflowQueries.getStepAttempts(conn,
          init.getAttemptId(),
          null
      );

      for (StepAttempt.Attributes attempt : attempts) {
        attemptsById.put(attempt.getId(), attempt);
      }

      List<MapreduceJob.Attributes> mapreduceJobs = WorkflowQueries.getMapreduceJobs(conn,
          attemptsById.keySet()
      );

      Set<Long> jobIds = Sets.newHashSet();
      Multimap<Long, MapreduceJob.Attributes> jobsByStepId = HashMultimap.create();
      for (MapreduceJob.Attributes mapreduceJob : mapreduceJobs) {
        jobsByStepId.put((long)mapreduceJob.getStepAttemptId(), mapreduceJob);
        jobIds.add(mapreduceJob.getId());
      }

      List<MapreduceCounter.Attributes> counters = WorkflowQueries.getMapreduceCounters(conn,
          jobIds
      );

      Multimap<Long, MapreduceCounter.Attributes> countersByJobId = HashMultimap.create();
      for (MapreduceCounter.Attributes counter : counters) {
        countersByJobId.put((long)counter.getMapreduceJobId(), counter);
      }

      List<StepAttemptDatastore.Attributes> storeUsages = WorkflowQueries.getStepAttemptDatastores(conn,
          attemptsById.keySet()
      );

      Set<Long> allStores = Sets.newHashSet();
      for (StepAttemptDatastore.Attributes storeUsage : storeUsages) {
        allStores.add((long)storeUsage.getWorkflowAttemptDatastoreId());
      }

      Map<Long, WorkflowAttemptDatastore.Attributes> storesById = BaseJackUtil.attributesById(WorkflowQueries.getWorkflowAttemptDatastores(conn, allStores, null));
      TwoNestedMap<String, DSAction, WorkflowAttemptDatastore.Attributes> stepToDatastoreUsages = new TwoNestedMap<>();
      for (StepAttemptDatastore.Attributes usage : storeUsages) {
        stepToDatastoreUsages.put(
            attemptsById.get((long)usage.getStepAttemptId()).getStepToken(),
            DSAction.findByValue(usage.getDsAction()),
            storesById.get((long)usage.getWorkflowAttemptDatastoreId())
        );

      }

      Multimap<String, String> stepToDependencies = HashMultimap.create();
      for (StepDependency.Attributes dependency : WorkflowQueries.getStepDependencies(conn, attemptsById.keySet())) {
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

          List<TaskFailure> taskFailureList = Lists.newArrayList();

          for (MapreduceJobTaskException exception : conn.mapreduceJobTaskExceptions().findByMapreduceJobId(job.getIntId())) {
            taskFailureList.add(new TaskFailure(exception.getTaskAttemptId(), exception.getException()));
          }

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
                  job.getStdevReduceDuration(),
                  taskFailureList
              ),
              counterList
          ));

        }

        states.put(token, state);

      }

      return states;
    }
  }

  @Override
  public String getShutdownRequest() throws IOException {
    synchronized (lock) {
      return init.getAttempt().getShutdownReason();
    }
  }

  @Override
  public String getPriority() throws IOException {
    synchronized (lock) {
      return init.getAttempt().getPriority();
    }
  }

  @Override
  public String getPool() throws IOException {
    synchronized (lock) {
      return WorkflowQueries.getPool(init.getAttempt(), init.getExecution());
    }
  }

  @Override
  public String getName() throws IOException {
    synchronized (lock) {
      return init.getExecution().getApplication().getName();
    }
  }

  @Override
  public String getScopeIdentifier() throws IOException {
    synchronized (lock) {
      return init.getExecution().getScopeIdentifier();
    }
  }

  @Override
  public String getId() throws IOException {
    synchronized (lock) {
      return Long.toString(init.getAttempt().getId());
    }
  }

  @Override
  public AttemptStatus getStatus() throws IOException {
    synchronized (lock) {
      return AttemptStatus.findByValue(init.getAttempt().getStatus());
    }
  }


  @Override
  public List<AlertsHandler> getRecipients(WorkflowRunnerNotification notification) throws IOException {
    synchronized (lock) {

      List<ConfiguredNotification.Attributes> allNotifications = Lists.newArrayList();
      IRlDb conn = init.getDb();

      allNotifications.addAll(WorkflowQueries.getAttemptNotifications(conn,
          notification,
          init.getAttemptId()
      ));

      allNotifications.addAll(WorkflowQueries.getExecutionNotifications(conn,
          getExecutionId(),
          notification
      ));

      allNotifications.addAll(WorkflowQueries.getApplicationNotifications(conn,
          init.getExecution().getApplicationId().longValue(),
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
        AlertsHandler providedHandler = init.getProvidedHandler();
        if (providedHandler == null) {
          throw new RuntimeException("Provided alerts handler not available for notification " + notification);
        }

        handlers.add(providedHandler);
      }

      if (!emailsToAlert.isEmpty()) {

        AlertsHandlers.Builder builder = AlertsHandlers.builder(TeamList.NULL)  // won't actually get used
            .setTestMailBuffer(testMailBuffer)
            .setEngineeringRecipient(AlertRecipients.of(emailsToAlert));

        handlers.add(builder.build());
      }

      return handlers;

    }
  }

  @Override
  public ThreeNestedMap<String, String, String, Long> getCountersByStep() throws IOException {
    synchronized (lock) {
      return WorkflowQueries.getCountersByStep(init.getDb(), getExecutionId());
    }
  }

  @Override
  public TwoNestedMap<String, String, Long> getFlatCounters() throws IOException {
    synchronized (lock) {
      return WorkflowQueries.getFlatCounters(init.getDb(), getExecutionId());
    }
  }

  @Override
  public long getExecutionId() throws IOException {
    synchronized (lock) {
      return init.getExecution().getId();
    }
  }

  @Override
  public long getAttemptId() throws IOException {
    synchronized (lock) {
      return init.getAttemptId();
    }
  }

  //  for testing

  public MailBuffer getTestMailBuffer() {
    return testMailBuffer;
  }


}
