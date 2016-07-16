package com.liveramp.workflow_db_state;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.collections.comparators.ReverseComparator;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.Accessors;
import com.liveramp.commons.collections.map.NestedMultimap;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedCountingMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.databases.workflow_db.IWorkflowDb;
import com.liveramp.databases.workflow_db.models.Application;
import com.liveramp.databases.workflow_db.models.ApplicationConfiguredNotification;
import com.liveramp.databases.workflow_db.models.ApplicationCounterSummary;
import com.liveramp.databases.workflow_db.models.ConfiguredNotification;
import com.liveramp.databases.workflow_db.models.MapreduceCounter;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.databases.workflow_db.models.MapreduceJobTaskException;
import com.liveramp.databases.workflow_db.models.StepAttempt;
import com.liveramp.databases.workflow_db.models.StepAttemptDatastore;
import com.liveramp.databases.workflow_db.models.StepDependency;
import com.liveramp.databases.workflow_db.models.WorkflowAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowAttemptConfiguredNotification;
import com.liveramp.databases.workflow_db.models.WorkflowAttemptDatastore;
import com.liveramp.databases.workflow_db.models.WorkflowExecution;
import com.liveramp.databases.workflow_db.models.WorkflowExecutionConfiguredNotification;
import com.liveramp.importer.generated.AppType;
import com.liveramp.workflow.types.WorkflowExecutionStatus;
import com.liveramp.workflow_state.DSAction;
import com.liveramp.workflow_state.DbPersistence;
import com.liveramp.workflow_state.ProcessStatus;
import com.liveramp.workflow.types.StepStatus;
import com.liveramp.workflow_state.WorkflowEnums;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.rapleaf.jack.queries.Column;
import com.rapleaf.jack.queries.GenericQuery;
import com.rapleaf.jack.queries.QueryOrder;
import com.rapleaf.jack.queries.Record;
import com.rapleaf.jack.queries.Records;
import com.rapleaf.jack.queries.where_operators.Between;
import com.rapleaf.jack.queries.where_operators.In;
import com.rapleaf.jack.queries.where_operators.IsNull;

import static com.rapleaf.jack.queries.AggregatedColumn.COUNT;

public class WorkflowQueries {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowQueries.class);

  public static Optional<Application> getApplication(IWorkflowDb rldb, String name) throws IOException {
    return Accessors.firstOrAbsent(rldb.applications().findByName(name));
  }

  public static Set<WorkflowExecution.Attributes> getIncompleteExecutions(IWorkflowDb rldb, String name, String scopeId) throws IOException {
    Set<WorkflowExecution.Attributes> incompleteExecutions = Sets.newHashSet();
    Records records = rldb.createQuery()
        .from(Application.TBL)
        .innerJoin(WorkflowExecution.TBL)
        .on(Application.ID.equalTo(WorkflowExecution.APPLICATION_ID.as(Long.class)))
        .where(Application.NAME.equalTo(name))
        .where(WorkflowExecution.SCOPE_IDENTIFIER.equalTo(scopeId))
        .where(WorkflowExecution.STATUS.equalTo(WorkflowExecutionStatus.INCOMPLETE.ordinal()))
        .select(WorkflowExecution.TBL.getAllColumns())
        .fetch();

    for (Record record : records) {
      incompleteExecutions.add(record.getAttributes(WorkflowExecution.TBL));
    }

    if (incompleteExecutions.size() > 1) {
      throw new RuntimeException("Found multiple incomplete workflow executions for name: "+name+" scope: "+scopeId);
    }

    return incompleteExecutions;
  }

  public static WorkflowExecution getLatestExecution(IWorkflowDb db, String name, String scopeIdentifier) throws IOException {
    Records records = db.createQuery()
        .from(Application.TBL)
        .innerJoin(WorkflowExecution.TBL)
        .on(Application.ID.equalTo(WorkflowExecution.APPLICATION_ID.as(Long.class)))
        .where(Application.NAME.equalTo(name))
        .where(WorkflowExecution.SCOPE_IDENTIFIER.equalTo(scopeIdentifier))
        .orderBy(WorkflowExecution.ID, QueryOrder.DESC)
        .select(WorkflowExecution.TBL.getAllColumns())
        .limit(1)
        .fetch();

    if (records.isEmpty()) {
      return null;
    }


    return Accessors.only(records).getModel(WorkflowExecution.TBL, db.getDatabases());
  }

  public static Optional<WorkflowExecution> getLatestExecution(IWorkflowDb db, AppType type, String scopeIdentifier) throws IOException {
    Records records = db.createQuery()
        .from(Application.TBL)
        .innerJoin(WorkflowExecution.TBL)
        .on(Application.ID.equalTo(WorkflowExecution.APPLICATION_ID.as(Long.class)))
        .where(Application.APP_TYPE.equalTo(type.getValue()))
        .where(WorkflowExecution.SCOPE_IDENTIFIER.equalTo(scopeIdentifier))
        .orderBy(WorkflowExecution.ID, QueryOrder.DESC)
        .select(WorkflowExecution.TBL.getAllColumns())
        .limit(1)
        .fetch();

    if (records.isEmpty()) {
      return Optional.absent();
    }

    return Optional.of(Accessors.only(records).getModel(WorkflowExecution.TBL, db.getDatabases()));
  }

  //  TODO temporary for some scripts while stuff is getting migrated
  public static boolean hasExecution(IWorkflowDb db, AppType type, String scopeIdentifier) throws IOException {
    return !db.createQuery()
        .from(Application.TBL)
        .innerJoin(WorkflowExecution.TBL)
        .on(Application.ID.equalTo(WorkflowExecution.APPLICATION_ID.as(Long.class)))
        .where(Application.APP_TYPE.equalTo(type.getValue()))
        .where(WorkflowExecution.SCOPE_IDENTIFIER.equalTo(scopeIdentifier))
        .orderBy(WorkflowExecution.ID, QueryOrder.DESC)
        .limit(1)
        .fetch().isEmpty();
  }

  //  TODO temporary for some scripts while stuff is getting migrated
  public static boolean hasExecution(IWorkflowDb db, String name, String scopeIdentifier) throws IOException {
    return !db.createQuery()
        .from(Application.TBL)
        .innerJoin(WorkflowExecution.TBL)
        .on(Application.ID.equalTo(WorkflowExecution.APPLICATION_ID.as(Long.class)))
        .where(Application.NAME.equalTo(name))
        .where(WorkflowExecution.SCOPE_IDENTIFIER.equalTo(scopeIdentifier))
        .orderBy(WorkflowExecution.ID, QueryOrder.DESC)
        .limit(1)
        .fetch().isEmpty();
  }

  public static WorkflowExecutionStatus getLatestExecutionStatus(IWorkflowDb db, AppType appType, String scopeIdentifier) throws IOException {
    Optional<WorkflowExecution> execution = getLatestExecution(db, appType, scopeIdentifier);
    if (execution.isPresent()) {
      return WorkflowExecutionStatus.findByValue(execution.get().getStatus());
    } else {
      throw new IllegalStateException("No executions present for the supplied app and scope id");
    }
  }

  public static WorkflowExecutionStatus getLatestExecutionStatus(IWorkflowDb db, String name, String scopeIdentifier) throws IOException {
    WorkflowExecution execution = getLatestExecution(db, name, scopeIdentifier);
    if (execution == null) {
      return null;
    }
    return WorkflowExecutionStatus.findByValue(execution.getStatus());
  }

  public static boolean isStepComplete(String step, WorkflowExecution execution) throws IOException {
    return getCompletedStep(step, execution) != null;

  }

  public static StepAttempt getCompletedStep(String step, WorkflowExecution execution) throws IOException {

    Set<StepAttempt> matches = Sets.newHashSet();

    for (WorkflowAttempt attempts : execution.getWorkflowAttempt()) {
      for (StepAttempt stepAttempt : attempts.getStepAttempt()) {
        if (stepAttempt.getStepToken().equals(step) && stepAttempt.getStepStatus() == StepStatus.COMPLETED.ordinal()) {
          matches.add(stepAttempt);
        }
      }
    }

    if (matches.size() > 1) {
      throw new RuntimeException("Found multiple complete step attempts for a workflow attempt!");
    }

    if (matches.isEmpty()) {
      return null;
    }

    return Accessors.first(matches);

  }

  public static TwoNestedMap<String, String, Long> countersAsMap(Collection<MapreduceCounter> counters) {
    TwoNestedMap<String, String, Long> asMap = new TwoNestedMap<>();
    for (MapreduceCounter counter : counters) {
      asMap.put(counter.getGroup(), counter.getName(), counter.getValue());
    }
    return asMap;
  }

  public static Optional<WorkflowAttempt> getLatestAttemptOptional(WorkflowExecution execution) throws IOException {
    return getLatestAttemptOptional(execution.getWorkflowAttempt());
  }

  public static Optional<WorkflowAttempt> getLatestAttemptOptional(Collection<WorkflowAttempt> attempts) throws IOException {
    return Accessors.firstOrAbsent(getAttemptsDescending(attempts));
  }

  public static WorkflowAttempt getLatestAttempt(WorkflowExecution execution) throws IOException {
    return Accessors.first(getAttemptsDescending(execution.getWorkflowAttempt()));
  }

  public static List<WorkflowAttempt> getAttemptsDescending(Collection<WorkflowAttempt> attempts) throws IOException {

    List<WorkflowAttempt> attemptsList = Lists.newArrayList(attempts);
    Collections.sort(attemptsList, new ReverseComparator());

    return attemptsList;
  }

  public static String getPool(WorkflowAttempt attempt, WorkflowExecution execution) {

    String poolOverride = execution.getPoolOverride();
    if (poolOverride != null) {
      return poolOverride;
    }

    return attempt.getPool();
  }

  public static ProcessStatus getProcessStatus(WorkflowAttempt attempt, WorkflowExecution execution) throws IOException {
    return getProcessStatus(attempt, execution, DbPersistence.NUM_HEARTBEAT_TIMEOUTS);
  }

  //  it's dumb to require both but it lets us avoid extra db lookups
  public static ProcessStatus getProcessStatus(WorkflowAttempt attempt, WorkflowExecution execution, int missedHeartbeatsThreshold) throws IOException {
    return getProcessStatus(System.currentTimeMillis(), attempt, execution, missedHeartbeatsThreshold);
  }

  public static ProcessStatus getProcessStatus(long fetchTime, WorkflowAttempt attempt, WorkflowExecution execution, int missedHeartbeatsThreshold) {
    Long lastHeartbeat = attempt.getLastHeartbeat();

    Integer status = attempt.getStatus();

    if (!WorkflowEnums.LIVE_ATTEMPT_STATUSES.contains(status)) {
      return ProcessStatus.STOPPED;
    }

    //  assume dead (OOME killed, etc) if no heartbeat for 4x interval
    if (fetchTime - lastHeartbeat >
        missedHeartbeatsThreshold * DbPersistence.HEARTBEAT_INTERVAL) {

      //  let manual cleanup get rid of the timeout status
      if (execution.getStatus() == WorkflowExecutionStatus.CANCELLED.ordinal()) {
        return ProcessStatus.STOPPED;
      }

      return ProcessStatus.TIMED_OUT;
    }

    return ProcessStatus.ALIVE;

  }

  public static List<WorkflowExecution> getDiedUncleanExecutions(IDatabases databases, AppType app, long windowDays, int missedHeartbeatsThreshold) throws IOException {

    List<WorkflowExecution> executions = queryWorkflowExecutions(databases, null, null, null, null,
        System.currentTimeMillis() - windowDays * 24 * 60 * 60 * 1000, null, WorkflowExecutionStatus.INCOMPLETE, null
    );

    List<WorkflowExecution> dead = Lists.newArrayList();

    for (WorkflowExecution execution : executions) {
      Optional<WorkflowAttempt> attemptOptional = getLatestAttemptOptional(execution);

      if (attemptOptional.isPresent()) {
        if (getProcessStatus(attemptOptional.get(), execution, missedHeartbeatsThreshold) == ProcessStatus.TIMED_OUT) {
          if (app == null || execution.getApplication().getAppType().equals(app.getValue())) {
            dead.add(execution);
          }
        }
      }

    }

    return dead;
  }

  public static boolean workflowComplete(WorkflowExecution workflowExecution) throws IOException {

    for (StepAttempt attempt : getLatestAttempt(workflowExecution).getStepAttempt()) {
      if (!isStepComplete(attempt.getStepToken(), workflowExecution)) {
        return false;
      }
    }

    return true;
  }

  public static boolean isLatestExecution(IWorkflowDb db, WorkflowExecution execution) throws IOException {

    WorkflowExecution latestExecution = getLatestExecution(db,
        execution.getName(),
        execution.getScopeIdentifier()
    );

    if (latestExecution == null) {
      return true;
    }

    return latestExecution.getId() == execution.getId();
  }

  //  either steps or cancel
  public static boolean canRevert(IWorkflowDb db, WorkflowExecution execution) throws IOException {

    if (!isLatestExecution(db, execution)) {
      LOG.info("Execution is not latest");
      return false;
    }

    if (execution.getStatus() == WorkflowExecutionStatus.CANCELLED.ordinal()) {
      LOG.info("Execution is already cancelled");
      return false;
    }

    if (getProcessStatus(getLatestAttempt(execution), execution) == ProcessStatus.ALIVE) {
      LOG.info("Process is still alive");
      return false;
    }

    return true;
  }

  public static ThreeNestedMap<String, String, String, Long> getCountersByStep(IWorkflowDb db, Long workflowExecution) throws IOException {
    ThreeNestedMap<String, String, String, Long> counters = new ThreeNestedMap<>();

    for (Record record : getCompleteStepCounters(db, workflowExecution).select(StepAttempt.STEP_TOKEN, MapreduceCounter.GROUP, MapreduceCounter.NAME, MapreduceCounter.VALUE)
        .fetch()) {
      counters.put(record.getString(StepAttempt.STEP_TOKEN),
          record.getString(MapreduceCounter.GROUP),
          record.getString(MapreduceCounter.NAME),
          record.getLong(MapreduceCounter.VALUE)
      );
    }

    return counters;
  }

  public static TwoNestedMap<String, String, Long> getFlatCounters(IWorkflowDb db, Long workflowExecution) throws IOException {
    TwoNestedCountingMap<String, String> counters = new TwoNestedCountingMap<>(0l);

    for (Record record : getCompleteStepCounters(db, workflowExecution).select(MapreduceCounter.GROUP, MapreduceCounter.NAME, MapreduceCounter.VALUE)
        .fetch()) {
      counters.incrementAndGet(record.getString(MapreduceCounter.GROUP),
          record.getString(MapreduceCounter.NAME),
          record.getLong(MapreduceCounter.VALUE)
      );
    }

    return counters;
  }

  public static GenericQuery completeMapreduceJobQuery(IDatabases databases,
                                                       Long endedAfter,
                                                       Long endedBefore) {

    GenericQuery stepAttempts = databases.getWorkflowDb().createQuery().from(StepAttempt.TBL)
        .where(StepAttempt.END_TIME.isNotNull())
        .where(StepAttempt.STEP_STATUS.equalTo(StepStatus.COMPLETED.ordinal()));

    if (endedAfter != null) {
      stepAttempts = stepAttempts.where(StepAttempt.END_TIME.greaterThan(endedAfter));
    }
    if (endedBefore != null) {
      stepAttempts = stepAttempts.where(StepAttempt.END_TIME.lessThanOrEqualTo(endedBefore));
    }

    return stepAttempts.innerJoin(MapreduceJob.TBL)
        .on(MapreduceJob.STEP_ATTEMPT_ID.equalTo(StepAttempt.ID.as(Integer.class)));

  }

  public static List<MapreduceJob> getCompleteMapreduceJobs(IDatabases databases,
                                                            Long endedAfter,
                                                            Long endedBefore) throws IOException {

    List<MapreduceJob> jobs = Lists.newArrayList();
    for (Record record : completeMapreduceJobQuery(databases,
        endedAfter, endedBefore)
        .select(MapreduceJob.TBL.getAllColumns()).fetch()) {
      jobs.add(record.getModel(MapreduceJob.TBL, databases));
    }

    return jobs;
  }

  public static Map<Long, Long> getStepAttemptIdtoWorkflowExecutionId(IDatabases databases,
                                                                      Collection<Long> stepAttemptIds) throws IOException {
    Map<Long, Long> map = Maps.newHashMap();
    for (Record record : stepAttemptToExecutionQuery(databases, stepAttemptIds)
        .select(StepAttempt.ID, WorkflowExecution.ID)
        .fetch()) {
      map.put(record.getLong(StepAttempt.ID), record.getLong(WorkflowExecution.ID));
    }
    return map;
  }

  private static GenericQuery stepAttemptToExecutionQuery(IDatabases databases, Collection<Long> stepAttemptIds) {
    return databases.getWorkflowDb().createQuery().from(WorkflowExecution.TBL)
        .innerJoin(WorkflowAttempt.TBL)
        .on(WorkflowAttempt.WORKFLOW_EXECUTION_ID.equalTo(WorkflowExecution.ID.as(Integer.class)))
        .innerJoin(StepAttempt.TBL)
        .on(StepAttempt.WORKFLOW_ATTEMPT_ID.equalTo(WorkflowAttempt.ID.as(Integer.class)))
        .where(StepAttempt.ID.in(stepAttemptIds));
  }

  public static Set<WorkflowExecution> getExecutionsForStepAttempts(IDatabases databases,
                                                                    Collection<Long> stepAttemptIds) throws IOException {

    //  maybe a little conservative to distinct IDs first but some workflows have 123213214123131 steps so eh will reduce result size
    Set<Long> ids = Sets.newHashSet();
    for (Record record : stepAttemptToExecutionQuery(databases, stepAttemptIds)
        .select(WorkflowExecution.TBL.ID).fetch()) {
      ids.add(record.getLong(WorkflowExecution.TBL.ID));
    }

    Set<WorkflowExecution> executions = Sets.newHashSet();
    for (Record record : databases.getWorkflowDb().createQuery().from(WorkflowExecution.TBL)
        .where(WorkflowExecution.ID.in(ids))
        .fetch()) {
      executions.add(record.getModel(WorkflowExecution.TBL, databases));
    }

    return executions;
  }

  public static List<MapreduceCounter> getAllJobCounters(IDatabases databases,
                                                         Long endedAfter,
                                                         Long endedBefore,
                                                         Set<String> group,
                                                         Set<String> name) throws IOException {

    GenericQuery counterQuery = completeMapreduceJobQuery(databases, endedAfter, endedBefore)
        .innerJoin(MapreduceCounter.TBL)
        .on(MapreduceCounter.MAPREDUCE_JOB_ID.equalTo(MapreduceJob.ID.as(Integer.class)));

    if (group != null) {
      counterQuery = counterQuery.where(MapreduceCounter.GROUP.in(group));
    }

    if (name != null) {
      counterQuery = counterQuery.where(MapreduceCounter.NAME.in(name));
    }

    List<MapreduceCounter> counters = Lists.newArrayList();
    for (Record record : counterQuery
        .select(MapreduceCounter.TBL.getAllColumns()).fetch()) {
      counters.add(record.getModel(MapreduceCounter.TBL, databases));
    }

    return counters;

  }

  public static List<WorkflowAttempt> getLiveWorkflowAttempts(IWorkflowDb db,
                                                              Long executionId) throws IOException {

    List<WorkflowAttempt> attempts = db.workflowAttempts().query()
        .workflowExecutionId(executionId.intValue())
        .whereStatus(new In<>(WorkflowEnums.LIVE_ATTEMPT_STATUSES))
        .find();

    attempts.addAll(db.workflowAttempts().query()
        .workflowExecutionId(executionId.intValue())
        .whereStatus(new IsNull<Integer>())
        .find()
    );

    return attempts;
  }

  public static List<Application> getAllApplications(IDatabases databases) throws IOException {
    return databases.getWorkflowDb().applications().findAll();
  }

  public static NestedMultimap<Long, DSAction, WorkflowAttemptDatastore> getApplicationIdToWorkflowAttemptDatastores(IDatabases databases,
                                                                                                                     Long startedAfter,
                                                                                                                     Long startedBefore) throws IOException {

    Set<Column> columns = Sets.newHashSet(WorkflowAttemptDatastore.TBL.getAllColumns());
    columns.add(WorkflowExecution.APPLICATION_ID);
    columns.add(StepAttemptDatastore.DS_ACTION);

    GenericQuery on = joinStepAttempts(workflowExecutionQuery(databases.getWorkflowDb(), null, null, startedAfter, startedBefore))
        .innerJoin(StepAttemptDatastore.TBL)
        .on(StepAttempt.ID.equalTo(StepAttemptDatastore.STEP_ATTEMPT_ID.as(Long.class)))
        .innerJoin(WorkflowAttemptDatastore.TBL)
        .on(StepAttemptDatastore.WORKFLOW_ATTEMPT_DATASTORE_ID.equalTo(WorkflowAttemptDatastore.ID.as(Integer.class)))
        .select(columns);

    NestedMultimap<Long, DSAction, WorkflowAttemptDatastore> stores = new NestedMultimap<>();
    for (Record record : on.fetch()) {

      Integer appId = record.getInt(WorkflowExecution.APPLICATION_ID);
      Integer dsAction = record.getInt(StepAttemptDatastore.DS_ACTION);
      WorkflowAttemptDatastore model = record.getModel(WorkflowAttemptDatastore.TBL, databases);

      stores.put(appId.longValue(), DSAction.findByValue(dsAction), model);

    }

    return stores;

  }

  public static Multimap<WorkflowExecution, WorkflowAttempt> getExecutionsToAttempts(IDatabases databases,
                                                                                     AppType appType,
                                                                                     WorkflowExecutionStatus status) throws IOException {

    return getExecutionsToAttempts(databases, null, null, null, appType.getValue(), null, null, status, null);

  }

  public static Multimap<WorkflowExecution, WorkflowAttempt> getExecutionsToAttempts(IDatabases databases,
                                                                                     Long id,
                                                                                     String name,
                                                                                     String scope,
                                                                                     Integer appType,
                                                                                     Long startedAfter,
                                                                                     Long startedBefore,
                                                                                     WorkflowExecutionStatus status,
                                                                                     Integer limit) throws IOException {

    List<WorkflowExecution> executions = queryWorkflowExecutions(databases, id, name, scope, appType, startedAfter, startedBefore, status, limit);

    Map<Long, WorkflowExecution> executionsById = Maps.newHashMap();
    for (WorkflowExecution execution : executions) {
      executionsById.put(execution.getId(), execution);
    }

    Multimap<WorkflowExecution, WorkflowAttempt> executionAttempts = HashMultimap.create();
    for (WorkflowAttempt attempt : getWorkflowAttempts(databases, executionsById.keySet())) {
      executionAttempts.put(executionsById.get((long)attempt.getWorkflowExecutionId()), attempt);
    }

    return executionAttempts;
  }

  public static List<ConfiguredNotification.Attributes> getAttemptNotifications(IWorkflowDb db, WorkflowRunnerNotification type, Long attemptId) throws IOException {
    return getNotifications(db.createQuery().from(WorkflowAttemptConfiguredNotification.TBL)
            .where(WorkflowAttemptConfiguredNotification.WORKFLOW_ATTEMPT_ID.equalTo(attemptId))
            .innerJoin(ConfiguredNotification.TBL)
            .on(WorkflowAttemptConfiguredNotification.CONFIGURED_NOTIFICATION_ID.equalTo(ConfiguredNotification.ID)),
        type,
        null
    );
  }

  public static List<ConfiguredNotification.Attributes> getExecutionNotifications(IWorkflowDb db, Long executionId) throws IOException {
    return getExecutionNotifications(db, executionId, null, null);
  }

  public static List<ConfiguredNotification.Attributes> getExecutionNotifications(IWorkflowDb db, Long executionId, String email) throws IOException {
    return getExecutionNotifications(db, executionId, null, email);
  }

  public static List<ConfiguredNotification.Attributes> getExecutionNotifications(IWorkflowDb db, Long executionId, WorkflowRunnerNotification type) throws IOException {
    return getExecutionNotifications(db, executionId, type, null);
  }

  public static List<ConfiguredNotification.Attributes> getExecutionNotifications(IWorkflowDb db, Long executionId, WorkflowRunnerNotification type, String email) throws IOException {
    return getNotifications(db.createQuery().from(WorkflowExecutionConfiguredNotification.TBL)
            .where(WorkflowExecutionConfiguredNotification.WORKFLOW_EXECUTION_ID.equalTo(executionId))
            .innerJoin(ConfiguredNotification.TBL)
            .on(WorkflowExecutionConfiguredNotification.CONFIGURED_NOTIFICATION_ID.equalTo(ConfiguredNotification.ID)),
        type,
        email
    );
  }

  public static List<ConfiguredNotification.Attributes> getApplicationNotifications(IWorkflowDb db, Long applicationId) throws IOException {
    return getApplicationNotifications(db, applicationId, null, null);
  }

  public static List<ConfiguredNotification.Attributes> getApplicationNotifications(IWorkflowDb db, Long applicationId, WorkflowRunnerNotification type) throws IOException {
    return getApplicationNotifications(db, applicationId, type, null);
  }

  public static List<ConfiguredNotification.Attributes> getApplicationNotifications(IWorkflowDb db, Long applicationId, String email) throws IOException {
    return getApplicationNotifications(db, applicationId, null, email);
  }

  public static List<ConfiguredNotification.Attributes> getApplicationNotifications(IWorkflowDb db, Long applicationId, WorkflowRunnerNotification type, String email) throws IOException {
    return getNotifications(db.createQuery().from(ApplicationConfiguredNotification.TBL)
            .where(ApplicationConfiguredNotification.APPLICATION_ID.equalTo(applicationId))
            .innerJoin(ConfiguredNotification.TBL)
            .on(ApplicationConfiguredNotification.CONFIGURED_NOTIFICATION_ID.equalTo(ConfiguredNotification.ID)),
        type,
        email
    );
  }

  private static List<ConfiguredNotification.Attributes> getNotifications(GenericQuery configuredNotifications, WorkflowRunnerNotification type, String email) throws IOException {

    if (type != null) {
      configuredNotifications = configuredNotifications.where(ConfiguredNotification.WORKFLOW_RUNNER_NOTIFICATION.equalTo(type.ordinal()));
    }

    if (email != null) {
      configuredNotifications = configuredNotifications.where(ConfiguredNotification.EMAIL.equalTo(email));
    }

    List<ConfiguredNotification.Attributes> notifications = Lists.newArrayList();
    for (Record record : configuredNotifications
        .select(ConfiguredNotification.TBL.getAllColumns())
        .fetch()) {
      notifications.add(record.getAttributes(ConfiguredNotification.TBL));
    }

    return notifications;
  }


  public static List<WorkflowAttempt> getWorkflowAttempts(IDatabases databases,
                                                          Long endedAfter,
                                                          Long endedBefore) throws IOException {

    List<WorkflowAttempt> workflowAttempts = Lists.newArrayList();
    for (Record record : databases.getWorkflowDb().createQuery().from(WorkflowAttempt.TBL)
        .where(WorkflowAttempt.END_TIME.between(endedAfter, endedBefore))
        .fetch()) {
      workflowAttempts.add(record.getModel(WorkflowAttempt.TBL, databases));
    }
    return workflowAttempts;

  }

  //  public static Map<Long, Multimap<DSAction, WorkflowAttemptDatastore>> getApplicationDSActions(IDatabases db, Long startedAfter, Long startedBefore) throws IOException {
  //
  //    GenericQuery genericQuery = workflowExecutionQuery(db.getWorkflowDb(), null, null, startedAfter, startedBefore);
  //
  //    workflowAttemptquer
  //
  //
  //  }
  //
  //  private GenericQuery attemptDataStore

  public static List<WorkflowAttempt> getWorkflowAttempts(IDatabases databases,
                                                          Set<Long> workflowExecutionIds) throws IOException {

    List<WorkflowAttempt> workflowAttempts = Lists.newArrayList();
    for (Record record : databases.getWorkflowDb().createQuery().from(WorkflowAttempt.TBL)
        .where(WorkflowAttempt.WORKFLOW_EXECUTION_ID.as(Long.class).in(workflowExecutionIds))
        .fetch()) {
      workflowAttempts.add(record.getModel(WorkflowAttempt.TBL, databases));
    }
    return workflowAttempts;
  }

  public static List<WorkflowExecution> queryWorkflowExecutions(IDatabases databases,
                                                                String name,
                                                                String scope,
                                                                Integer appType,
                                                                Long startedAfter,
                                                                Long startedBefore,
                                                                Integer limit) throws IOException {
    return queryWorkflowExecutions(databases, null, name, scope, appType, startedAfter, startedBefore, null, limit);
  }

  public static List<WorkflowExecution> queryWorkflowExecutions(IDatabases databases,
                                                                Long id,
                                                                String name,
                                                                String scope,
                                                                Integer appType,
                                                                Long startedAfter,
                                                                Long startedBefore,
                                                                WorkflowExecutionStatus status,
                                                                Integer limit) throws IOException {
    Records fetch = workflowExecutionQuery(databases.getWorkflowDb(), id, name, scope, appType, startedAfter, startedBefore, status, limit)
        .select(WorkflowExecution.TBL.getAllColumns())
        .fetch();

    List<WorkflowExecution> executions = Lists.newArrayList();

    for (Record record : fetch) {
      executions.add(record.getModel(WorkflowExecution.TBL, databases));
    }

    return executions;
  }

  public static GenericQuery workflowExecutionQuery(IWorkflowDb db, String name, Integer appType, Long startedAfter, Long startedBefore) throws IOException {
    return workflowExecutionQuery(db, null, name, null, appType, startedAfter, startedBefore, null, null);
  }

  public static GenericQuery workflowExecutionQuery(IWorkflowDb db,
                                                    Long id,
                                                    String name,
                                                    String scope,
                                                    Integer appType,
                                                    Long startedAfter,
                                                    Long startedBefore,
                                                    WorkflowExecutionStatus status,
                                                    Integer limit) throws IOException {

    GenericQuery.Builder queryb = db.createQuery();
    GenericQuery query;

    if (name != null || appType != null) {

      query = queryb.from(Application.TBL);

      if (name != null) {
        query = query.where(Application.NAME.equalTo(name));
      }

      if (appType != null) {
        query.where(Application.APP_TYPE.equalTo(appType));
      }

      query.innerJoin(WorkflowExecution.TBL)
          .on(WorkflowExecution.APPLICATION_ID.equalTo(Application.ID.as(Integer.class)));

    } else {
      query = queryb.from(WorkflowExecution.TBL);
    }

    if (id != null) {
      query = query.where(WorkflowExecution.ID.equalTo(id));
    }

    if (scope != null) {
      //  TODO hack, figure out migrating to a default non-null scope ID to avoid this
      if (scope.equals("__NULL")) {
        query = query.where(WorkflowExecution.SCOPE_IDENTIFIER.isNull());
      } else {
        query = query.where(WorkflowExecution.SCOPE_IDENTIFIER.equalTo(scope));
      }
    }

    if (startedBefore != null) {
      query = query.where(WorkflowExecution.START_TIME.lessThan(startedBefore));
    }

    if (startedAfter != null) {
      query = query.where(WorkflowExecution.START_TIME.greaterThan(startedAfter));
    }

    if (status != null) {
      query = query.where(WorkflowExecution.STATUS.equalTo(status.ordinal()));
    }

    if (limit != null) {
      query = query.orderBy(WorkflowExecution.ID, QueryOrder.DESC);
      query = query.limit(limit);
    }

    return query;
  }

  public static List<StepAttempt.Attributes> getStepAttempts(IWorkflowDb db, Long workflowAttemptId) throws IOException {
    return getStepAttempts(db, workflowAttemptId, null);
  }

  public static List<StepAttempt.Attributes> getStepAttempts(IWorkflowDb db, Long workflowAttemptId, String stepToken) throws IOException {
    List<StepAttempt.Attributes> executions = Lists.newArrayList();

    for (Record record : queryStepAttempts(db, workflowAttemptId, stepToken).select(StepAttempt.TBL.getAllColumns()).fetch()) {
      executions.add(record.getAttributes(StepAttempt.TBL));
    }
    return executions;
  }

  public static Map<String, StepStatus> getStepStatuses(IWorkflowDb db, Long workflowAttemptId, String stepToken) throws IOException {
    Map<String, StepStatus> statuses = Maps.newHashMap();
    for (Record record : queryStepAttempts(db, workflowAttemptId, stepToken).select(StepAttempt.STEP_TOKEN, StepAttempt.STEP_STATUS).fetch()) {
      statuses.put(record.get(StepAttempt.STEP_TOKEN), StepStatus.findByValue(record.get(StepAttempt.STEP_STATUS)));
    }
    return statuses;
  }

  private static GenericQuery queryStepAttempts(IWorkflowDb db, Long workflowAttemptId, String stepToken) {
    GenericQuery query = db.createQuery().from(StepAttempt.TBL).where(StepAttempt.WORKFLOW_ATTEMPT_ID.as(Long.class).equalTo(workflowAttemptId));

    if (stepToken != null) {
      query = query.where(StepAttempt.STEP_TOKEN.equalTo(stepToken));
    }

    return query;
  }

  public static List<StepDependency.Attributes> getStepDependencies(IWorkflowDb db, Set<Long> stepAttemptIds) throws IOException {
    List<StepDependency.Attributes> dependencies = Lists.newArrayList();
    for (Record record : db.createQuery().from(StepDependency.TBL).where(StepDependency.STEP_ATTEMPT_ID.as(Long.class).in(stepAttemptIds).or(StepDependency.DEPENDENCY_ATTEMPT_ID.as(Long.class).in(stepAttemptIds))).fetch()) {
      dependencies.add(record.getAttributes(StepDependency.TBL));
    }
    return dependencies;
  }

  public static List<MapreduceJob.Attributes> getMapreduceJobs(IWorkflowDb db, Set<Long> stepAttemptIds) throws IOException {
    List<MapreduceJob.Attributes> jobs = Lists.newArrayList();
    for (Record record : db.createQuery().from(MapreduceJob.TBL).where(MapreduceJob.STEP_ATTEMPT_ID.as(Long.class).in(stepAttemptIds)).fetch()) {
      jobs.add(record.getAttributes(MapreduceJob.TBL));
    }
    return jobs;
  }

  public static List<MapreduceJobTaskException.Attributes> getMapreduceJobTaskExceptions(IWorkflowDb db, Set<Long> mapreduceJobIds) throws IOException {
    List<MapreduceJobTaskException.Attributes> exceptions = Lists.newArrayList();

    for (Record record : db.createQuery().from(MapreduceJobTaskException.TBL)
        .where(MapreduceJobTaskException.MAPREDUCE_JOB_ID.as(Long.class).in(mapreduceJobIds)).fetch()) {
      exceptions.add(record.getAttributes(MapreduceJobTaskException.TBL));
    }

    return exceptions;
  }

  public static List<MapreduceCounter.Attributes> getMapreduceCounters(IWorkflowDb db, Set<Long> mapreduceJobIds) throws IOException {
    List<MapreduceCounter.Attributes> counters = Lists.newArrayList();
    for (Record record : db.createQuery().from(MapreduceCounter.TBL).where(MapreduceCounter.MAPREDUCE_JOB_ID.as(Long.class).in(mapreduceJobIds)).fetch()) {
      counters.add(record.getAttributes(MapreduceCounter.TBL));
    }
    return counters;
  }

  public static List<StepAttemptDatastore.Attributes> getStepAttemptDatastores(IWorkflowDb db, Set<Long> stepIds) throws IOException {
    List<StepAttemptDatastore.Attributes> attemptDatastores = Lists.newArrayList();
    for (Record record : db.createQuery().from(StepAttemptDatastore.TBL).where(StepAttemptDatastore.STEP_ATTEMPT_ID.as(Long.class).in(stepIds)).fetch()) {
      attemptDatastores.add(record.getAttributes(StepAttemptDatastore.TBL));
    }
    return attemptDatastores;
  }

  public static List<WorkflowAttemptDatastore.Attributes> getWorkflowAttemptDatastores(IWorkflowDb db, Set<Long> ids, Long workflowAttemptId) throws IOException {
    List<WorkflowAttemptDatastore.Attributes> workflowAttemptDatastore = Lists.newArrayList();

    GenericQuery query = db.createQuery().from(WorkflowAttemptDatastore.TBL);

    if (workflowAttemptId != null) {
      query = query.where(WorkflowAttemptDatastore.WORKFLOW_ATTEMPT_ID.as(Long.class).equalTo(workflowAttemptId));
    }

    if (ids != null) {
      query = query.where(WorkflowAttemptDatastore.ID.in(ids));
    }

    for (Record record : query.fetch()) {
      workflowAttemptDatastore.add(record.getAttributes(WorkflowAttemptDatastore.TBL));
    }
    return workflowAttemptDatastore;
  }

  //  join queries

  public static GenericQuery getStepAttempts(IWorkflowDb db, Long execution, Set<String> latestTokens, EnumSet<StepStatus> statuses) {
    return filterStepAttempts(joinStepAttempts(db.createQuery().from(WorkflowExecution.TBL)
            .where(WorkflowExecution.ID.equalTo(execution))),
        latestTokens,
        statuses
    );
  }

  public static GenericQuery getCompleteStepCounters(IWorkflowDb db, Long executionId) throws IOException {

    //  TODO can use one query for this whole thing probably
    WorkflowExecution execution = db.workflowExecutions().find(executionId);

    WorkflowAttempt latestAttempt = getLatestAttempt(execution);
    List<StepAttempt> steps = latestAttempt.getStepAttempt();

    Set<String> latestTokens = Sets.newHashSet();
    for (StepAttempt attempt : steps) {
      latestTokens.add(attempt.getStepToken());
    }

    return db.createQuery()
        .from(StepAttempt.TBL)
        .where(StepAttempt.STEP_TOKEN.in(latestTokens))
        .where(StepAttempt.STEP_STATUS.equalTo(StepStatus.COMPLETED.ordinal()))
        .innerJoin(WorkflowAttempt.TBL)
        .on(StepAttempt.WORKFLOW_ATTEMPT_ID.equalTo(WorkflowAttempt.ID.as(Integer.class)))
        .where(WorkflowAttempt.WORKFLOW_EXECUTION_ID.as(Long.class).equalTo(executionId))
        .innerJoin(MapreduceJob.TBL)
        .on(MapreduceJob.STEP_ATTEMPT_ID.equalTo(StepAttempt.ID.as(Integer.class)))
        .innerJoin(MapreduceCounter.TBL)
        .on(MapreduceCounter.MAPREDUCE_JOB_ID.equalTo(MapreduceJob.ID.as(Integer.class)));

  }

  public static GenericQuery getExecutionsByEndQuery(IWorkflowDb db, LocalDate startDate, LocalDate endDate) {
    return db.createQuery().from(WorkflowExecution.TBL)
        .where(WorkflowExecution.END_TIME.between(startDate.toDate().getTime(), endDate.toDate().getTime()))
        .select(WorkflowExecution.NAME, COUNT(WorkflowExecution.ID))
        .groupBy(WorkflowExecution.NAME);
  }

  public static List<ApplicationCounterSummary> getSummaries(IWorkflowDb db, Multimap<String, String> countersToQuery, LocalDate startDate, LocalDate endDate) throws IOException {
    return db.applicationCounterSummaries().query()
        .whereDate(new Between<>(startDate.toDateTimeAtStartOfDay().getMillis(), endDate.toDateTimeAtStartOfDay().getMillis()-1))  // stupid mysql
        .whereGroup(new In<>(countersToQuery.keySet()))
        .whereName(new In<>(countersToQuery.values()))
        .find();
  }

  public static GenericQuery getMapreduceCounters(IWorkflowDb db, Set<String> stepToken, String name, Integer appType, Long endedAfter, Long endedBefore,
                                                  Set<String> specificGroups,
                                                  Set<String> specificNames) throws IOException {
    return getMapreduceCounters(getStepAttempts(db, stepToken, name, appType, endedAfter, endedBefore), specificGroups, specificNames);
  }

  public static GenericQuery getMapreduceCounters(IWorkflowDb db, Set<String> stepToken, Set<Long> workflowExecutionIds,
                                                  Set<String> specificGroups,
                                                  Set<String> specificNames) throws IOException {
    return getMapreduceCounters(getStepAttempts(db, stepToken, workflowExecutionIds), specificGroups, specificNames);
  }

  public static GenericQuery getMapreduceCounters(GenericQuery stepQuery,
                                                  Set<String> specificGroups,
                                                  Set<String> specificNames) {
    GenericQuery query = stepQuery.innerJoin(MapreduceJob.TBL)
        .on(StepAttempt.ID.equalTo(MapreduceJob.STEP_ATTEMPT_ID.as(Long.class)))
        .innerJoin(MapreduceCounter.TBL)
        .on(MapreduceJob.ID.equalTo(MapreduceCounter.MAPREDUCE_JOB_ID.as(Long.class)));

    if (specificGroups != null) {
      query = query.where(MapreduceCounter.GROUP.in(specificGroups));
    }

    if (specificNames != null) {
      query = query.where(MapreduceCounter.NAME.in(specificNames));
    }

    return query;
  }

  public static GenericQuery getStepAttempts(IWorkflowDb db, Set<String> stepTokens, String name, Integer appType, Long endedAfter, Long endedBefore) throws IOException {
    return filterStepAttempts(
        joinStepAttempts(workflowExecutionQuery(db, name, appType, null, null)),
        stepTokens,
        null,
        endedAfter,
        endedBefore
    );
  }
  public static GenericQuery getStepAttempts(IWorkflowDb db, Set<String> stepTokens, Set<Long> workflowExecutionIds) throws IOException {

    GenericQuery attempts = db.createQuery().from(WorkflowAttempt.TBL)
        .where(WorkflowAttempt.WORKFLOW_EXECUTION_ID.as(Long.class).in(workflowExecutionIds))
        .innerJoin(StepAttempt.TBL)
        .on(WorkflowAttempt.ID.equalTo(StepAttempt.WORKFLOW_ATTEMPT_ID.as(Long.class)));

    return filterStepAttempts(attempts, stepTokens, null);
  }

  private static GenericQuery joinStepAttempts(GenericQuery workflowExecutions) {
    return joinWorkflowAttempts(workflowExecutions)
        .innerJoin(StepAttempt.TBL)
        .on(WorkflowAttempt.ID.equalTo(StepAttempt.WORKFLOW_ATTEMPT_ID.as(Long.class)));
  }

  private static GenericQuery joinWorkflowAttempts(GenericQuery workflowExecutions) {
    return workflowExecutions.innerJoin(WorkflowAttempt.TBL)
        .on(WorkflowExecution.ID.equalTo(WorkflowAttempt.WORKFLOW_EXECUTION_ID.as(Long.class)));
  }

  private static GenericQuery filterStepAttempts(GenericQuery stepQuery, Set<String> stepToken, EnumSet<StepStatus> inStatuses) {
    return filterStepAttempts(stepQuery, stepToken, inStatuses, null, null);
  }

  private static GenericQuery filterStepAttempts(GenericQuery stepQuery, Set<String> stepToken, EnumSet<StepStatus> inStatuses,
                                                 Long endedAfter,
                                                 Long endedBefore) {

    if (stepToken != null) {
      stepQuery = stepQuery.where(StepAttempt.STEP_TOKEN.in(Sets.newHashSet(stepToken)));
    }

    if (inStatuses != null) {

      Set<Integer> inStatusInts = Sets.newHashSet();
      for (StepStatus status : inStatuses) {
        inStatusInts.add(status.ordinal());
      }

      stepQuery = stepQuery.where(StepAttempt.STEP_STATUS.in(inStatusInts));
    }

    if(endedAfter != null){
      stepQuery = stepQuery.where(StepAttempt.END_TIME.greaterThan(endedAfter));
    }

    if(endedBefore != null){
      stepQuery = stepQuery.where(StepAttempt.END_TIME.lessThan(endedBefore));
    }

    return stepQuery;
  }


}