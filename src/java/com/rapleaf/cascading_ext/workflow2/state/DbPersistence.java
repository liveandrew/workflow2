package com.rapleaf.cascading_ext.workflow2.state;

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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.iface.IMapreduceJobPersistence;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.StepAttemptDatastore;
import com.rapleaf.db_schemas.rldb.models.StepDependency;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttemptDatastore;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;

public class DbPersistence implements WorkflowStatePersistence {
  private static final Logger LOG = Logger.getLogger(DbPersistence.class);

  private final IRlDb rldb;
  private final long workflowAttemptId;

  public DbPersistence(long workflowAttemptId) {
    this.rldb = new DatabasesImpl().getRlDb();
    this.rldb.disableCaching();
    this.workflowAttemptId = workflowAttemptId;
  }

  private StepAttempt getStep(String stepToken) throws IOException {

    Set<StepAttempt> attempts = rldb.stepAttempts().query()
        .workflowAttemptId((int)workflowAttemptId)
        .stepToken(stepToken)
        .find();

    if (attempts.size() > 1) {
      throw new RuntimeException();
    }

    if (attempts.isEmpty()) {
      return null;
    }

    return attempts.iterator().next();
  }

  private WorkflowAttempt getAttempt() throws IOException {
    return rldb.workflowAttempts().find(workflowAttemptId);
  }

  private WorkflowExecution getExecution() throws IOException {
    return getAttempt().getWorkflowExecution();
  }

  private void save(StepAttempt attempt) throws IOException {
    rldb.stepAttempts().save(attempt);
  }

  private void save(WorkflowExecution execution) throws IOException {
    rldb.workflowExecutions().save(execution);
  }

  private void save(WorkflowAttempt attempt) throws IOException {
    rldb.workflowAttempts().save(attempt);
  }

  @Override
  public synchronized void markStepRunning(String stepToken) throws IOException {
    LOG.info("Marking step " + stepToken + " as running");
    save(getStep(stepToken)
            .setStepStatus(StepStatus.RUNNING.ordinal())
            .setStartTime(System.currentTimeMillis())
    );
  }

  @Override
  public synchronized void markStepFailed(String stepToken, Throwable t) throws IOException {
    LOG.info("Marking step " + stepToken + " as failed");

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);

    save(getStep(stepToken)
            .setFailureCause(t.getMessage())
            .setFailureTrace(StringUtils.substring(sw.toString(), 0, 10000)) //  db limit
            .setStepStatus(StepStatus.FAILED.ordinal())
            .setEndTime(System.currentTimeMillis())
    );

  }

  @Override
  public synchronized void markStepSkipped(String stepToken) throws IOException {
    LOG.info("Marking step " + stepToken + " as skipped");
    save(getStep(stepToken)
            .setStepStatus(StepStatus.SKIPPED.ordinal())
    );
  }

  @Override
  public synchronized void markStepCompleted(String stepToken) throws IOException {
    LOG.info("Marking step " + stepToken + " as completed");
    save(getStep(stepToken)
            .setStepStatus(StepStatus.COMPLETED.ordinal())
            .setEndTime(System.currentTimeMillis())
    );
  }

  @Override
  public synchronized void markStepStatusMessage(String stepToken, String newMessage) throws IOException {
    LOG.info("Marking step status message: " + stepToken + " message " + newMessage);
    save(getStep(stepToken)
            .setStatusMessage(newMessage)
    );
  }

  @Override
  public synchronized void markStepRunningJob(String stepToken, RunningJob job) throws IOException {

    StepAttempt step = getStep(stepToken);
    String jobId = job.getID().toString();

    Set<MapreduceJob> saved = rldb.mapreduceJobs().query()
        .stepAttemptId((int)step.getId())
        .jobIdentifier(jobId)
        .find();

    if (saved.isEmpty()) {
      LOG.info("Marking step " + stepToken + " as running job " + job.getID());

      IMapreduceJobPersistence jobPersistence = rldb.mapreduceJobs();
      jobPersistence.save(jobPersistence.create((int)step.getId(),
          jobId,
          job.getJobName(),
          job.getTrackingURL()
      ));
    }

  }

  @Override
  public synchronized void markPool(String pool) throws IOException {
    LOG.info("Setting pool: " + pool);
    save(getAttempt()
            .setPool(pool)
    );
  }

  @Override
  public synchronized void markPriority(String priority) throws IOException {
    LOG.info("Setting priority: " + priority);
    save(getAttempt()
            .setPriority(priority)
    );
  }

  @Override
  public synchronized void markShutdownRequested(String reason) throws IOException {
    LOG.info("Processing shutdown request: " + reason);
    save(getAttempt()
            .setShutdownReason(reason)
    );
  }

  @Override
  public synchronized void markWorkflowStopped() throws IOException {
    LOG.info("Marking workflow stopped");
    if (allStepsSucceeded() && getShutdownRequest() == null) {
      LOG.info("Marking execution as complete");
      save(getExecution().setStatus(WorkflowExecutionStatus.COMPLETE.ordinal()));
    }
  }

  private boolean allStepsSucceeded() throws IOException {
    for (StepAttempt attempt : attemptSteps()) {
      if (!StepStatus.NON_BLOCKING_IDS.contains((int)attempt.getId())) {
        return false;
      }
    }
    return true;
  }

  private Set<StepAttempt> attemptSteps() throws IOException {
    return rldb.stepAttempts().query()
        .workflowAttemptId((int)workflowAttemptId)
        .find();
  }


  @Override
  public synchronized StepState getState(String stepToken) throws IOException {

    StepAttempt step = getStep(stepToken);

    Set<String> dependnecies = Sets.newHashSet();
    for (StepDependency dependency : step.getStepDependencies()) {
      dependnecies.add(dependency.getDependencyAttempt().getStepToken());
    }

    Multimap<Action.DSAction, DataStoreInfo> datastores = HashMultimap.create();
    for (StepAttemptDatastore ds : step.getStepAttemptDatastores()) {
      datastores.put(Action.DSAction.values()[ds.getDsAction()], asDSInfo(ds.getWorkflowAttemptDatastore()));
    }

    StepState state = new StepState(step.getStepToken(),
        StepStatus.values()[step.getStepStatus()],
        step.getActionClass(),
        dependnecies,
        datastores)
        .setFailureMessage(step.getFailureCause())
        .setFailureTrace(step.getFailureTrace())
        .setStatusMessage(step.getStatusMessage());

    for (MapreduceJob job : step.getMapreduceJobs()) {
      state.addMrjob(new MapReduceJob(job.getJobIdentifier(), job.getJobName(), job.getTrackingUrl()));
    }

    if(step.getStartTime() != null){
      state.setStartTimestamp(step.getStartTime());
    }

    if(step.getEndTime() != null){
      state.setEndTimestamp(step.getEndTime());
    }

    return state;
  }

  @Override
  public synchronized Map<String, StepState> getStepStatuses() throws IOException {

    Map<String, StepState> states = Maps.newHashMap();
    for (StepAttempt attempt : attemptSteps()) {
      String token = attempt.getStepToken();

      states.put(token, getState(token));
    }

    return states;
  }

  private DataStoreInfo asDSInfo(WorkflowAttemptDatastore store) {
    return new DataStoreInfo(store.getName(), store.getClassName(), store.getPath(), (int)store.getId());
  }

  @Override
  public synchronized List<DataStoreInfo> getDatastores() throws IOException {

    List<DataStoreInfo> info = Lists.newArrayList();

    for (WorkflowAttemptDatastore attemptDatastore : rldb.workflowAttemptDatastores().query()
        .workflowAttemptId((int)workflowAttemptId)
        .find()) {
      info.add(asDSInfo(attemptDatastore));
    }

    return info;

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
    return getAttempt().getPool();
  }

  @Override
  public synchronized String getName() throws IOException {
    return getExecution().getName();
  }

  @Override
  public synchronized String getId() throws IOException {
    return Long.toString(getAttempt().getId());
  }

  @Override
  public synchronized String getHost() throws IOException {
    return getAttempt().getHost();
  }

  @Override
  public synchronized String getUsername() throws IOException {
    return getAttempt().getSystemUser();
  }
}
