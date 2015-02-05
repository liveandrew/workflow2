package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.importer.generated.AppType;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttemptDatastore;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.AttemptStatus;
import com.rapleaf.db_schemas.rldb.workflow.DSAction;
import com.rapleaf.db_schemas.rldb.workflow.DbPersistence;
import com.rapleaf.db_schemas.rldb.workflow.StepStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowExecutionStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowQueries;

public class DbPersistenceFactory implements WorkflowPersistenceFactory {
  private static final Logger LOG = Logger.getLogger(DbPersistence.class);

  private final IRlDb rldb;

  public DbPersistenceFactory() {
    this.rldb = new DatabasesImpl().getRlDb();
    this.rldb.disableCaching();
  }

  @Override
  public synchronized DbPersistence prepare(DirectedGraph<Step, DefaultEdge> flatSteps,
                                            String name,
                                            String scopeId,
                                            AppType appType,
                                            String host,
                                            String username,
                                            String pool,
                                            String priority,
                                            String launchDir,
                                            String launchJar) {

    try {

      WorkflowExecution execution = getExecution(name, scopeId, appType);
      LOG.info("Using workflow execution: " + execution + " id " + execution.getId());

      Set<WorkflowAttempt> prevAttempts = execution.getWorkflowAttempt();
      resolveRunningAttempts(prevAttempts);

      LOG.info("Found previous attempts: " + prevAttempts);

      WorkflowAttempt attempt = createAttempt(host, username, pool, priority, launchDir, launchJar, execution);
      LOG.info("Using new attempt: " + attempt + " id " + attempt.getId());
      long workflowAttemptId = attempt.getId();


      Set<DataStore> allStores = Sets.newHashSet();

      for (Step step : flatSteps.vertexSet()) {
        for (DataStore store : step.getAction().getAllDatastores().values()) {
          allStores.add(store);
        }
      }

      Map<DataStore, WorkflowAttemptDatastore> datastores = Maps.newHashMap();
      for (DataStore store : allStores) {
        datastores.put(store, rldb.workflowAttemptDatastores().create(
            (int)workflowAttemptId,
            store.getName(),
            store.getPath(),
            store.getClass().getName()
        ));
      }

      Map<String, StepAttempt> attempts = Maps.newHashMap();
      for (Step step : flatSteps.vertexSet()) {

        StepAttempt stepAttempt = createStepAttempt(step, attempt, prevAttempts);
        attempts.put(stepAttempt.getStepToken(), stepAttempt);

        for (Map.Entry<DSAction, DataStore> entry : step.getAction().getAllDatastores().entries()) {
          rldb.stepAttemptDatastores().create(
              (int)stepAttempt.getId(),
              (int)datastores.get(entry.getValue()).getId(),
              entry.getKey().ordinal()
          );
        }

      }

      for (DefaultEdge edge : flatSteps.edgeSet()) {

        Step dep = flatSteps.getEdgeTarget(edge);
        Step step = flatSteps.getEdgeSource(edge);

        rldb.stepDependencies().create(
            (int)attempts.get(step.getCheckpointToken()).getId(),
            (int)attempts.get(dep.getCheckpointToken()).getId()
        );

      }

      return new DbPersistence(workflowAttemptId);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private void resolveRunningAttempts(Set<WorkflowAttempt> prevAttempts) throws IOException {

    for (WorkflowAttempt attempt : prevAttempts) {

      //  check to see if any of these workflows are still marked as alive
      if(AttemptStatus.LIVE_STATUSES.contains(attempt.getStatus())){

        //  if it's still heartbeating, fail loudly
        if(WorkflowQueries.isLive(attempt)){
          throw new RuntimeException("Cannot start, a previous attempt is still alive! Attempt: "+attempt);
        }

        //  otherwise it is safe to clean up
        LOG.info("Marking old running attempt as FAILED: "+attempt);
        attempt
            .setStatus(AttemptStatus.FAILED.ordinal())
            .setEndTime(System.currentTimeMillis())
            .save();

      }

      //  and mark any step that was still running as failed
      for (StepAttempt step : attempt.getStepAttempt()) {
        if(step.getStepStatus() == StepStatus.RUNNING.ordinal()){
          LOG.info("Marking old runing step as FAILED: "+step);
          step
              .setStepStatus(StepStatus.FAILED.ordinal())
              .setFailureCause("Unknown, failed by cleanup.")
              .setEndTime(System.currentTimeMillis())
              .save();
        }
      }

    }

  }

  private WorkflowAttempt createAttempt(String host, String username, String pool, String priority, String launchDir, String launchJar, WorkflowExecution execution) throws IOException {
    WorkflowAttempt attempt = rldb.workflowAttempts().create((int)execution.getId(), username, priority, pool, host)
        .setLaunchDir(launchDir)
        .setLaunchJar(launchJar);
    attempt.save();
    return attempt;
  }

  private StepAttempt createStepAttempt(Step step, WorkflowAttempt attempt, Set<WorkflowAttempt> attempts) throws IOException {

    String token = step.getCheckpointToken();

    return rldb.stepAttempts().create((int)attempt.getId(), token, null, null,
        getInitialStatus(token, attempts).ordinal(),
        null,
        null,
        step.getAction().getClass().getName(),
        ""
    );

  }

  private StepStatus getInitialStatus(String stepId, Set<WorkflowAttempt> attempts) throws IOException {

    if (WorkflowQueries.isStepComplete(rldb, stepId, attempts)) {
      return StepStatus.SKIPPED;
    }

    return StepStatus.WAITING;
  }

  private WorkflowExecution getExecution(String name, String scopeId, AppType appType) throws IOException {

    Set<WorkflowExecution> incompleteExecutions = rldb.workflowExecutions().query()
        .name(name)
        .scopeIdentifier(scopeId)
        .status(WorkflowExecutionStatus.INCOMPLETE.ordinal())
        .find();

    if (incompleteExecutions.size() > 1) {
      throw new RuntimeException("Found multiple incomplete workflow executions!");
    }

    if (incompleteExecutions.isEmpty()) {
      LOG.info("No incomplete execution found, creating new execution");

      //  new execution
      WorkflowExecution ex = rldb.workflowExecutions().create(name, WorkflowExecutionStatus.INCOMPLETE.ordinal())
          .setScopeIdentifier(scopeId)
          .setStartTime(System.currentTimeMillis());

      if (appType != null) {
        ex.setAppType(appType.getValue());
      }

      ex.save();

      return ex;

    } else {
      //  only one, return it
      WorkflowExecution prevExecution = incompleteExecutions.iterator().next();
      LOG.info("Found previous execution " + prevExecution + " id " + prevExecution.getId());

      return prevExecution;

    }

  }


}
