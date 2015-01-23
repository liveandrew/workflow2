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
import com.rapleaf.db_schemas.rldb.workflow.DSAction;
import com.rapleaf.db_schemas.rldb.workflow.DbPersistence;
import com.rapleaf.db_schemas.rldb.workflow.StepStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowExecutionStatus;
import com.rapleaf.jack.queries.QueryOrder;
import com.rapleaf.jack.queries.where_operators.In;

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
                                   String priority) {

    try {

      WorkflowExecution execution = getExecution(name, scopeId, appType);
      LOG.info("Using workflow execution: " + execution);

      Set<WorkflowAttempt> previousAttempt = findLastWorkflowAttempt(execution);
      LOG.info("Found previous attempt: " + previousAttempt);

      WorkflowAttempt attempt = createAttempt(host, username, pool, priority, execution);
      LOG.info("Using new attempt: " + attempt);
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

        StepAttempt stepAttempt = createStepAttempt(step, attempt, previousAttempt);
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
            (int) attempts.get(step.getCheckpointToken()).getId(),
            (int) attempts.get(dep.getCheckpointToken()).getId()
        );

      }

      return new DbPersistence(workflowAttemptId);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private WorkflowAttempt createAttempt(String host, String username, String pool, String priority, WorkflowExecution execution) throws IOException {
    return rldb.workflowAttempts().create((int)execution.getId(), username, null, priority, pool, host);
  }


  private StepAttempt createStepAttempt(Step step, WorkflowAttempt attempt, Set<WorkflowAttempt> previousAttempt) throws IOException {

    Set<Integer> prevIds = Sets.newHashSet();
    for (WorkflowAttempt prev : previousAttempt) {
      prevIds.add((int) prev.getId());
    }

    String token = step.getCheckpointToken();

    //  if the previous workflow died uncleanly (machine died, OOME, etc), we might see steps as still running.  fail them here to clean up.
    for (StepAttempt runningStepAttmpt : rldb.stepAttempts().query()
        .whereWorkflowAttemptId(new In<Integer>(prevIds))
        .stepToken(token)
        .stepStatus(StepStatus.RUNNING.ordinal())
        .find()) {

      runningStepAttmpt
          .setStepStatus(StepStatus.FAILED.ordinal())
          .setFailureCause("Unknown, failed by cleanup.")
          .save();

    }


    return rldb.stepAttempts().create((int)attempt.getId(), token, null, null,
        getInitialStatus(token, prevIds).ordinal(),
        null,
        null,
        step.getAction().getClass().getName(),
        ""
    );

  }

  private StepStatus getInitialStatus(String stepId, Set<Integer> prevAttemptIds) throws IOException {

    if (isStepComplete(stepId, prevAttemptIds)) {
      return StepStatus.SKIPPED;
    }

    return StepStatus.WAITING;
  }

  private boolean isStepComplete(String step, Set<Integer> previousAttemptIds) throws IOException {

    Set<StepAttempt> completeAttempt = rldb.stepAttempts().query()
        .whereWorkflowAttemptId(new In<Integer>(previousAttemptIds))
        .stepToken(step)
        .whereStepStatus(new In<Integer>(StepStatus.NON_BLOCKING_IDS))
        .find();

    if (completeAttempt.size() > 1) {
      throw new RuntimeException("Found multiple complete step attempts for a workflow attempt!");
    }

    return !completeAttempt.isEmpty();

  }

  private Set<WorkflowAttempt> findLastWorkflowAttempt(WorkflowExecution execution) throws IOException {
    return rldb.workflowAttempts().query()
        .workflowExecutionId((int)execution.getId())
        .orderById(QueryOrder.DESC)
        .find();
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

      //  new execution
      WorkflowExecution ex = rldb.workflowExecutions().create(name, WorkflowExecutionStatus.INCOMPLETE.ordinal())
          .setScopeIdentifier(scopeId);

      if (appType != null) {
        ex.setAppType(appType.getValue());
      }

      return ex;

    } else {

      //  only one, return it
      return incompleteExecutions.iterator().next();

    }

  }


}
