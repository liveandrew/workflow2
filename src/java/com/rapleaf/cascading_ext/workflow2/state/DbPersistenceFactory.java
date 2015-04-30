package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.util.Time;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.importer.generated.AppType;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.Application;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttemptDatastore;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.util.JackUtil;
import com.rapleaf.db_schemas.rldb.workflow.Assertions;
import com.rapleaf.db_schemas.rldb.workflow.AttemptStatus;
import com.rapleaf.db_schemas.rldb.workflow.DSAction;
import com.rapleaf.db_schemas.rldb.workflow.DbPersistence;
import com.rapleaf.db_schemas.rldb.workflow.StepStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowExecutionStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowQueries;
import com.rapleaf.jack.queries.Record;
import com.rapleaf.jack.queries.Records;
import com.rapleaf.support.collections.Accessors;

public class DbPersistenceFactory implements WorkflowPersistenceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DbPersistence.class);

  private final IDatabases databases;
  private final IRlDb rldb;

  public DbPersistenceFactory() {
    this.databases = new DatabasesImpl();
    this.rldb = databases.getRlDb();
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

      Application application = getApplication(name, appType);
      LOG.info("Using application: "+application);

      WorkflowExecution execution = getExecution(application, name, appType, scopeId);
      LOG.info("Using workflow execution: " + execution + " id " + execution.getId());

      cleanUpRunningAttempts(execution);

      Optional<WorkflowAttempt> latestAttempt = WorkflowQueries
          .getLatestAttemptOptional(execution);

      WorkflowAttempt attempt = createAttempt(host,
          username,
          getPool(latestAttempt, pool),
          getPriority(latestAttempt, priority),
          launchDir,
          launchJar,
          execution
      );

      long workflowAttemptId = attempt.getId();
      LOG.info("Using new attempt: " + attempt + " id " + workflowAttemptId);

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

        StepAttempt stepAttempt = createStepAttempt(step, attempt, execution);
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

  private Application getApplication(String name, AppType appType) throws IOException {

    Optional<Application> application = Accessors.firstOrAbsent(rldb.applications().findByName(name));

    if (application.isPresent()) {
      LOG.info("Using existing application");

      return application.get();
    } else {
      LOG.info("Creating new application for name: "+name+", app type "+appType);

      Application app = rldb.applications().create(name);
      if (appType != null) {
        app.setAppType(appType.getValue());
      }
      rldb.applications().save(app);
      return app;
    }

  }

  private String getPool(Optional<WorkflowAttempt> last, String provided) {
    if (last.isPresent()) {
      return last.get().getPool();
    }
    return provided;
  }

  private String getPriority(Optional<WorkflowAttempt> last, String priority) {
    if (last.isPresent()) {
      return last.get().getPriority();
    }
    return priority;
  }

  private void cleanUpRunningAttempts(WorkflowExecution execution) throws IOException {

    List<WorkflowAttempt> prevAttempts = execution.getWorkflowAttempt();
    LOG.info("Found previous attempts: " + prevAttempts);

    for (WorkflowAttempt attempt : prevAttempts) {

      //  check to see if any of these workflows are still marked as alive
      if (AttemptStatus.LIVE_STATUSES.contains(attempt.getStatus())) {

        Assertions.assertDead(attempt);

        //  otherwise it is safe to clean up
        LOG.info("Marking old running attempt as FAILED: " + attempt);
        attempt
            .setStatus(AttemptStatus.FAILED.ordinal())
            .setEndTime(System.currentTimeMillis())
            .save();

      }

      //  and mark any step that was still running as failed
      for (StepAttempt step : attempt.getStepAttempt()) {
        if (step.getStepStatus() == StepStatus.RUNNING.ordinal()) {
          LOG.info("Marking old runing step as FAILED: " + step);
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
        .setLaunchJar(launchJar)
        .setLastHeartbeat(System.currentTimeMillis());
    attempt.save();
    return attempt;
  }

  private StepAttempt createStepAttempt(Step step, WorkflowAttempt attempt, WorkflowExecution execution) throws IOException {

    String token = step.getCheckpointToken();

    return rldb.stepAttempts().create((int)attempt.getId(), token, null, null,
        getInitialStatus(token, execution).ordinal(),
        null,
        null,
        step.getAction().getClass().getName(),
        ""
    );

  }

  private StepStatus getInitialStatus(String stepId, WorkflowExecution execution) throws IOException {

    if (WorkflowQueries.isStepComplete(stepId, execution)) {
      return StepStatus.SKIPPED;
    }

    return StepStatus.WAITING;
  }

  private WorkflowExecution getExecution(Application app, String name, AppType appType, String scopeId) throws IOException {

    Set<WorkflowExecution> incompleteExecutions = Sets.newHashSet();
    Records records = rldb.createQuery()
        .from(Application.TBL)
        .innerJoin(WorkflowExecution.TBL)
        .on(Application.ID.equalTo(WorkflowExecution.APPLICATION_ID))
        .where(Application.NAME.equalTo(name))
        .where(WorkflowExecution.SCOPE_IDENTIFIER.equalTo(scopeId))
        .where(WorkflowExecution.STATUS.equalTo(WorkflowExecutionStatus.INCOMPLETE.ordinal()))
        .fetch();

    for (Record record : records) {
      incompleteExecutions.add(new WorkflowExecution(JackUtil.getModel(WorkflowExecution.Attributes.class, record), databases));
    }

    if (incompleteExecutions.size() > 1) {
      throw new RuntimeException("Found multiple incomplete workflow executions!");
    }

    if (incompleteExecutions.isEmpty()) {
      LOG.info("No incomplete execution found, creating new execution");

      //  new execution
      WorkflowExecution ex = rldb.workflowExecutions().create(null, name, scopeId, WorkflowExecutionStatus.INCOMPLETE.ordinal(), Time.now(), null, app.getIntId());

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
