package com.liveramp.workflow.state;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.util.Time;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.databases.workflow_db.IWorkflowDb;
import com.liveramp.databases.workflow_db.models.Application;
import com.liveramp.databases.workflow_db.models.ApplicationConfiguredNotification;
import com.liveramp.databases.workflow_db.models.ConfiguredNotification;
import com.liveramp.databases.workflow_db.models.StepAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowAttemptConfiguredNotification;
import com.liveramp.databases.workflow_db.models.WorkflowAttemptDatastore;
import com.liveramp.databases.workflow_db.models.WorkflowExecution;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipient;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.liveramp.workflow_db_state.Assertions;
import com.liveramp.workflow_db_state.DbPersistence;
import com.liveramp.workflow_db_state.InitializedDbPersistence;
import com.liveramp.workflow_db_state.WorkflowQueries;
import com.liveramp.workflow_state.DSAction;
import com.liveramp.workflow_state.DataStoreInfo;
import com.liveramp.workflow_state.IStep;
import com.liveramp.workflow_state.StepStatus;
import com.liveramp.workflow_state.WorkflowExecutionStatus;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.types.person_data.WorkflowAttemptStatus;

public class WorkflowDbPersistenceFactory extends WorkflowPersistenceFactory<InitializedDbPersistence> {
  private static final Logger LOG = LoggerFactory.getLogger(DbPersistence.class);

  public WorkflowDbPersistenceFactory() {
  }

  @Override
  public synchronized InitializedDbPersistence initializeInternal(String name,
                                                                  String scopeId,
                                                                  String description,
                                                                  AppType appType,
                                                                  String host,
                                                                  String username,
                                                                  String pool,
                                                                  String priority,
                                                                  String launchDir,
                                                                  String launchJar,
                                                                  Set<WorkflowRunnerNotification> configuredNotifications,
                                                                  AlertsHandler providedHandler,
                                                                  String remote,
                                                                  String implementationBuild) throws IOException {


    DatabasesImpl databases = new DatabasesImpl();
    IWorkflowDb workflowDb = databases.getWorkflowDb();
    workflowDb.disableCaching();

    //  first, make sure there is no incomplete execution in the old DB
    assertMigrationSafety(name, scopeId);

    Application application = getApplication(workflowDb, name, appType);
    LOG.info("Using application: " + application);

    WorkflowExecution.Attributes execution = getExecution(workflowDb, application, name, appType, scopeId);
    LOG.info("Using workflow execution: " + execution + " id " + execution.getId());

    cleanUpRunningAttempts(databases, execution);

    WorkflowAttempt attempt = createAttempt(databases,
        host,
        username,
        description,
        pool,
        priority,
        launchDir,
        launchJar,
        providedHandler,
        configuredNotifications,
        execution,
        remote,
        implementationBuild
    );

    assertOnlyLiveAttempt(workflowDb, execution, attempt);

    long workflowAttemptId = attempt.getId();
    LOG.info("Using new attempt: " + attempt + " id " + workflowAttemptId);

    return new InitializedDbPersistence(attempt.getId(), workflowDb, true, providedHandler);
  }

  @Override
  public synchronized DbPersistence prepare(InitializedDbPersistence persistence, DirectedGraph<IStep, DefaultEdge> flatSteps) {
    IWorkflowDb rldb = persistence.getDb();

    try {
      rldb.setAutoCommit(false);

      long workflowAttemptId = persistence.getAttemptId();
      WorkflowAttempt attempt = persistence.getAttempt();
      WorkflowExecution execution = persistence.getExecution();
      Set<DataStoreInfo> allStores = Sets.newHashSet();

      for (IStep step : flatSteps.vertexSet()) {
        for (DataStoreInfo store : step.getDataStores().values()) {
          allStores.add(store);
        }
      }

      Map<DataStoreInfo, WorkflowAttemptDatastore> datastores = Maps.newHashMap();
      for (DataStoreInfo store : allStores) {
        datastores.put(store, rldb.workflowAttemptDatastores().create(
            (int)workflowAttemptId,
            store.getName(),
            store.getPath(),
            store.getClass().getName()
        ));
      }

      //  save datastores
      rldb.commit();

      Map<String, StepAttempt> attempts = Maps.newHashMap();
      for (IStep step : flatSteps.vertexSet()) {

        StepAttempt stepAttempt = createStepAttempt(rldb, step, attempt, execution);
        attempts.put(stepAttempt.getStepToken(), stepAttempt);
      }

      //  save created steps
      rldb.commit();;

      for (IStep step : flatSteps.vertexSet()) {
        StepAttempt stepAttempt = attempts.get(step.getCheckpointToken());

        for (Map.Entry<DSAction, DataStoreInfo> entry : step.getDataStores().entries()) {
          rldb.stepAttemptDatastores().create(
              (int)stepAttempt.getId(),
              (int)datastores.get(entry.getValue()).getId(),
              entry.getKey().ordinal()
          );
        }

      }

      //  save refs to steps
      rldb.commit();

      for (DefaultEdge edge : flatSteps.edgeSet()) {

        IStep dep = flatSteps.getEdgeTarget(edge);
        IStep step = flatSteps.getEdgeSource(edge);

        rldb.stepDependencies().create(
            (int)attempts.get(step.getCheckpointToken()).getId(),
            (int)attempts.get(dep.getCheckpointToken()).getId()
        );

      }

      //  save step deps
      rldb.commit();

      return new DbPersistence(persistence);

    } catch (Exception e) {
      rldb.rollback();
      throw new RuntimeException(e);
    }finally {
      rldb.setAutoCommit(true);
    }

  }

  private void assertMigrationSafety(String name, String scopeId) throws IOException {

    IRlDb rlDb = new com.rapleaf.db_schemas.DatabasesImpl().getRlDb();

    Optional<com.rapleaf.db_schemas.rldb.models.Application> app = com.liveramp.workflow_state.WorkflowQueries.getApplication(rlDb, name);

    //  new app -- totally fine to run in new DB
    if(!app.isPresent()){
      return;
    }

    Set<com.rapleaf.db_schemas.rldb.models.WorkflowExecution.Attributes> incomplete = com.liveramp.workflow_state.WorkflowQueries.getIncompleteExecutions(rlDb,
        name,
        scopeId
    );

    if(!incomplete.isEmpty()){
      throw new RuntimeException("Will not launch workflow using workflow_db because executions are incomplete in rldb: "+incomplete);
    }

  }

  private Application getApplication(IWorkflowDb rldb, String name, AppType appType) throws IOException {

    Optional<Application> application = WorkflowQueries.getApplication(rldb, name);

    if (application.isPresent()) {
      LOG.info("Using existing application");

      return application.get();
    } else {
      LOG.info("Creating new application for name: " + name + ", app type " + appType);

      Application app = rldb.applications().create(name);
      if (appType != null) {
        app.setAppType(appType.getValue());
      }
      rldb.applications().save(app);

      //  add DT to the performance notifications for all new applications

      ConfiguredNotification dtNotification = rldb.configuredNotifications()
          .create(WorkflowRunnerNotification.PERFORMANCE.ordinal())
          .setEmail("dt-workflow-alerts@liveramp.com");
      dtNotification.save();

      ApplicationConfiguredNotification appNotif = rldb.applicationConfiguredNotifications()
          .create(app.getId(), dtNotification.getId());
      appNotif.save();

      return app;
    }

  }

  private void cleanUpRunningAttempts(IDatabases databases, WorkflowExecution.Attributes execution) throws IOException {
    IWorkflowDb rldb = databases.getWorkflowDb();

    List<WorkflowAttempt> prevAttempts = WorkflowQueries.getLiveWorkflowAttempts(rldb, execution.getId());
    LOG.info("Found previous attempts: " + prevAttempts);

    for (WorkflowAttempt attempt : prevAttempts) {

      //  check to see if any of these workflows are still marked as alive
      Assertions.assertDead(attempt);

      //  otherwise it is safe to clean up
      LOG.info("Marking old running attempt as FAILED: " + attempt);
      attempt
          .setStatus(WorkflowAttemptStatus.FAILED.ordinal())
          .setEndTime(System.currentTimeMillis())
          .save();

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

  public void assertOnlyLiveAttempt(IWorkflowDb rldb, WorkflowExecution.Attributes execution, WorkflowAttempt attempt) throws IOException {
    List<WorkflowAttempt> liveAttempts = WorkflowQueries.getLiveWorkflowAttempts(rldb, execution.getId());
    if (liveAttempts.size() != 1) {
      attempt.setStatus(WorkflowAttemptStatus.FAILED.ordinal()).save();
      throw new RuntimeException("Multiple live attempts found for workflow execution! " + liveAttempts + " Not starting workflow.");
    }
  }

  private static final Set<WorkflowRunnerNotification> PROVIDED_HANDLER_NOTIFICATIONS = Sets.newHashSet(
      WorkflowRunnerNotification.START,
      WorkflowRunnerNotification.SUCCESS,
      WorkflowRunnerNotification.FAILURE,
      WorkflowRunnerNotification.STEP_FAILURE,
      WorkflowRunnerNotification.SHUTDOWN,
      WorkflowRunnerNotification.INTERNAL_ERROR
  );

  private WorkflowAttempt createAttempt(IDatabases databases, String host, String username, String description, String pool, String priority, String launchDir, String launchJar, AlertsHandler providedHandler, Set<WorkflowRunnerNotification> configuredNotifications, WorkflowExecution.Attributes execution, String remote, String implementationBuild) throws IOException {
    IWorkflowDb rldb = databases.getWorkflowDb();

    Map<AlertSeverity, String> recipients = Maps.newHashMap();
    for (AlertSeverity severity : AlertSeverity.values()) {
      recipients.put(severity, getEmail(providedHandler, AlertRecipients.engineering(severity)));
    }

    WorkflowAttempt attempt = rldb.workflowAttempts().create((int)execution.getId(), username, priority, pool, host)
        .setStatus(WorkflowAttemptStatus.INITIALIZING.ordinal())
        .setDescription(description)
        .setLaunchDir(launchDir)
        .setLaunchJar(launchJar)
        .setErrorEmail(recipients.get(AlertSeverity.ERROR))     //  TODO remove these on attempt once notifications redone
        .setInfoEmail(recipients.get(AlertSeverity.INFO))
        .setLastHeartbeat(System.currentTimeMillis())
        .setScmRemote(remote)
        .setCommitRevision(implementationBuild);
    attempt.save();


    for (WorkflowRunnerNotification notification : configuredNotifications) {

      ConfiguredNotification configured = buildConfiguredNotification(rldb, notification, recipients.get(notification.serverity()));
      if (configured != null) {
        configured.save();

        WorkflowAttemptConfiguredNotification attemptConfigured = rldb.workflowAttemptConfiguredNotifications().create(attempt.getId(), configured.getId());
        attemptConfigured.save();

      }
    }

    return attempt;
  }

  private ConfiguredNotification buildConfiguredNotification(IWorkflowDb rldb, WorkflowRunnerNotification notification, String emailForSeverity) throws IOException {
    if (PROVIDED_HANDLER_NOTIFICATIONS.contains(notification)) {
      return rldb.configuredNotifications().create(notification.ordinal()).setProvidedAlertsHandler(true);
    } else {
      if (emailForSeverity != null) {
        return rldb.configuredNotifications().create(notification.ordinal()).setEmail(emailForSeverity);
      }
    }

    return null;
  }


  private static String getEmail(AlertsHandler handler, AlertRecipient recipient) {
    List<String> emails = handler.resolveRecipients(Lists.newArrayList(recipient)).getEmailRecipients();
    if (emails.isEmpty()) {
      return null;
    }
    return emails.get(0);
  }


  private StepAttempt createStepAttempt(IWorkflowDb rldb, IStep step, WorkflowAttempt attempt, WorkflowExecution execution) throws IOException {

    String token = step.getCheckpointToken();

    return rldb.stepAttempts().create((int)attempt.getId(), token, null, null,
        getInitialStatus(token, execution).ordinal(),
        null,
        null,
        step.getActionClass(),
        ""
    );

  }

  private StepStatus getInitialStatus(String stepId, WorkflowExecution execution) throws IOException {

    if (WorkflowQueries.isStepComplete(stepId, execution)) {
      return StepStatus.SKIPPED;
    }

    return StepStatus.WAITING;
  }

  private WorkflowExecution.Attributes getExecution(IWorkflowDb rldb, Application app, String name, AppType appType, String scopeId) throws IOException {

    Set<WorkflowExecution.Attributes> incompleteExecutions = WorkflowQueries.getIncompleteExecutions(rldb, name, scopeId);

    if (incompleteExecutions.isEmpty()) {
      LOG.info("No incomplete execution found, creating new execution");

      //  new execution
      WorkflowExecution ex = rldb.workflowExecutions().create(name, WorkflowExecutionStatus.INCOMPLETE.ordinal())
          .setScopeIdentifier(scopeId)
          .setStartTime(Time.now())
          .setApplicationId(app.getIntId());

      if (appType != null) {
        ex.setAppType(appType.getValue());
      }

      ex.save();

      return ex.getAttributes();

    } else {
      //  only one, return it
      WorkflowExecution.Attributes prevExecution = incompleteExecutions.iterator().next();
      LOG.info("Found previous execution " + prevExecution + " id " + prevExecution.getId());

      return prevExecution;

    }

  }


}