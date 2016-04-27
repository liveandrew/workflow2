package com.rapleaf.cascading_ext.workflow2.state;

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

import com.liveramp.commons.Accessors;
import com.liveramp.db_utils.BaseJackUtil;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipient;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.liveramp.workflow_state.Assertions;
import com.liveramp.workflow_state.AttemptStatus;
import com.liveramp.workflow_state.DSAction;
import com.liveramp.workflow_state.DbPersistence;
import com.liveramp.workflow_state.StepStatus;
import com.liveramp.workflow_state.WorkflowExecutionStatus;
import com.liveramp.workflow_state.WorkflowQueries;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.Application;
import com.rapleaf.db_schemas.rldb.models.ApplicationConfiguredNotification;
import com.rapleaf.db_schemas.rldb.models.ConfiguredNotification;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttemptConfiguredNotification;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttemptDatastore;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.jack.queries.Record;
import com.rapleaf.jack.queries.Records;

public class DbPersistenceFactory implements WorkflowPersistenceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DbPersistence.class);

  public DbPersistenceFactory() {}

  @Override
  public synchronized DbPersistence prepare(DirectedGraph<Step, DefaultEdge> flatSteps,
                                            String name,
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
                                            String implementationBuild) {

    try {

      DatabasesImpl databases = new DatabasesImpl();
      IRlDb rldb = databases.getRlDb();
      rldb.disableCaching();

      Application application = getApplication(databases, name, appType);
      LOG.info("Using application: " + application);

      WorkflowExecution execution = getExecution(databases, application, name, appType, scopeId);
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

      assertOnlyLiveAttempt(rldb, execution, attempt);

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

        StepAttempt stepAttempt = createStepAttempt(databases, step, attempt, execution);
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

      return DbPersistence.runPersistence(rldb, workflowAttemptId, providedHandler);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private Application getApplication(IDatabases databases, String name, AppType appType) throws IOException {
    IRlDb rldb = databases.getRlDb();

    Optional<Application> application = Accessors.firstOrAbsent(rldb.applications().findByName(name));

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

  private void cleanUpRunningAttempts(IDatabases databases, WorkflowExecution execution) throws IOException {
    IRlDb rldb = databases.getRlDb();

    List<WorkflowAttempt> prevAttempts = WorkflowQueries.getLiveWorkflowAttempts(rldb, execution.getId());
    LOG.info("Found previous attempts: " + prevAttempts);

    for (WorkflowAttempt attempt : prevAttempts) {

      //  check to see if any of these workflows are still marked as alive
      Assertions.assertDead(attempt);

      //  otherwise it is safe to clean up
      LOG.info("Marking old running attempt as FAILED: " + attempt);
      attempt
          .setStatus(AttemptStatus.FAILED.ordinal())
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

  public void assertOnlyLiveAttempt(IRlDb rldb, WorkflowExecution execution, WorkflowAttempt attempt) throws IOException {
    List<WorkflowAttempt> liveAttempts = WorkflowQueries.getLiveWorkflowAttempts(rldb, execution.getId());
    if (liveAttempts.size() != 1) {
      attempt.setStatus(AttemptStatus.FAILED.ordinal()).save();
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

  private WorkflowAttempt createAttempt(IDatabases databases, String host, String username, String description, String pool, String priority, String launchDir, String launchJar, AlertsHandler providedHandler, Set<WorkflowRunnerNotification> configuredNotifications, WorkflowExecution execution, String remote, String implementationBuild) throws IOException {
    IRlDb rldb = databases.getRlDb();

    Map<AlertSeverity, String> recipients = Maps.newHashMap();
    for (AlertSeverity severity : AlertSeverity.values()) {
      recipients.put(severity, getEmail(providedHandler, AlertRecipients.engineering(severity)));
    }

    WorkflowAttempt attempt = rldb.workflowAttempts().create((int)execution.getId(), username, priority, pool, host)
        .setStatus(AttemptStatus.INITIALIZING.ordinal())
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

  private ConfiguredNotification buildConfiguredNotification(IRlDb rldb, WorkflowRunnerNotification notification, String emailForSeverity) throws IOException {
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


  private StepAttempt createStepAttempt(IDatabases databases, Step step, WorkflowAttempt attempt, WorkflowExecution execution) throws IOException {
    IRlDb rldb = databases.getRlDb();

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

  private WorkflowExecution getExecution(IDatabases databases, Application app, String name, AppType appType, String scopeId) throws IOException {
    IRlDb rldb = databases.getRlDb();

    Set<WorkflowExecution> incompleteExecutions = Sets.newHashSet();
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
      incompleteExecutions.add(new WorkflowExecution(BaseJackUtil.getModel(WorkflowExecution.Attributes.class, record), databases));
    }

    if (incompleteExecutions.size() > 1) {
      throw new RuntimeException("Found multiple incomplete workflow executions!");
    }

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

      return ex;

    } else {
      //  only one, return it
      WorkflowExecution prevExecution = incompleteExecutions.iterator().next();
      LOG.info("Found previous execution " + prevExecution + " id " + prevExecution.getId());

      return prevExecution;

    }

  }


}
