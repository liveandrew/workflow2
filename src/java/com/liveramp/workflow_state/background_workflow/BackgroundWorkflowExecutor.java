package com.liveramp.workflow_state.background_workflow;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.resource.ResourceDeclarerFactory;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.Accessors;
import com.liveramp.commons.collections.list.ListBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.liveramp.databases.workflow_db.IWorkflowDb;
import com.liveramp.databases.workflow_db.models.BackgroundAttemptInfo;
import com.liveramp.databases.workflow_db.models.BackgroundStepAttemptInfo;
import com.liveramp.databases.workflow_db.models.StepAttempt;
import com.liveramp.databases.workflow_db.models.StepDependency;
import com.liveramp.databases.workflow_db.models.WorkflowAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowExecution;
import com.liveramp.workflow.types.StepStatus;
import com.liveramp.workflow.types.WorkflowAttemptStatus;
import com.liveramp.workflow_core.WorkflowEnums;
import com.liveramp.workflow_core.background_workflow.BackgroundAction;
import com.liveramp.workflow_core.background_workflow.PreconditionFunction;
import com.liveramp.workflow_db_state.DbPersistence;
import com.liveramp.workflow_db_state.InitializedDbPersistence;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.jack.AttributesWithId;
import com.rapleaf.jack.queries.Column;
import com.rapleaf.jack.queries.Record;
import com.rapleaf.jack.queries.Records;
import com.rapleaf.jack.queries.Updates;
import com.rapleaf.jack.queries.where_operators.In;

public class BackgroundWorkflowExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(BackgroundWorkflowExecutor.class);

  private final IWorkflowDb workflowDb;
  private final List<String> runnableWorkflowNames;
  private boolean shouldShutdown = false;
  private final long pollDelay;
  private final String workerName;
  private final Thread pollerThread;

  public BackgroundWorkflowExecutor(List<String> runnableWorkflowNames, long pollDelayMs, String workerName) {
    this.workflowDb = new DatabasesImpl().getWorkflowDb();
    this.runnableWorkflowNames = runnableWorkflowNames;
    this.pollDelay = pollDelayMs;
    this.workerName = workerName;
    this.pollerThread = new Thread(new Runner());
  }

  public void stop() throws InterruptedException {
    shouldShutdown = true;
    pollerThread.interrupt();
    pollerThread.join();
  }

  public void start() {
    this.pollerThread.start();
  }

  public boolean claimStep(Long backgroundStepInfoId) throws IOException {

    Updates execute = workflowDb.createUpdate().table(BackgroundStepAttemptInfo.TBL)
        .where(BackgroundStepAttemptInfo.ID.equalTo(backgroundStepInfoId))
        .where(BackgroundStepAttemptInfo.CLAIMED_BY_WORKER.isNull())
        .set(BackgroundStepAttemptInfo.CLAIMED_BY_WORKER, workerName)
        .execute();

    return execute.getMatchedRowCount() > 0;
  }

  private class Runner implements Runnable {
    public void run() {

      try {
        IWorkflowDb workflowDb = new DatabasesImpl().getWorkflowDb();
        workflowDb.disableCaching();

        ExecutorService executorService = Executors.newCachedThreadPool();

        while (true) {

          if (shouldShutdown) {
            executorService.shutdown();
            executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
            break;
          }

          //  get all incomplete (running?) attempts for given apps

          Map<WorkflowAttempt.Attributes, BackgroundAttemptInfo.Attributes> incompleteAttempts = workflowDb.createQuery()
              .from(WorkflowExecution.TBL)
              .where(WorkflowExecution.NAME.in(Sets.newHashSet(runnableWorkflowNames)))
              .innerJoin(WorkflowAttempt.TBL)
              .on(WorkflowAttempt.WORKFLOW_EXECUTION_ID.equalTo(WorkflowExecution.ID.as(Integer.class)))
              .where(WorkflowAttempt.STATUS.in(WorkflowEnums.EXECUTING_ATTEMPT_STATUSES))
              .where(WorkflowAttempt.LAST_HEARTBEAT.equalTo(0L))
              .innerJoin(BackgroundAttemptInfo.TBL)
              .on(BackgroundAttemptInfo.WORKFLOW_ATTEMPT_ID.equalTo(WorkflowAttempt.ID))
              .select(new ListBuilder<Column>()
                  .addAll(WorkflowAttempt.TBL.getAllColumns())
                  .addAll(BackgroundAttemptInfo.TBL.getAllColumns())
                  .get())
              .fetch()
              .stream()
              .collect(Collectors.toMap(
                  record -> record.getAttributes(WorkflowAttempt.TBL),
                  record -> record.getAttributes(BackgroundAttemptInfo.TBL)
              ));

          //  TODO we can cache the dependency model for an attempt to reduce db churn

          Map<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> incompleteSteps = workflowDb.createQuery().from(StepAttempt.TBL)
              .where(StepAttempt.WORKFLOW_ATTEMPT_ID.in(incompleteAttempts.keySet().stream().map(attributes -> (int)attributes.getId()).collect(Collectors.toSet())))
              .innerJoin(BackgroundStepAttemptInfo.TBL)
              .on(BackgroundStepAttemptInfo.STEP_ATTEMPT_ID.equalTo(StepAttempt.ID))
              .select(new ListBuilder<Column>()
                  .addAll(StepAttempt.TBL.getAllColumns())
                  .add(BackgroundStepAttemptInfo.ID)
                  .add(BackgroundStepAttemptInfo.NEXT_EXECUTE_CHECK)
                  .add(BackgroundStepAttemptInfo.EXECUTE_CHECK_COOLDOWN_SECONDS)
                  .get())
              .fetch().stream()
              .collect(Collectors.toMap(
                  record -> record.getAttributes(StepAttempt.TBL),
                  record -> record.getAttributes(BackgroundStepAttemptInfo.TBL)
              ));

          List<StepDependency.Attributes> dependencies = workflowDb.createQuery().from(StepAttempt.TBL)
              .where(StepAttempt.ID.in(incompleteSteps.keySet().stream().map(AttributesWithId::getId).collect(Collectors.toList())))
              .innerJoin(StepDependency.TBL)
              .on(StepDependency.STEP_ATTEMPT_ID.equalTo(StepAttempt.ID))
              .fetch()
              .stream().map(record -> record.getAttributes(StepDependency.TBL))
              .collect(Collectors.toList());


          LiveAttemptCache cache = new LiveAttemptCache();

          for (Map.Entry<Long, AttemptState> attemptEntry : splitStepsByAttempt(incompleteAttempts.keySet(), incompleteSteps, dependencies).entrySet()) {
            Long workflowAttemptID = attemptEntry.getKey();

            //noinspection ConstantConditions
            AttemptState incompleteAttempt = attemptEntry.getValue();
            switch (WorkflowAttemptStatus.findByValue(incompleteAttempt.attributes.getStatus())) {

              case SHUTDOWN_PENDING:
                // TODO add support.  tricky synchronization
              case RUNNING:
              case FAIL_PENDING:

                long currentTime = System.currentTimeMillis();

                Map<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> startableSteps = incompleteAttempt.getStartableSteps();

                for (Map.Entry<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> startable : startableSteps.entrySet()) {

                  BackgroundStepAttemptInfo.Attributes info = startable.getValue();
                  StepAttempt.Attributes step = startable.getKey();

                  if (info.getNextExecuteCheck() <= currentTime) {

                    String actionClass = step.getActionClass();

                    //  TODO we should email / alert on this error
                    BackgroundAction action = (BackgroundAction)Class.forName(actionClass).newInstance();

                    Serializable context = (Serializable)SerializationUtils.deserialize(Accessors.only(workflowDb.createQuery().from(BackgroundStepAttemptInfo.TBL)
                        .where(BackgroundStepAttemptInfo.ID.equalTo(info.getId()))
                        .select(BackgroundStepAttemptInfo.SERIALIZED_CONTEXT)
                        .select(Sets.newHashSet(BackgroundStepAttemptInfo.SERIALIZED_CONTEXT))
                        .fetch()).get(BackgroundStepAttemptInfo.SERIALIZED_CONTEXT));

                    PreconditionFunction allowExecute = action.getAllowExecute();
                    Boolean canExecute = (Boolean)allowExecute.apply(context);

                    if (canExecute) {
                      //  if false, someone else got to it first -- this is fine, carry on
                      if (claimStep(info.getId())) {
                        WorkflowAttempt.Attributes attemptAttributes = attemptEntry.getValue().attributes;

                        executorService.submit(new StepRunnable(cache,
                            attemptAttributes,
                            incompleteAttempts.get(attemptAttributes),
                            step, action, context));
                      }
                    } else {
                      workflowDb.createUpdate().table(BackgroundStepAttemptInfo.TBL)
                          .where(BackgroundStepAttemptInfo.ID.equalTo(info.getId()))
                          .set(BackgroundStepAttemptInfo.NEXT_EXECUTE_CHECK, info.getExecuteCheckCooldownSeconds() * 1000 + currentTime)
                          .execute();
                    }
                  }
                }

                //  nothing is running and nothing can launch.  so we mark this as ded.
                if (incompleteAttempt.getRunningSteps().isEmpty() && startableSteps.isEmpty()) {
                  cache.getPersistence(workflowAttemptID).markWorkflowStopped();
                }

                //  TODO figure out fallthrough behavior if an executor dies during execution.  do we rely on the executor to figure out what it was
                //  running once it comes back up?  or do we monitor externally somehow.

                break;

            }


          }

          cache.shutdown();

          //  TODO more efficient
          //  1) batch up things we can potentially start
          //  2) as threads become free, try to start them
          //  3) if the start fails b/c it's already started, take the next one
          //      -
          //  4) when we run out of work to do, run a new query

          //  get step dependencies

          Thread.sleep(pollDelay);
        }


        //

        //  if an attempt has no more runnable steps not blocked by failures, attempt fails
      } catch (Exception e) {
        //  TODO real handling
        e.printStackTrace();
      }
    }


    private class StepRunnable implements Runnable {
      private final LiveAttemptCache cache;
      private final WorkflowAttempt.Attributes workflowAttempt;
      private final BackgroundAttemptInfo.Attributes backgroundInfo;
      private final StepAttempt.Attributes step;
      private final BackgroundAction action;
      private final Serializable context;

      public StepRunnable(LiveAttemptCache cache, WorkflowAttempt.Attributes attemptAttributes, BackgroundAttemptInfo.Attributes backgroundInfo, StepAttempt.Attributes step, BackgroundAction action, Serializable context) {
        this.cache = cache;
        this.workflowAttempt = attemptAttributes;
        this.backgroundInfo = backgroundInfo;
        this.step = step;
        this.action = action;
        this.context = context;
      }

      @Override
      public void run() {

        try {
          //  mark running

          WorkflowStatePersistence persistence = cache.getPersistence(workflowAttempt.getId());
          String token = step.getStepToken();

          persistence.markStepRunning(token);

          try {

            action.initializeContext(context);

            List<String> nameParts = Lists.newArrayList(token.split("__"));

            action.getActionId()
                .setParentPrefix(StringUtils.join(nameParts.subList(0, nameParts.size() - 1), "__"))
                .setRelativeName(Accessors.last(nameParts));

            action.setOptionObjects(
                persistence,
                cache.getManager(workflowAttempt, backgroundInfo),
                null, //  TODO realisticlly just sweep this (old resources)
                null // TODO flow submission controller + store locker.  important but only once we have the hadoopy version.
            );

            //  TODO finish creating property models in migration, use here
            action.internalExecute(new NestedProperties(Collections.emptyMap(), true));

            persistence.markStepCompleted(token);

          } catch (Exception e) {
            LOG.error("Execution of step " + token + " failed!", e);
            persistence.markStepFailed(token, e);
          }

          //  run


        } catch (Exception e) {

          //  TODO deal with this
          LOG.error("Internal error", e);

        }
      }

    }
  }

  private class LiveAttemptCache {


    Map<Long, WorkflowStatePersistence> cachedPersistences = Maps.newHashMap();
    Map<Long, ResourceManager> cachedManagers = Maps.newHashMap();

    public ResourceManager getManager(WorkflowAttempt.Attributes attempt, BackgroundAttemptInfo.Attributes info) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {

      long attemptId = attempt.getId();
      if(!cachedManagers.containsKey(attemptId)){
        String factoryClass = info.getResourceManagerFactory();
        ResourceDeclarerFactory factory = (ResourceDeclarerFactory) Class.forName(factoryClass).newInstance();
        cachedManagers.put(attemptId, factory.create().create(attempt.getWorkflowExecutionId(), InitializedDbPersistence.class.getName()));
      }

      return cachedManagers.get(attemptId);

    }

    public WorkflowStatePersistence getPersistence(Long workflowAttemptID) {
      if (!cachedPersistences.containsKey(workflowAttemptID)) {
        cachedPersistences.put(workflowAttemptID, DbPersistence.queryPersistence(workflowAttemptID, new DatabasesImpl().getWorkflowDb()));
      }
      return cachedPersistences.get(workflowAttemptID);
    }

    public void shutdown() throws IOException {
      for (WorkflowStatePersistence persistence : cachedPersistences.values()) {
        persistence.shutdown();
      }
    }

  }

  private Map<Long, AttemptState> splitStepsByAttempt(
      Collection<WorkflowAttempt.Attributes> attempts,
      Map<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> incompleteSteps,
      List<StepDependency.Attributes> dependencies) {

    Multimap<Long, StepAttempt.Attributes> stepAttemptsByWA = HashMultimap.create();

    Map<Long, StepAttempt.Attributes> stepsByID = Maps.newHashMap();
    for (Map.Entry<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> entry : incompleteSteps.entrySet()) {
      stepsByID.put(entry.getKey().getId(), entry.getKey());
    }

    for (StepAttempt.Attributes attributes : incompleteSteps.keySet()) {
      stepAttemptsByWA.put((long)attributes.getWorkflowAttemptId(), attributes);
    }

    Multimap<Long, StepDependency.Attributes> dependenciesByWA = HashMultimap.create();

    for (StepDependency.Attributes dependency : dependencies) {
      dependenciesByWA.put(
          (long)stepsByID.get(dependency.getStepAttemptId()).getWorkflowAttemptId(),
          dependency
      );
    }


    Map<Long, AttemptState> attemptStates = Maps.newHashMap();

    for (WorkflowAttempt.Attributes attempt : attempts) {

      Map<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> scopedInfos = Maps.newHashMap();
      for (StepAttempt.Attributes stepAttempt : stepAttemptsByWA.get(attempt.getId())) {
        scopedInfos.put(stepAttempt, incompleteSteps.get(stepAttempt));
      }

      DirectedGraph<Long, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);

      for (StepAttempt.Attributes attributes : stepAttemptsByWA.get(attempt.getId())) {
        graph.addVertex(attributes.getId());
      }

      for (StepDependency.Attributes edge : dependenciesByWA.get(attempt.getId())) {
        graph.addEdge(edge.getStepAttemptId(), edge.getDependencyAttemptId());
      }

      attemptStates.put(attempt.getId(), new AttemptState(attempt, scopedInfos, graph));

    }


    return attemptStates;

  }

  private class AttemptState {

    private final WorkflowAttempt.Attributes attributes;
    private final Map<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> incompleteSteps;
    private final Map<Long, StepAttempt.Attributes> stepsByID;
    private final DirectedGraph<Long, DefaultEdge> graph;

    public AttemptState(WorkflowAttempt.Attributes attributes,
                        Map<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> incompleteSteps,
                        DirectedGraph<Long, DefaultEdge> graph) {

      Map<Long, StepAttempt.Attributes> steps = Maps.newHashMap();
      for (StepAttempt.Attributes step : incompleteSteps.keySet()) {
        steps.put(step.getId(), step);
      }

      this.attributes = attributes;
      this.incompleteSteps = incompleteSteps;
      this.stepsByID = steps;
      this.graph = graph;
    }

    public Map<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> getRunningSteps() {

      Map<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> running = Maps.newHashMap();
      for (Map.Entry<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> entry : incompleteSteps.entrySet()) {
        if (entry.getKey().getStepStatus() == StepStatus.RUNNING.getValue()) {
          running.put(entry.getKey(), entry.getValue());
        }
      }

      return running;
    }

    public Map<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> getStartableSteps() {

      Map<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> startable = Maps.newHashMap();
      for (Map.Entry<StepAttempt.Attributes, BackgroundStepAttemptInfo.Attributes> entry : incompleteSteps.entrySet()) {
        if (isStartable(entry.getKey())) {
          startable.put(entry.getKey(), entry.getValue());
        }
      }

      return startable;

    }

    private boolean isStartable(StepAttempt.Attributes step) {

      if (step.getStepStatus() != StepStatus.WAITING.ordinal()) {
        return false;
      }

      for (DefaultEdge dependency : graph.outgoingEdgesOf(step.getId())) {
        StepAttempt.Attributes attributes = stepsByID.get(graph.getEdgeTarget(dependency));
        if (!WorkflowEnums.NON_BLOCKING_STEP_STATUSES.contains(StepStatus.findByValue(attributes.getStepStatus()))) {
          return false;
        }
      }

      return true;

    }

  }

}
