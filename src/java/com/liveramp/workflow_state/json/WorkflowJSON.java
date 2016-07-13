package com.liveramp.workflow_state.json;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.liveramp.commons.Accessors;
import com.liveramp.commons.collections.map.MapBuilder;
import com.liveramp.db_utils.BaseJackUtil;
import com.liveramp.workflow_state.DSAction;
import com.liveramp.workflow_state.DbPersistence;
import com.liveramp.workflow_state.ProcessStatus;
import com.liveramp.workflow_state.StepStatus;
import com.liveramp.workflow_state.WorkflowExecutionStatus;
import com.liveramp.workflow_state.WorkflowQueries;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.Application;
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
import com.rapleaf.types.person_data.WorkflowAttemptStatus;

public class WorkflowJSON {

  public static JSONObject getDbJSONState(IRlDb rldb, DbPersistence persistence) throws JSONException, IOException {

    DirectedGraph<Long, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);

    long workflowAttemptId = persistence.getAttemptId();

    List<StepAttempt.Attributes> attempts = WorkflowQueries.getStepAttempts(rldb,
        workflowAttemptId
    );

    Map<Long, StepAttempt.Attributes> attemptsById = Maps.newHashMap();
    for (StepAttempt.Attributes attempt : attempts) {
      graph.addVertex(attempt.getId());
      attemptsById.put(attempt.getId(), attempt);
    }

    for (StepDependency.Attributes dependency : WorkflowQueries.getStepDependencies(rldb, attemptsById.keySet())) {
      graph.addEdge((long)dependency.getDependencyAttemptId(), (long)dependency.getStepAttemptId());
    }

    removeRedundantEdges(graph);

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

    List<MapreduceJobTaskException.Attributes> exceptions = WorkflowQueries.getMapreduceJobTaskExceptions(rldb,
        jobIds
    );

    Multimap<Long, MapreduceJobTaskException.Attributes> taskExceptionsByJobId = HashMultimap.create();

    for (MapreduceJobTaskException.Attributes exception : exceptions) {
     taskExceptionsByJobId.put((long) exception.getMapreduceJobId(), exception);
    }
    
    List<StepAttemptDatastore.Attributes> storeUsages = WorkflowQueries.getStepAttemptDatastores(rldb,
        attemptsById.keySet()
    );

    List<WorkflowAttemptDatastore.Attributes> stores = WorkflowQueries.getWorkflowAttemptDatastores(rldb,
        null,
        workflowAttemptId
    );

    TopologicalOrderIterator<Long, DefaultEdge> iter = new TopologicalOrderIterator<>(graph);

    JSONArray steps = new JSONArray();
    JSONArray edges = new JSONArray();

    Multimap<String, String> allEdges = HashMultimap.create();
    Map<Long, Integer> stepIdToIndex = Maps.newHashMap();

    int nodeIndex = 0;
    while (iter.hasNext()) {
      Long stepId = iter.next();

      StepAttempt.Attributes step = attemptsById.get(stepId);
      stepIdToIndex.put(stepId, nodeIndex);

      Map<String, JSONObject> mapReduceJobs = Maps.newHashMap();
      for (MapreduceJob.Attributes mapreduceJob : jobsByStepId.get(stepId)) {
        long jobID = mapreduceJob.getId();
        Collection<MapreduceCounter.Attributes> stepCounters = countersByJobId.get(jobID);

        mapReduceJobs.put(mapreduceJob.getJobIdentifier(),
             new JSONObject()
                .put("job_id", mapreduceJob.getJobIdentifier())
                .put("job_name", mapreduceJob.getJobName())
                .put("tracking_url", mapreduceJob.getTrackingUrl())
                .put("counters", toJSONDb(stepCounters))
                .put("task_exceptions", toJSON(taskExceptionsByJobId.get(jobID))));
      }

      steps.put(new JSONObject()
              .put("id", step.getStepToken())
              .put("index", nodeIndex)
              .put("status", StepStatus.findByValue(step.getStepStatus()).name().toLowerCase())
              .put("start_timestamp", safeTime(step.getStartTime()))
              .put("end_timestamp", safeTime(step.getEndTime()))
              .put("message", safeStr(step.getStatusMessage()))
              .put("action_name", safeStr(step.getActionClass()))
              .put("mapreduce_jobs", mapReduceJobs)
              .put("failure_message", step.getFailureCause())
              .put("failure_trace", step.getFailureTrace())
      );

      for (DefaultEdge inEdge : graph.incomingEdgesOf(stepId)) {
        Long source = graph.getEdgeSource(inEdge);
        allEdges.put(attemptsById.get(source).getStepToken(), attemptsById.get(stepId).getStepToken());
      }

      nodeIndex++;
    }

    JSONArray dsConnections = new JSONArray();

    for (StepAttemptDatastore.Attributes storeUse : storeUsages) {
      dsConnections.put(new JSONObject()
          .put("step", stepIdToIndex.get((long)storeUse.getStepAttemptId()))
          .put("datastore", storeUse.getWorkflowAttemptDatastoreId())
          .put("connection", DSAction.findByValue(storeUse.getDsAction())));
    }

    JSONArray storeJson = new JSONArray();

    for (WorkflowAttemptDatastore.Attributes datastore : stores) {
      storeJson.put(new JSONObject()
          .put("index", datastore.getId())
          .put("name", datastore.getName())
          .put("path", datastore.getPath())
          .put("type", datastore.getClassName()));
    }

    for (DefaultEdge edge : graph.edgeSet()) {

      Long source = graph.getEdgeSource(edge);
      Long target = graph.getEdgeTarget(edge);

      edges.put(new JSONObject()
          .put("source", stepIdToIndex.get(source))
          .put("target", stepIdToIndex.get(target)));
    }

    WorkflowAttempt attempt = rldb.workflowAttempts().find(workflowAttemptId);
    WorkflowExecution execution = attempt.getWorkflowExecution();

    JSONObject toReturn = new JSONObject()
        .put("name", execution.getName())
        .put("description", nullStr(attempt.getDescription()))
        .put("host", attempt.getHost())
        .put("id", attempt.getId())
        .put("username", attempt.getSystemUser())
        .put("shutdown_reason", attempt.getShutdownReason())
        .put("priority", attempt.getPriority())
        .put("pool", WorkflowQueries.getPool(attempt, execution))
        .put("edges", edges)
        .put("datastore_uses", dsConnections)
        .put("datastores", storeJson)
        .put("steps", steps);

    toReturn.put("process_status", WorkflowQueries.getProcessStatus(attempt, execution));
    toReturn.put("is_latest_execution", WorkflowQueries.isLatestExecution(rldb, execution));
    toReturn.put("status", WorkflowAttemptStatus.findByValue(attempt.getStatus()));
    toReturn.put("execution_status", WorkflowExecutionStatus.findByValue(execution.getStatus()));

    toReturn.put("execution", toJSON(execution.getAttributes()))
        .put("num_attempts", execution.getWorkflowAttempt().size());

    return toReturn;

  }

  private static String nullStr(String item){
    if (item == null) {
      return "";
    }
    return item;
  }

  private static Long safeTime(Long time) {
    if (time == null) {
      return 0l;
    }
    return time;
  }

  private static String safeStr(Object obj) {
    if (obj == null) {
      return "";
    }
    return obj.toString();
  }

  public static JSONObject toJSON(IRlDb rldb, boolean details, String processStatusFilter, WorkflowExecution execution, Collection<WorkflowAttempt> atts) throws IOException, JSONException {
    List<WorkflowAttempt> attempts = BaseJackUtil.sortDescending(atts);

    if (!attempts.isEmpty()) {
      WorkflowAttempt attempt = Accessors.first(attempts);

      JSONArray attemptsArray = new JSONArray();
      for (WorkflowAttempt workflowAttempt : attempts) {
        attemptsArray.put(WorkflowJSON.toJSON(workflowAttempt.getAttributes())
            .put("process_status", WorkflowQueries.getProcessStatus(workflowAttempt, execution)));
      }

      ProcessStatus latestAttemptStatus = WorkflowQueries.getProcessStatus(attempt, execution);

      if (processStatusFilter == null || latestAttemptStatus.name().equals(processStatusFilter)) {

        JSONObject data = new JSONObject()
            .put("attempts", attemptsArray)
            .put("execution", WorkflowJSON.toJSON(execution.getAttributes()));

        //  these require extra queries.  don't do it if we're returning long lists of executions
        if (details) {

          JSONArray notifications = new JSONArray();
          for (ConfiguredNotification.Attributes attributes : WorkflowQueries.getExecutionNotifications(rldb, execution.getId())) {
            notifications.put(toJSON(attributes));
          }

          data.put("can_cancel", WorkflowQueries.canRevert(rldb, execution))
              .put("configured_notifications", notifications);

        }

        return data;
      }

    }
    return null;
  }



  private static JSONArray toJSONDb(Collection<MapreduceCounter.Attributes> counters) throws JSONException {
    JSONArray array = new JSONArray();
    for (MapreduceCounter.Attributes counter : counters) {
      array.put(new JSONObject()
          .put("group", counter.getGroup())
          .put("name", counter.getName())
          .put("value", counter.getValue()));
    }
    return array;
  }

  private static void removeRedundantEdges(DirectedGraph<Long, DefaultEdge> graph) {
    for (Long vertex : graph.vertexSet()) {
      Set<Long> firstDegDeps = Sets.newHashSet();
      Set<Long> secondPlusDegDeps = Sets.newHashSet();
      for (DefaultEdge edge : graph.outgoingEdgesOf(vertex)) {
        Long depVertex = graph.getEdgeTarget(edge);
        firstDegDeps.add(depVertex);
        getOutgoingVerticesRecursive(depVertex, secondPlusDegDeps, graph);
      }

      for (Long firstDegDep : firstDegDeps) {
        if (secondPlusDegDeps.contains(firstDegDep)) {
          graph.removeAllEdges(vertex, firstDegDep);
        }
      }
    }
  }

  private static void getOutgoingVerticesRecursive(Long vertex, Set<Long> results, DirectedGraph<Long, DefaultEdge> graph) {
    for (DefaultEdge edge : graph.outgoingEdgesOf(vertex)) {
      Long s = graph.getEdgeTarget(edge);
      if (!results.contains(s)) {
        results.add(s);
        getOutgoingVerticesRecursive(s, results, graph);
      }
    }
  }

  public static String getShutdownReason(String provided) {
    if (provided == null || provided.equals("")) {
      return "No reason provided.";
    } else {
      return provided;
    }
  }

  public static JSONArray toJSON(Collection<MapreduceJobTaskException.Attributes> exceptions){
    JSONArray array = new JSONArray();
    for (MapreduceJobTaskException.Attributes exception : exceptions) {
      array.put(toJSON(exception));
    }
    return array;
  }

  public static JSONObject toJSON(MapreduceJobTaskException.Attributes exception){
    return BaseJackUtil.toJSON(exception, Collections.<MapreduceJobTaskException._Fields, Class<? extends Enum>>emptyMap(), "");
  }

  public static JSONObject toJSON(Application.Attributes notification) {
    return BaseJackUtil.toJSON(notification, Collections.<Enum, Class<? extends Enum>>emptyMap(), "");
  }

  public static JSONObject toJSON(ConfiguredNotification.Attributes notification) {
    return BaseJackUtil.toJSON(notification, MapBuilder.<ConfiguredNotification._Fields, Class<? extends Enum>>of(ConfiguredNotification._Fields.workflow_runner_notification, WorkflowRunnerNotification.class).get(), "");
  }

  public static JSONObject toJSON(WorkflowAttempt.Attributes attempt) {
    return BaseJackUtil.toJSON(attempt, MapBuilder.<WorkflowAttempt._Fields, Class<? extends Enum>>of(WorkflowAttempt._Fields.status, WorkflowAttemptStatus.class).get(), "");
  }

  public static JSONObject toJSON(WorkflowExecution.Attributes execution) {
    return BaseJackUtil.toJSON(execution, MapBuilder.<WorkflowExecution._Fields, Class<? extends Enum>>of(WorkflowExecution._Fields.status, WorkflowExecutionStatus.class).get(), "");
  }

  public static JSONObject toJSON(StepAttempt.Attributes attempt) {
    return BaseJackUtil.toJSON(attempt, MapBuilder.<StepAttempt._Fields, Class<? extends Enum>>of(StepAttempt._Fields.step_status, StepStatus.class).get(), "");
  }
}
