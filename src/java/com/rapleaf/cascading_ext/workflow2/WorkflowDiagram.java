package com.rapleaf.cascading_ext.workflow2;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.liveramp.types.workflow.LiveWorkflowMeta;
import com.liveramp.workflow_service.generated.StepDefinition;
import com.liveramp.workflow_service.generated.StepExecuteStatus;
import com.liveramp.workflow_service.generated.WorkflowDefinition;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.support.event_timer.EventTimer;


public class WorkflowDiagram {

  public static class Vertex {
    private String id;
    private String name;
    private String status;
    private long startTimestamp;
    private long endTimestamp;
    private String message;
    private String actionName;
    private Map<String, String> statusLinks;

    public Vertex(Step step, StepExecuteStatus._Fields status) {
      this.id = step.getCheckpointToken();
      this.name = step.getSimpleCheckpointToken();
      this.status = status.name().toLowerCase();

      statusLinks = step.getAction().getStatusLinks();

      Action action = step.getAction();
      actionName = action.getClass().getSimpleName();
      if (action instanceof MultiStepAction) {
        message = "";
        startTimestamp = computeStartTimestamp((MultiStepAction)action);
        endTimestamp = computeEndTimestamp((MultiStepAction)action);
      } else {
        message = action.getStatusMessage();
        startTimestamp = action.getStartTimestamp();
        endTimestamp = action.getEndTimestamp();
      }
    }

    private long computeStartTimestamp(MultiStepAction action) {
      long best = Long.MAX_VALUE;
      for (Step substep : action.getSubSteps()) {
        Action subAction = substep.getAction();
        if (subAction instanceof MultiStepAction) {
          best = Math.min(best, computeStartTimestamp((MultiStepAction)subAction));
        } else {
          long ts = subAction.getStartTimestamp();
          if (ts != 0) {
            best = Math.min(best, ts);
          }
        }
      }
      return best;
    }

    private long computeEndTimestamp(MultiStepAction action) {
      long best = Long.MIN_VALUE;
      for (Step substep : action.getSubSteps()) {
        Action subAction = substep.getAction();
        if (subAction instanceof MultiStepAction) {
          best = Math.max(best, computeEndTimestamp((MultiStepAction)subAction));
        } else {
          best = Math.max(best, subAction.getEndTimestamp());
        }
      }
      return best;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public String getStatus() {
      return status;
    }

    public String getId() {
      return id;
    }

    public long getStartTimestamp() {
      return startTimestamp;
    }

    public long getEndTimestamp() {
      return endTimestamp;
    }

    public String getMessage() {
      return message;
    }

    public Map<String, String> getStatusLinks() {
      return statusLinks;
    }

    public String getActionName() {
      return actionName;
    }
  }

  private final WorkflowRunner workflowRunner;
  private Set<String> multiStepsIds;
  private Map<String, Step> vertexIdToStep;
  private Map<String, String> vertexIdToParentVertexId;

  private Set<Step> multiStepsToExpand;
  private Stack<Step> isolated = new Stack<Step>();

  public WorkflowDiagram(WorkflowRunner workflowRunner) {
    this.workflowRunner = workflowRunner;
    this.multiStepsToExpand = new HashSet<Step>();
    populateVertexMappings();
  }

  private void populateVertexMappings() {
    Set<Step> tailSteps = workflowRunner.getTailSteps();
    multiStepsIds = new HashSet<String>();
    vertexIdToStep = new HashMap<String, Step>();
    vertexIdToParentVertexId = new HashMap<String, String>();
    Queue<Step> toProcess = new LinkedList<Step>(tailSteps);

    while (!toProcess.isEmpty()) {
      Step step = toProcess.poll();

      vertexIdToStep.put(step.getCheckpointToken(), step);
      if (step.getAction() instanceof MultiStepAction) {
        adjustTokenStrsOfChildren(step);
        multiStepsIds.add(step.getCheckpointToken());
        for (Step substep : ((MultiStepAction)step.getAction()).getSubSteps()) {
          vertexIdToParentVertexId.put(substep.getCheckpointToken(), step.getCheckpointToken());
          toProcess.add(substep);
        }
      }

      for (Step dependency : step.getDependencies()) {
        toProcess.add(dependency);
      }
    }
  }


  private void adjustTokenStrsOfChildren(Step step) {
    MultiStepAction msa = (MultiStepAction)step.getAction();
    for (Step substep : msa.getSubSteps()) {
      substep.setCheckpointTokenPrefix(step.getCheckpointTokenPrefix() + msa.getCheckpointToken() + "__");
    }
  }


  private Step peekIsolated() {
    return isolated.empty() ? null : isolated.peek();
  }


  public void expandMultistepVertex(String vertexId) {
    Step multiStep = vertexIdToStep.get(vertexId);
    multiStepsToExpand.add(multiStep);
  }


  public void expandAllMultistepVertices() {
    multiStepsToExpand = new HashSet<Step>(vertexIdToStep.values());
  }

  public DirectedGraph<Vertex, DefaultEdge> getDiagramGraph() {
    return getDiagramGraph(dependencyGraphFromTailSteps(workflowRunner.getTailSteps(), null, multiStepsToExpand));
  }

  public DirectedGraph<Vertex, DefaultEdge> getDiagramGraph(DirectedGraph<Step, DefaultEdge> graph) {
    if (peekIsolated() != null) {
      isolateStep(graph, peekIsolated());
    }
    DirectedGraph<Step, DefaultEdge> dependencyGraph = new EdgeReversedGraph(graph);
    DirectedGraph<Vertex, DefaultEdge> diagramGraph = wrapVertices(dependencyGraph);
    removeRedundantEdges(diagramGraph);
    return diagramGraph;
  }

  private Integer getIndex(DataStore ds, Map<DataStore, Integer> dsToIndex) {


    return dsToIndex.get(ds);
  }

  private void addDatastoreConnections(Step step,
                                       Map<String, Integer> stepIdToNum,
                                       JSONArray dsConnections,
                                       Multimap<Action.DSAction, DataStore> stores,
                                       Map<DataStore, Integer> dsToIndex) throws JSONException {

    for (Action.DSAction action : stores.keySet()) {
      String type = action.toString().toLowerCase();

      for (DataStore ds : stores.get(action)) {
        if (!dsToIndex.containsKey(ds)) {
          dsToIndex.put(ds, dsToIndex.size());
        }

        dsConnections.put(new JSONObject()
            .put("step", stepIdToNum.get(step.getCheckpointToken()))
            .put("datastore", getIndex(ds, dsToIndex))
            .put("connection", type));
      }
    }


  }

  public JSONObject getJSONState() throws JSONException, UnknownHostException {

    DirectedGraph<Step, DefaultEdge> stepGraph = dependencyGraphFromTailSteps(workflowRunner.getTailSteps(), null, multiStepsToExpand);
    DirectedGraph<Vertex, DefaultEdge> graph = getDiagramGraph(stepGraph);

    JSONArray steps = new JSONArray();
    JSONArray edges = new JSONArray();

    Map<String, Integer> stepIdToNum = Maps.newHashMap();
    Multimap<String, String> allEdges = HashMultimap.create();

    TopologicalOrderIterator<Vertex, DefaultEdge> iter = new TopologicalOrderIterator<Vertex, DefaultEdge>(graph);

    while (iter.hasNext()) {
      Vertex vertex = iter.next();

      int nodeIndex = stepIdToNum.size();
      stepIdToNum.put(vertex.getId(), nodeIndex);

      steps.put(new JSONObject()
              .put("id", vertex.getId())
              .put("index", nodeIndex)
              .put("name", vertex.getName())
              .put("status", vertex.getStatus())
              .put("start_timestamp", vertex.getStartTimestamp())
              .put("end_timestamp", vertex.getEndTimestamp())
              .put("message", vertex.getMessage())
              .put("action_name", vertex.getActionName())
              .put("status_links", vertex.getStatusLinks())
      );

      for (DefaultEdge inEdge : graph.incomingEdgesOf(vertex)) {
        Vertex source = graph.getEdgeSource(inEdge);
        allEdges.put(source.getId(), vertex.getId());
      }
    }


    Map<DataStore, Integer> dataStoresToIndex = Maps.newHashMap();

    JSONArray dsConnections = new JSONArray();

    for (Step step : stepGraph.vertexSet()) {

      addDatastoreConnections(step,
          stepIdToNum,
          dsConnections,
          step.getAction().getAllDatastores(),
          dataStoresToIndex
      );

    }

    JSONArray stores = new JSONArray();

    List<Map.Entry<DataStore, Integer>> storesById = Lists.newArrayList(dataStoresToIndex.entrySet());
    Collections.sort(storesById, new Comparator<Map.Entry<DataStore, Integer>>() {
      @Override
      public int compare(Map.Entry<DataStore, Integer> o1, Map.Entry<DataStore, Integer> o2) {
        return o1.getValue() - o2.getValue();
      }
    });

    for (Map.Entry<DataStore, Integer> entry : storesById) {
      DataStore store = entry.getKey();
      stores.put(new JSONObject()
          .put("index", entry.getValue())
          .put("name", store.getName())
          .put("path", store.getPath())
          .put("type", store.getClass().getName()));

    }

    for (Map.Entry<String, String> edge : allEdges.entries()) {
      edges.put(new JSONObject()
          .put("source", stepIdToNum.get(edge.getKey()))
          .put("target", stepIdToNum.get(edge.getValue())));
    }

    LiveWorkflowMeta meta = workflowRunner.getMeta();

    return new JSONObject()
        .put("name", meta.get_name())
        .put("host", meta.get_host())
        .put("id", workflowRunner.getWorkflowUUID())
        .put("username", meta.get_username())
        .put("shutdown_reason", workflowRunner.getReasonForShutdownRequest())
        .put("priority", workflowRunner.getPriority())
        .put("pool", workflowRunner.getPool())
        .put("status", getStatus())
        .put("edges", edges)
        .put("datastore_uses", dsConnections)
        .put("datastores", stores)
        .put("steps", steps);
  }

  private String getStatus() {
    if (workflowRunner.isFailPending()) {
      return "failPending";
    }
    if (workflowRunner.isShutdownPending()) {
      return "shutdownPending";
    }
    return "running";
  }

  public static WorkflowDefinition getDefinition(DirectedGraph<Step, DefaultEdge> flatGraph, String workflowName) {

    //  TODO remove redundant edges

    Map<String, StepDefinition> steps = Maps.newHashMap();
    for (Step step : flatGraph.vertexSet()) {
      StepDefinition def = new StepDefinition(step.getAction().getClass().getName(), step.getCheckpointToken(), Lists.<String>newArrayList());
      steps.put(step.getCheckpointToken(), def);
    }

    for (DefaultEdge edge : flatGraph.edgeSet()) {
      Step source = flatGraph.getEdgeSource(edge);
      Step target = flatGraph.getEdgeTarget(edge);
      steps.get(source.getCheckpointToken()).add_to_requiredCheckpoints(target.getCheckpointToken());
    }

    return new WorkflowDefinition(workflowName, steps);
  }

  private DirectedGraph<Vertex, DefaultEdge> wrapVertices(DirectedGraph<Step, DefaultEdge> graph) {
    DirectedGraph<Vertex, DefaultEdge> resultGraph =
        new SimpleDirectedGraph<Vertex, DefaultEdge>(DefaultEdge.class);

    Map<Step, Vertex> stepToVertex = new HashMap<Step, Vertex>();
    for (Step step : graph.vertexSet()) {
      Vertex vwrapper = createVertexFromStep(step);
      stepToVertex.put(step, vwrapper);
      resultGraph.addVertex(vwrapper);
    }

    for (DefaultEdge edge : graph.edgeSet()) {
      Vertex source = stepToVertex.get(graph.getEdgeSource(edge));
      Vertex target = stepToVertex.get(graph.getEdgeTarget(edge));
      resultGraph.addEdge(source, target);
    }

    return resultGraph;
  }

  private Vertex createVertexFromStep(Step step) {
    return new Vertex(step, getStepStatus(step));
  }

  private StepExecuteStatus._Fields getStepStatus(Step step) {
    if (step.getAction() instanceof MultiStepAction) {
      MultiStepAction msa = (MultiStepAction)step.getAction();
      Set<StepExecuteStatus._Fields> statusSet = new HashSet<StepExecuteStatus._Fields>();
      for (Step substep : msa.getSubSteps()) {
        statusSet.add(getStepStatus(substep));
      }
      if (statusSet.contains(StepExecuteStatus._Fields.FAILED)) {
        return StepExecuteStatus._Fields.FAILED;
      } else if (statusSet.contains(StepExecuteStatus._Fields.RUNNING)) {
        return StepExecuteStatus._Fields.RUNNING;
      } else if (statusSet.contains(StepExecuteStatus._Fields.WAITING)) {
        return StepExecuteStatus._Fields.WAITING;
      } else if (statusSet.contains(StepExecuteStatus._Fields.COMPLETED)) {
        return StepExecuteStatus._Fields.COMPLETED;
      } else {
        return StepExecuteStatus._Fields.SKIPPED;
      }
    } else {
      return workflowRunner.getStepStatus(step).getSetField();
    }
  }


  private void removeRedundantEdges(DirectedGraph<Vertex, DefaultEdge> graph) {
    for (Vertex vertex : graph.vertexSet()) {
      if (!vertex.getStatus().equals("datastore")) {
        Set<Vertex> firstDegDeps = new HashSet<Vertex>();
        Set<Vertex> secondPlusDegDeps = new HashSet<Vertex>();
        for (DefaultEdge edge : graph.outgoingEdgesOf(vertex)) {
          Vertex depVertex = graph.getEdgeTarget(edge);
          firstDegDeps.add(depVertex);
          getOutgoingVerticesRecursive(depVertex, secondPlusDegDeps, graph);
        }

        for (Vertex firstDegDep : firstDegDeps) {
          if (secondPlusDegDeps.contains(firstDegDep)) {
            graph.removeAllEdges(vertex, firstDegDep);
          }
        }
      }
    }
  }

  private void getOutgoingVerticesRecursive(Vertex vertex, Set<Vertex> results, DirectedGraph<Vertex, DefaultEdge> graph) {
    for (DefaultEdge edge : graph.outgoingEdgesOf(vertex)) {
      Vertex s = graph.getEdgeTarget(edge);
      results.add(s);
      getOutgoingVerticesRecursive(s, results, graph);
    }
  }

  protected static DirectedGraph<Step, DefaultEdge> flatDependencyGraphFromTailSteps(Set<Step> tailSteps,
                                                                                     EventTimer workflowTimer) {
    return dependencyGraphFromTailSteps(tailSteps, workflowTimer, null);
  }

  private static DirectedGraph<Step, DefaultEdge> dependencyGraphFromTailSteps(Set<Step> tailSteps, EventTimer workflowTimer,
                                                                               Set<Step> multiStepsToExpand) {
    verifyNoOrphanedTailSteps(tailSteps);
    return dependencyGraphFromTailStepsNoVerification(tailSteps, workflowTimer, multiStepsToExpand);
  }

  private static DirectedGraph<Step, DefaultEdge> dependencyGraphFromTailStepsNoVerification(Set<Step> tailSteps, EventTimer workflowTimer,
                                                                                             Set<Step> multiStepsToExpand) {

    Set<Step> tailsAndDependencies = addDependencies(tailSteps);
    addToWorkflowTimer(tailsAndDependencies, workflowTimer);
    Queue<Step> multiSteps = new LinkedList<Step>(filterMultiStep(tailsAndDependencies));
    verifyUniqueCheckpointTokens(tailsAndDependencies);
    DirectedGraph<Step, DefaultEdge> dependencyGraph = createGraph(tailsAndDependencies);

    // now, proceed through each MultiStepAction node in the dependency graph,
    // recursively flattening it out and merging it into the current dependency
    // graph.
    if (multiStepsToExpand != null) { // Control which multisteps should be flattened
      multiSteps.retainAll(multiStepsToExpand);
    }
    while (!multiSteps.isEmpty()) {
      Step s = multiSteps.poll();
      // If the dependency graph doesn't contain the vertex, then we've already
      // processed it
      if (dependencyGraph.containsVertex(s)) {
        MultiStepAction msa = (MultiStepAction)s.getAction();

        pullUpSubstepsAndAdjustCheckpointTokens(dependencyGraph, multiSteps, s,
            msa, multiStepsToExpand);

        // now that the dep graph contains the unwrapped multistep *and* the
        // original multistep, let's move the edges to the unwrapped stuff so that
        // we can remove the multistep.

        copyIncomingEdges(dependencyGraph, s, msa);

        copyOutgoingEdges(dependencyGraph, s, msa);

        // finally, the multistep's vertex should be removed.
        dependencyGraph.removeVertex(s);
      }
    }

    return dependencyGraph;
  }

  private static DirectedGraph<Step, DefaultEdge> createGraph(Set<Step> tailsAndDependencies) {
    DirectedGraph<Step, DefaultEdge> dependencyGraph = new SimpleDirectedGraph<Step, DefaultEdge>(DefaultEdge.class);
    for (Step step : tailsAndDependencies) {
      addStepAndDependencies(dependencyGraph, step);
    }
    return dependencyGraph;
  }

  public static void verifyNoOrphanedTailStep(Step tailStep) {
    verifyNoOrphanedTailSteps(Collections.singleton(tailStep));
  }

  public static void verifyNoOrphanedTailSteps(Set<Step> tailSteps) {
    Set<Step> orphans = getOrphanedTailSteps(tailSteps);
    if (orphans.size() != 0) {
      throw new RuntimeException("Orphaned tail steps:" + orphans);
    }
  }

  public static Set<Step> reachableSteps(Set<Step> steps) {
    Set<Step> reachableSteps = new HashSet<Step>();
    Stack<Step> toProcess = new Stack<Step>();
    toProcess.addAll(steps);
    while (!toProcess.isEmpty()) {
      Step step = toProcess.pop();
      if (!reachableSteps.contains(step)) {
        reachableSteps.add(step);
        toProcess.addAll(step.getChildren());
        toProcess.addAll(step.getDependencies());
      }
    }
    return reachableSteps;
  }

  static Set<Step> getOrphanedTailSteps(Set<Step> tailSteps) {
    Set<Step> multiStepsToExpand = new HashSet<Step>();
    tailSteps = new HashSet<Step>(tailSteps);
    Set<Step> reachableSteps = reachableSteps(tailSteps);
    for (Step step : reachableSteps) {
      if (step.getAction() instanceof MultiStepAction) {
        multiStepsToExpand.add(step);
      }
    }

    DirectedGraph<Step, DefaultEdge> dependencyGraph = dependencyGraphFromTailStepsNoVerification(tailSteps, null, multiStepsToExpand);

    return getOrphanedTailSteps(dependencyGraph, reachableSteps);
  }

  private static Set<Step> getOrphanedTailSteps(DirectedGraph<Step, DefaultEdge> dependencyGraph, Set<Step> allSteps) {
    Set<Step> tailSteps = new HashSet<Step>();
    for (Step step : allSteps) {
      if (!(step.getAction() instanceof MultiStepAction)) {
        for (Step child : step.getChildren()) {
          if (!dependencyGraph.containsVertex(child) && !(child.getAction() instanceof MultiStepAction)) {
            tailSteps.add(child);
          }
        }
      }
    }
    return tailSteps;
  }

  private static void isolateStep(DirectedGraph<Step, DefaultEdge> graph, Step isolated) {
    Set<Step> toInclude = new HashSet<Step>();
    toInclude.add(isolated);

    Queue<MultiStepAction> multiSteps = new LinkedList<MultiStepAction>();
    if (isolated.getAction() instanceof MultiStepAction) {
      multiSteps.add((MultiStepAction)isolated.getAction());
    }

    while (!multiSteps.isEmpty()) {
      MultiStepAction action = multiSteps.poll();
      toInclude.addAll(action.getSubSteps());
      for (Step step : action.getSubSteps()) {
        if (step.getAction() instanceof MultiStepAction) {
          multiSteps.add((MultiStepAction)step.getAction());
        }
      }
    }

    Set<Step> toRemove = new HashSet<Step>(graph.vertexSet());
    toRemove.removeAll(toInclude);
    graph.removeAllVertices(toRemove);
  }

  private static void verifyUniqueCheckpointTokens(Iterable<Step> steps) {
    Set<String> tokens = new HashSet<String>();
    for (Step step : steps) {
      String token = step.getAction().getCheckpointToken();
      if (tokens.contains(token)) {
        throw new IllegalArgumentException(step.toString() + " has a non-unique checkpoint token!");
      }
      tokens.add(token);
    }
  }

  private static Set<Step> addDependencies(Set<Step> steps) {
    Queue<Step> toProcess = new LinkedList<Step>(steps);
    Set<Step> visited = new HashSet<Step>();
    while (!toProcess.isEmpty()) {
      Step step = toProcess.poll();
      if (!visited.contains(step)) {
        visited.add(step);
        for (Step dependency : step.getDependencies()) {
          toProcess.add(dependency);
        }
      }
    }
    return visited;
  }

  private static Set<Step> filterMultiStep(Iterable<Step> steps) {
    Set<Step> multiSteps = new HashSet<Step>();
    for (Step step : steps) {
      if (step.getAction() instanceof MultiStepAction) {
        multiSteps.add(step);
      }
    }
    return multiSteps;
  }

  private static void addToWorkflowTimer(Set<Step> tailsAndDependencies,
                                         EventTimer workflowTimer) {
    if (workflowTimer != null) {
      for (Step step : tailsAndDependencies) {
        workflowTimer.addChild(step.getTimer());
      }
    }
  }

  private static void copyOutgoingEdges(DirectedGraph<Step, DefaultEdge> dependencyGraph, Step s, MultiStepAction msa) {
    // next, the head steps of this multistep, which are naturally dependent
    // upon nothing, should depend on all the dependencies of the multistep
    for (DefaultEdge dependedUponEdge : dependencyGraph.outgoingEdgesOf(s)) {
      Step dependedUpon = dependencyGraph.getEdgeTarget(dependedUponEdge);
      for (Step headStep : msa.getHeadSteps()) {
        dependencyGraph.addVertex(headStep);
        dependencyGraph.addVertex(dependedUpon);
        dependencyGraph.addEdge(headStep, dependedUpon);
      }
    }
  }

  private static void copyIncomingEdges(DirectedGraph<Step, DefaultEdge> dependencyGraph, Step s, MultiStepAction msa) {
    // anyone who was dependent on this multistep should instead be
    // dependent on the tail steps of the multistep
    for (DefaultEdge dependsOnThis : dependencyGraph.incomingEdgesOf(s)) {
      Step edgeSource = dependencyGraph.getEdgeSource(dependsOnThis);
      for (Step tailStep : msa.getTailSteps()) {
        dependencyGraph.addVertex(tailStep);
        dependencyGraph.addEdge(edgeSource, tailStep);
      }
    }
  }

  private static void pullUpSubstepsAndAdjustCheckpointTokens(
      DirectedGraph<Step, DefaultEdge> dependencyGraph, Queue<Step> multiSteps,
      Step s, MultiStepAction msa, Set<Step> multiStepsToExpand) {
    // take all the substeps out of the multistep and put them into the top
    // level dependency graph, making sure to add their dependencies.
    // this is certain to visit some vertices repeatedly, and could probably
    // be done more elegantly with a tail-first traversal.
    // TODO, perhaps?
    for (Step substep : msa.getSubSteps()) {
      // if we encounter another multistep, put it on the queue to be expanded
      if (substep.getAction() instanceof MultiStepAction) {
        if (multiStepsToExpand == null || multiStepsToExpand.contains(substep)) {
          multiSteps.add(substep);
        }
      }

      // adjust the checkpoint token prefix to include the checkpoint token of
      // the multistep. this makes sure that, so long as token are unique in
      // their subgraph, they will also be unique in the flattened graph.
      substep.setCheckpointTokenPrefix(s.getCheckpointTokenPrefix() + msa.getCheckpointToken() + "__");

      addStepAndDependencies(dependencyGraph, substep);
    }
  }

  private static void addStepAndDependencies(DirectedGraph<Step, DefaultEdge> dependencyGraph, Step step) {
    dependencyGraph.addVertex(step);
    for (Step dep : step.getDependencies()) {
      dependencyGraph.addVertex(dep);
      dependencyGraph.addEdge(step, dep);
    }
  }
}
