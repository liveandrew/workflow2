package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.HashMultimap;
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

import com.liveramp.cascading_ext.event_timer.EventTimer;
import com.rapleaf.cascading_ext.workflow2.state.DataStoreInfo;
import com.rapleaf.cascading_ext.workflow2.state.MapReduceJob;
import com.rapleaf.cascading_ext.workflow2.state.StepState;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowStatePersistence;


public class WorkflowDiagram {

  public static DirectedGraph<StepState, DefaultEdge> getDiagramGraph(WorkflowStatePersistence persistence) throws IOException {

    DirectedGraph<String, DefaultEdge> forwardGraph = new SimpleDirectedGraph<String, DefaultEdge>(DefaultEdge.class);

    for (Map.Entry<String, StepState> entry : persistence.getStepStatuses().entrySet()) {
      forwardGraph.addVertex(entry.getKey());
    }

    for (Map.Entry<String, StepState> entry : persistence.getStepStatuses().entrySet()) {
      for (String dep : entry.getValue().getStepDependencies()) {
        forwardGraph.addEdge(entry.getKey(), dep);
      }
    }

    DirectedGraph<String, DefaultEdge> dependencyGraph = new EdgeReversedGraph<String, DefaultEdge>(forwardGraph);
    DirectedGraph<StepState, DefaultEdge> diagramGraph = wrapVertices(dependencyGraph, persistence);
    removeRedundantEdges(diagramGraph);
    return diagramGraph;
  }

  private static DirectedGraph<StepState, DefaultEdge> wrapVertices(DirectedGraph<String, DefaultEdge> graph,
                                                                    WorkflowStatePersistence persistence) throws IOException {
    DirectedGraph<StepState, DefaultEdge> resultGraph =
        new SimpleDirectedGraph<StepState, DefaultEdge>(DefaultEdge.class);

    Map<String, StepState> stepToVertex = new HashMap<String, StepState>();
    for (String step : graph.vertexSet()) {
      StepState node = persistence.getState(step);

      stepToVertex.put(step, node);
      resultGraph.addVertex(node);
    }

    for (DefaultEdge edge : graph.edgeSet()) {
      StepState source = stepToVertex.get(graph.getEdgeSource(edge));
      StepState target = stepToVertex.get(graph.getEdgeTarget(edge));
      resultGraph.addEdge(source, target);
    }

    return resultGraph;
  }

  private static void addDatastoreConnections(StepState step,
                                              Map<String, Integer> stepIdToNum,
                                              JSONArray dsConnections,
                                              Multimap<Action.DSAction, DataStoreInfo> stores) throws JSONException {

    for (Action.DSAction action : stores.keySet()) {
      String type = action.toString().toLowerCase();

      for (DataStoreInfo ds : stores.get(action)) {

        dsConnections.put(new JSONObject()
            .put("step", stepIdToNum.get(step.getStepId()))
            .put("datastore", ds.getId())
            .put("connection", type));
      }
    }


  }

  public static JSONObject getJSONState(WorkflowStatePersistence persistence) throws JSONException, IOException {

    DirectedGraph<StepState, DefaultEdge> graph = getDiagramGraph(persistence);

    JSONArray steps = new JSONArray();
    JSONArray edges = new JSONArray();

    Map<String, Integer> stepIdToNum = Maps.newHashMap();
    Multimap<String, String> allEdges = HashMultimap.create();

    TopologicalOrderIterator<StepState, DefaultEdge> iter = new TopologicalOrderIterator<StepState, DefaultEdge>(graph);

    while (iter.hasNext()) {
      StepState vertex = iter.next();

      int nodeIndex = stepIdToNum.size();
      stepIdToNum.put(vertex.getStepId(), nodeIndex);

      Map<String, String> statusLinks = Maps.newHashMap();
      for (Map.Entry<String, MapReduceJob> entry : vertex.getMrJobsByID().entrySet()) {
        statusLinks.put(entry.getValue().getTrackingURL(), entry.getValue().getJobName());
      }

      steps.put(new JSONObject()
              .put("id", vertex.getStepId())
              .put("index", nodeIndex)
              .put("status", vertex.getStatus().name().toLowerCase())
              .put("start_timestamp", vertex.getStartTimestamp())
              .put("end_timestamp", vertex.getEndTimestamp())
              .put("message", vertex.getStatusMessage())
              .put("action_name", vertex.getActionClass())
              .put("status_links", statusLinks)
      );

      for (DefaultEdge inEdge : graph.incomingEdgesOf(vertex)) {
        StepState source = graph.getEdgeSource(inEdge);
        allEdges.put(source.getStepId(), vertex.getStepId());
      }
    }


    JSONArray dsConnections = new JSONArray();

    for (StepState step : graph.vertexSet()) {
      addDatastoreConnections(step,
          stepIdToNum,
          dsConnections,
          persistence.getState(step.getStepId()).getDatastores()
      );
    }

    JSONArray stores = new JSONArray();

    for (DataStoreInfo storeInfo : persistence.getDatastores()) {
      stores.put(new JSONObject()
          .put("index", storeInfo.getId())
          .put("name", storeInfo.getName())
          .put("path", storeInfo.getPath())
          .put("type", storeInfo.getClassName()));
    }

    for (Map.Entry<String, String> edge : allEdges.entries()) {
      edges.put(new JSONObject()
          .put("source", stepIdToNum.get(edge.getKey()))
          .put("target", stepIdToNum.get(edge.getValue())));
    }

    return new JSONObject()
        .put("name", persistence.getName())
        .put("host", persistence.getHost())
        .put("id", persistence.getId())
        .put("username", persistence.getUsername())
        .put("shutdown_reason", persistence.getShutdownRequest())
        .put("priority", persistence.getPriority())
        .put("pool", persistence.getPool())
        .put("status", getStatus(persistence))
        .put("edges", edges)
        .put("datastore_uses", dsConnections)
        .put("datastores", stores)
        .put("steps", steps);
  }

  private static String getStatus(WorkflowStatePersistence persistence) throws IOException {

    if (WorkflowUtil.isFailPending(persistence)) {
      return "failPending";
    }
    if (WorkflowUtil.isShutdownPending(persistence)) {
      return "shutdownPending";
    }
    return "running";
  }

  private static void removeRedundantEdges(DirectedGraph<StepState, DefaultEdge> graph) {
    for (StepState vertex : graph.vertexSet()) {
      Set<StepState> firstDegDeps = new HashSet<StepState>();
      Set<StepState> secondPlusDegDeps = new HashSet<StepState>();
      for (DefaultEdge edge : graph.outgoingEdgesOf(vertex)) {
        StepState depVertex = graph.getEdgeTarget(edge);
        firstDegDeps.add(depVertex);
        getOutgoingVerticesRecursive(depVertex, secondPlusDegDeps, graph);
      }

      for (StepState firstDegDep : firstDegDeps) {
        if (secondPlusDegDeps.contains(firstDegDep)) {
          graph.removeAllEdges(vertex, firstDegDep);
        }
      }
    }
  }

  private static void getOutgoingVerticesRecursive(StepState vertex, Set<StepState> results, DirectedGraph<StepState, DefaultEdge> graph) {
    for (DefaultEdge edge : graph.outgoingEdgesOf(vertex)) {
      StepState s = graph.getEdgeTarget(edge);
      results.add(s);
      getOutgoingVerticesRecursive(s, results, graph);
    }
  }

  public static DirectedGraph<Step, DefaultEdge> dependencyGraphFromTailSteps(Set<Step> tailSteps, EventTimer workflowTimer) {
    verifyNoOrphanedTailSteps(tailSteps);
    return dependencyGraphFromTailStepsNoVerification(tailSteps, workflowTimer);
  }

  private static DirectedGraph<Step, DefaultEdge> dependencyGraphFromTailStepsNoVerification(Set<Step> tailSteps, EventTimer workflowTimer) {

    Set<Step> tailsAndDependencies = addDependencies(tailSteps);
    addToWorkflowTimer(tailsAndDependencies, workflowTimer);
    Queue<Step> multiSteps = new LinkedList<Step>(filterMultiStep(tailsAndDependencies));
    verifyUniqueCheckpointTokens(tailsAndDependencies);
    DirectedGraph<Step, DefaultEdge> dependencyGraph = createGraph(tailsAndDependencies);

    // now, proceed through each MultiStepAction node in the dependency graph,
    // recursively flattening it out and merging it into the current dependency
    // graph.

    while (!multiSteps.isEmpty()) {
      Step s = multiSteps.poll();
      // If the dependency graph doesn't contain the vertex, then we've already
      // processed it
      if (dependencyGraph.containsVertex(s)) {
        MultiStepAction msa = (MultiStepAction)s.getAction();

        pullUpSubstepsAndAdjustCheckpointTokens(dependencyGraph, multiSteps, s, msa);

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
    return getOrphanedTailSteps(
        dependencyGraphFromTailStepsNoVerification(tailSteps, null),
        reachableSteps(tailSteps)
    );
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

  private static void verifyUniqueCheckpointTokens(Iterable<Step> steps) {
    Set<String> tokens = new HashSet<String>();
    for (Step step : steps) {
      String token = step.getCheckpointToken();
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
      Step s, MultiStepAction msa) {
    // take all the substeps out of the multistep and put them into the top
    // level dependency graph, making sure to add their dependencies.
    // this is certain to visit some vertices repeatedly, and could probably
    // be done more elegantly with a tail-first traversal.
    // TODO, perhaps?
    for (Step substep : msa.getSubSteps()) {
      // if we encounter another multistep, put it on the queue to be expanded
      if (substep.getAction() instanceof MultiStepAction) {
        multiSteps.add(substep);
      }

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
