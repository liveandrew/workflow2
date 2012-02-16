package com.rapleaf.cascading_ext.workflow2;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.support.event_timer.EventTimer;
import com.rapleaf.support.event_timer.MultiTimedEvent;
import com.rapleaf.support.event_timer.TimedEventWithChildren;

public class WorkflowDiagram {
  
  public static class Vertex {
    private String id;
    private String name;
    private String status;
    private long startTimestamp;
    private long endTimestamp;
    private int percentageComplete;
    private String message;
    private String actionName;
    private String jobTrackerLinks;
    
    public Vertex(String id, String name, String status) {
      this.id = id;
      this.name = name;
      this.status = status;
    }
    
    public Vertex(Step step, StepStatus status) {
      this.id = step.getCheckpointToken();
      this.name = step.getSimpleCheckpointToken();
      this.status = status.name().toLowerCase();
      
      jobTrackerLinks = StringUtils.join(step.getAction().getJobTrackerLinks().toArray(new String[0]));
      
      Action action = step.getAction();
      actionName = action.getClass().getSimpleName();
      if (action instanceof MultiStepAction) {
        percentageComplete = -1;
        message = "";
        startTimestamp = computeStartTimestamp((MultiStepAction) action);
        endTimestamp = computeEndTimestamp((MultiStepAction) action);
      } else {
        percentageComplete = action.getPercentComplete();
        message = action.getStatusMessage();
        startTimestamp = action.getStartTimestamp();
        endTimestamp = action.getEndTimestamp();
      }
    }
    
    public Vertex(DataStore datastore) {
      this.id = datastore.getName();
      this.name = datastore.getName();
      this.status = "datastore";
    }
    
    private long computeStartTimestamp(MultiStepAction action) {
      long best = Long.MAX_VALUE;
      for (Step substep : action.getSubSteps()) {
        Action subAction = substep.getAction();
        if (subAction instanceof MultiStepAction) {
          best = Math.min(best, computeStartTimestamp((MultiStepAction) subAction));
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
          best = Math.max(best, computeEndTimestamp((MultiStepAction) subAction));
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
    
    public int getPercentageComplete() {
      return percentageComplete;
    }
    
    public String getMessage() {
      return message;
    }
    
    public String getJobTrackerLinks() {
      return jobTrackerLinks;
    }
    
    public String getActionName() {
      return actionName;
    }
  }
  
  private final WorkflowRunner workflowRunner;
  private Set<String> multiStepsIds;
  private Map<String, Step> vertexIdToStep;
  private Map<String, String> vertexIdToParentVertexId;
  
  private Map<Step, Vertex> stepToVertex = new HashMap<Step, Vertex>();
  private Map<DataStore, Vertex> dsToVertex = new HashMap<DataStore, Vertex>();
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
        for (Step substep : ((MultiStepAction) step.getAction()).getSubSteps()) {
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
    MultiStepAction msa = (MultiStepAction) step.getAction();
    for (Step substep : msa.getSubSteps()) {
      substep.setCheckpointTokenPrefix(step.getCheckpointToken() + "__");
    }
  }
  
  public void reduceIsolation(String id) {
    while (!isolated.empty()) {
      isolated.pop();
      String current = isolated.empty() ? null : isolated.peek().getCheckpointToken();
      if (id.equals(current)) {
        break;
      }
    }
  }
  
  public void isolateVertex(String vertexId) {
    Step step = vertexIdToStep.get(vertexId);
    if (!isolated.contains(step)) {
      String parent = vertexIdToParentVertexId.get(vertexId);
      Stack<String> toAdd = new Stack<String>();
      while (parent != null) {
        toAdd.push(parent);
        parent = vertexIdToParentVertexId.get(parent);
      }
      while (!toAdd.empty()) {
        isolated.push(vertexIdToStep.get(toAdd.pop()));
      }
      isolated.push(step);
    }
  }
  
  private Step peekIsolated() {
    return isolated.empty() ? null : isolated.peek();
  }
  
  public List<String> getIsolated() {
    List<String> ret = new ArrayList<String>();
    if (!isolated.empty()) {
      ret.add("Entire Workflow");
      Enumeration<Step> steps = isolated.elements();
      while (steps.hasMoreElements()) {
        ret.add(steps.nextElement().getCheckpointToken());
      }
    }
    return ret;
  }
  
  public void expandMultistepVertex(String vertexId) {
    Step multiStep = vertexIdToStep.get(vertexId);
    multiStepsToExpand.add(multiStep);
  }
  
  public void collapseMultistepVertex(String vertexId) {
    Step multiStep = vertexIdToStep.get(vertexId);
    multiStepsToExpand.remove(multiStep);
  }
  
  public void collapseParentOfVertex(String vertexId) {
    String parentId = vertexIdToParentVertexId.get(vertexId);
    if (parentId != null) {
      multiStepsToExpand.remove(vertexIdToStep.get(parentId));
    }
  }
  
  public void collapseAllMultistepVertices() {
    if (isolated.empty()) {
      multiStepsToExpand.clear();
    } else {
      Iterator<Step> it = multiStepsToExpand.iterator();
      while (it.hasNext()) {
        Step step = it.next();
        if (!isolated.contains(step) || isolated.peek().equals(step)) {
          it.remove();
        }
      }
    }
  }
  
  public void expandAllMultistepVertices() {
    multiStepsToExpand = new HashSet<Step>(vertexIdToStep.values());
  }
  
  public boolean isExpandable(String vertexId) {
    return multiStepsIds.contains(vertexId);
  }
  
  public boolean isExpanded(String vertexId) {
    return multiStepsToExpand.contains(vertexIdToStep.get(vertexId));
  }
  
  public boolean hasParent(String vertexId) {
    return vertexIdToParentVertexId.containsKey(vertexId);
  }
  
  public DirectedGraph<Vertex, DefaultEdge> getDiagramGraph() {
    DirectedGraph<Step, DefaultEdge> dependencyGraph = new EdgeReversedGraph(
        dependencyGraphFromTailSteps(workflowRunner.getTailSteps(), null, multiStepsToExpand, peekIsolated()));
    DirectedGraph<Vertex, DefaultEdge> diagramGraph = wrapVertices(dependencyGraph);
    removeRedundantEdges(diagramGraph);
    return diagramGraph;
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
  
  public DirectedGraph<Vertex, DefaultEdge> getDiagramGraphWithDataStores() {
    DirectedGraph<Step, DefaultEdge> dependencyGraph = new EdgeReversedGraph(
        dependencyGraphFromTailSteps(workflowRunner.getTailSteps(), null, multiStepsToExpand, peekIsolated()));
    
    DirectedGraph<Vertex, DefaultEdge> diagramGraph =
        new SimpleDirectedGraph<Vertex, DefaultEdge>(DefaultEdge.class);
    
    addVerticesAndInputDSDependenciesToDiagramGraph(dependencyGraph, diagramGraph);
    addOriginalEdgesToDiagramGraph(dependencyGraph, diagramGraph);
    Set<Vertex> dsCreatingCycles = getDatastoresThatCreateCycles(dependencyGraph, diagramGraph);
    addOutputDSDependenciesToDiagramGraph(dependencyGraph, diagramGraph);
    removeCycles(dsCreatingCycles, diagramGraph);
    removeRedundantEdges(diagramGraph);
    Set<Vertex> toDelete = getInternalDatastores(dependencyGraph, diagramGraph);
    diagramGraph.removeAllVertices(toDelete);
    
    return diagramGraph;
  }
  
  private void addVerticesAndInputDSDependenciesToDiagramGraph(DirectedGraph<Step, DefaultEdge> dependencyGraph,
      DirectedGraph<Vertex, DefaultEdge> diagramGraph) {
    stepToVertex =
        new HashMap<Step, Vertex>();
    dsToVertex =
        new HashMap<DataStore, Vertex>();
    for (Step step : dependencyGraph.vertexSet()) {
      Vertex stepVertex = createVertexFromStep(step);
      stepToVertex.put(step, stepVertex);
      diagramGraph.addVertex(stepVertex);
      Set<DataStore> inputDSs = step.getAction().getReadsFromDatastores();
      for (DataStore ds : inputDSs) {
        Vertex dsVertex = getAndCashDSVertex(dsToVertex, ds);
        diagramGraph.addVertex(dsVertex);
        diagramGraph.addEdge(dsVertex, stepVertex);
      }
    }
  }
  
  private void addOriginalEdgesToDiagramGraph(DirectedGraph<Step, DefaultEdge> dependencyGraph,
      DirectedGraph<Vertex, DefaultEdge> diagramGraph) {
    for (DefaultEdge edge : dependencyGraph.edgeSet()) {
      Vertex source = stepToVertex.get(dependencyGraph.getEdgeSource(edge));
      Vertex target = stepToVertex.get(dependencyGraph.getEdgeTarget(edge));
      diagramGraph.addEdge(source, target);
    }
  }
  
  private Set<Vertex> getDatastoresThatCreateCycles(DirectedGraph<Step, DefaultEdge> dependencyGraph,
      DirectedGraph<Vertex, DefaultEdge> diagramGraph) {
    Set<Vertex> dsCreatingCycles = new HashSet<Vertex>();
    for (Step step : dependencyGraph.vertexSet()) {
      Vertex stepVertex = stepToVertex.get(step);
      Set<Vertex> invDeps = new HashSet<Vertex>();
      getIncomingVerticesRecursive(stepVertex, invDeps, diagramGraph);
      Set<DataStore> outputDSs = getOutputDSsFromStep(step);
      outputDSs.addAll(step.getAction().getWritesToDatastores());
      for (DataStore ds : outputDSs) {
        Vertex dsVertex = getAndCashDSVertex(dsToVertex, ds);
        diagramGraph.addVertex(dsVertex);
        if (invDeps.contains(dsVertex)) {
          dsCreatingCycles.add(dsVertex);
        }
      }
    }
    return dsCreatingCycles;
  }
  
  private void addOutputDSDependenciesToDiagramGraph(DirectedGraph<Step, DefaultEdge> dependencyGraph,
      DirectedGraph<Vertex, DefaultEdge> diagramGraph) {
    for (Step step : dependencyGraph.vertexSet()) {
      Vertex stepVertex = stepToVertex.get(step);
      Set<DataStore> outputDSs = getOutputDSsFromStep(step);
      for (DataStore ds : outputDSs) {
        Vertex dsVertex = getAndCashDSVertex(dsToVertex, ds);
        diagramGraph.addEdge(stepVertex, dsVertex);
      }
    }
  }
  
  private Set<Vertex> getInternalDatastores(DirectedGraph<Step, DefaultEdge> dependencyGraph,
      DirectedGraph<Vertex, DefaultEdge> resultGraph) {
    Set<Vertex> toDelete = new HashSet<Vertex>();
    for (Step step : dependencyGraph.vertexSet()) {
      if (step.getAction() instanceof MultiStepAction) {
        Set<DefaultEdge> edges = resultGraph.outgoingEdgesOf(stepToVertex.get(step));
        for (DefaultEdge edge : edges) {
          Vertex target = resultGraph.getEdgeTarget(edge);
          if (target.getStatus().equals("datastore") && resultGraph.outgoingEdgesOf(target).isEmpty()) {
            toDelete.add(target);
          }
        }
      }
    }
    return toDelete;
  }
  
  private Set<DataStore> getOutputDSsFromStep(Step step) {
    Set<DataStore> outputDSs =
        new HashSet<DataStore>(step.getAction().getCreatesDatastores());
    outputDSs.addAll(step.getAction().getWritesToDatastores());
    return outputDSs;
  }
  
  private Vertex createVertexFromStep(Step step) {
    return new Vertex(step, getStepStatus(step));
  }
  
  private StepStatus getStepStatus(Step step) {
    if (step.getAction() instanceof MultiStepAction) {
      MultiStepAction msa = (MultiStepAction) step.getAction();
      Set<StepStatus> statusSet = new HashSet<StepStatus>();
      for (Step substep : msa.getSubSteps()) {
        statusSet.add(getStepStatus(substep));
      }
      if (statusSet.contains(StepStatus.FAILED)) {
        return StepStatus.FAILED;
      } else if (statusSet.contains(StepStatus.RUNNING)) {
        return StepStatus.RUNNING;
      } else if (statusSet.contains(StepStatus.WAITING)) {
        return StepStatus.WAITING;
      } else if (statusSet.contains(StepStatus.COMPLETED)) {
        return StepStatus.COMPLETED;
      } else {
        return StepStatus.SKIPPED;
      }
    } else {
      return workflowRunner.getStepStatus(step);
    }
  }
  
  private Vertex getAndCashDSVertex(Map<DataStore, Vertex> dsToVertex, DataStore ds) {
    if (dsToVertex.containsKey(ds)) {
      return dsToVertex.get(ds);
    } else {
      Vertex wrapper = new Vertex(ds);
      dsToVertex.put(ds, wrapper);
      return wrapper;
    }
  }
  
  private void removeCycles(Set<Vertex> dsCreatingCycles, DirectedGraph<Vertex, DefaultEdge> graph) {
    for (Vertex vertex : dsCreatingCycles) {
      createVirtualDatastoresRecursively(graph, vertex, vertex, null, new HashSet<String>());
    }
  }
  
  private void createVirtualDatastoresRecursively(DirectedGraph<Vertex, DefaultEdge> graph,
      Vertex origVertex, Vertex currVertex, Vertex replacement, Set<String> visitedComb) {
    
    String replacementStr = replacement == null ? "" : replacement.getId();
    String comb = currVertex.getId() + replacementStr;
    if (!visitedComb.contains(comb)) {
      visitedComb.add(comb);
    } else { // Prevent potential infinite recursion
      return;
    }
    
    Set<Vertex> outgoingVertices = getOutgoingVertices(currVertex, graph);
    
    for (Vertex outgoingVertex : outgoingVertices) {
      Set<Vertex> outgoingVertices2ndDegree = getOutgoingVertices(outgoingVertex, graph);
      Vertex virtualVertex = null;
      if (outgoingVertices2ndDegree.contains(origVertex)) {
        virtualVertex = new Vertex(origVertex.getId() + "__" + outgoingVertex.getId(), origVertex.getName(), "datastore");
        graph.addVertex(virtualVertex);
        graph.addEdge(outgoingVertex, virtualVertex);
        graph.removeEdge(outgoingVertex, origVertex);
      }
      
      if (replacement != null) {
        Set<Vertex> deps = getIncomingVertices(outgoingVertex, graph);
        
        if (deps.contains(origVertex)) {
          graph.removeEdge(origVertex, outgoingVertex);
          graph.addEdge(replacement, outgoingVertex);
        }
      }
      
      Vertex newReplacement = virtualVertex != null ? virtualVertex : replacement;
      createVirtualDatastoresRecursively(graph, origVertex, outgoingVertex, newReplacement, visitedComb);
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
  
  private void getIncomingVerticesRecursive(Vertex vertex, Set<Vertex> results, DirectedGraph<Vertex, DefaultEdge> graph) {
    for (DefaultEdge edge : graph.incomingEdgesOf(vertex)) {
      Vertex s = graph.getEdgeSource(edge);
      results.add(s);
      getIncomingVerticesRecursive(s, results, graph);
    }
  }
  
  private Set<Vertex> getIncomingVertices(Vertex vertex, DirectedGraph<Vertex, DefaultEdge> graph) {
    Set<Vertex> deps = new HashSet<Vertex>();
    for (DefaultEdge edge : graph.incomingEdgesOf(vertex)) {
      deps.add(graph.getEdgeSource(edge));
    }
    return deps;
  }
  
  private Set<Vertex> getOutgoingVertices(Vertex vertex, DirectedGraph<Vertex, DefaultEdge> graph) {
    Set<Vertex> deps = new HashSet<Vertex>();
    for (DefaultEdge edge : graph.outgoingEdgesOf(vertex)) {
      deps.add(graph.getEdgeTarget(edge));
    }
    return deps;
  }
  
  protected static DirectedGraph<Step, DefaultEdge> flatDependencyGraphFromTailSteps(Set<Step> tailSteps,
      EventTimer workflowTimer) {
    return dependencyGraphFromTailSteps(tailSteps, workflowTimer, null, null);
  }
  
  private static DirectedGraph<Step, DefaultEdge> dependencyGraphFromTailSteps(Set<Step> tailSteps, EventTimer workflowTimer,
      Set<Step> multiStepsToExpand, Step isolated) {
    DirectedGraph<Step, DefaultEdge> dependencyGraph =
        new SimpleDirectedGraph<Step, DefaultEdge>(DefaultEdge.class);
    
    Queue<Step> multiSteps = new LinkedList<Step>();
    
    preprocessTailSteps(tailSteps, dependencyGraph, multiSteps, workflowTimer);
    
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
      if (!dependencyGraph.containsVertex(s)) {
        continue;
      }
      MultiStepAction msa = (MultiStepAction) s.getAction();
      
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
    
    if (isolated != null) {
      isolateStep(dependencyGraph, isolated);
    }
    
    return dependencyGraph;
  }
  
  private static void isolateStep(DirectedGraph<Step, DefaultEdge> graph, Step isolated) {
    Set<Step> toInclude = new HashSet<Step>();
    toInclude.add(isolated);
    
    Queue<MultiStepAction> multiSteps = new LinkedList<MultiStepAction>();
    if (isolated.getAction() instanceof MultiStepAction) {
      multiSteps.add((MultiStepAction) isolated.getAction());
    }
    
    while (!multiSteps.isEmpty()) {
      MultiStepAction action = multiSteps.poll();
      toInclude.addAll(action.getSubSteps());
      for (Step step : action.getSubSteps()) {
        if (step.getAction() instanceof MultiStepAction) {
          multiSteps.add((MultiStepAction) step.getAction());
        }
      }
    }
    
    Set<Step> toRemove = new HashSet<Step>(graph.vertexSet());
    toRemove.removeAll(toInclude);
    graph.removeAllVertices(toRemove);
  }
  
  private static void preprocessTailSteps(Set<Step> tailSteps,
      DirectedGraph<Step, DefaultEdge> dependencyGraph,
      Queue<Step> multiSteps,
      EventTimer workflowTimer) {
    Queue<Step> toProcess = new LinkedList<Step>(tailSteps);
    Set<String> tokens = new HashSet<String>();
    Set<Step> visited = new HashSet<Step>();
    // starting with the tailSteps, proceed through all through the entire
    // (possibly non-flat) step dependency graph, creating an actual dependency
    // graph.
    while (!toProcess.isEmpty()) {
      Step s = toProcess.poll();
      if (visited.contains(s)) {
        continue;
      }
      visited.add(s);
      
      Action action = s.getAction();
      String checkpointToken = s.getCheckpointToken();
      if (tokens.contains(checkpointToken)) {
        throw new IllegalArgumentException(s.toString() + " has a non-unique checkpoint token!");
      }
      tokens.add(checkpointToken);
      
      // if we see a MultiStepAction, we know that there are subgraphs
      // involved, so let's note them down for later.
      if (action instanceof MultiStepAction) {
        multiSteps.add(s);
      }
      
      if (workflowTimer != null) {
        addTimers(s, workflowTimer);
      }
      
      dependencyGraph.addVertex(s);
      for (Step dependency : s.getDependencies()) {
        dependencyGraph.addVertex(dependency);
        dependencyGraph.addEdge(s, dependency);
        toProcess.add(dependency);
      }
    }
  }
  
  private static class MultiStepActionTimer extends MultiTimedEvent {
    
    public MultiStepActionTimer(String checkpoint) {
      super(checkpoint);
    }
  }
  
  public static void addTimers(Step s, TimedEventWithChildren timedEvent) {
    
    if (s.getAction() instanceof MultiStepAction) {
      MultiStepActionTimer t = new MultiStepActionTimer(s.getCheckpointToken());
      
      timedEvent.addChild(t);
      for (Step substep : ((MultiStepAction) s.getAction()).getSubSteps()) {
        addTimers(substep, t);
      }
    } else {
      timedEvent.addChild(s.getTimer());
    }
  }
  
  private static void copyOutgoingEdges(
      DirectedGraph<Step, DefaultEdge> dependencyGraph, Step s,
      MultiStepAction msa) {
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
  
  private static void copyIncomingEdges(
      DirectedGraph<Step, DefaultEdge> dependencyGraph, Step s,
      MultiStepAction msa) {
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
      
      dependencyGraph.addVertex(substep);
      // adjust the checkpoint token prefix to include the checkpoint token of
      // the multistep. this makes sure that, so long as token are unique in
      // their subgraph, they will also be unique in the flattened graph.
      substep.setCheckpointTokenPrefix(s.getCheckpointToken() + "__");
      
      // add edges to their dependencies
      for (Step dep : substep.getDependencies()) {
        dependencyGraph.addVertex(dep);
        dependencyGraph.addEdge(substep, dep);
      }
    }
  }
}
