package com.rapleaf.cascading_ext.workflow2;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;


public class WorkflowDiagram {

  public static DirectedGraph<Step, DefaultEdge> dependencyGraphFromTailSteps(Set<Step> tailSteps) {
    verifyNoOrphanedTailSteps(tailSteps);
    return dependencyGraphFromTailStepsNoVerification(tailSteps);
  }

  private static DirectedGraph<Step, DefaultEdge> dependencyGraphFromTailStepsNoVerification(Set<Step> tailSteps) {

    Set<Step> tailsAndDependencies = addDependencies(tailSteps);
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
      throw new RuntimeException("Orphaned tail steps!");
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
        dependencyGraphFromTailStepsNoVerification(tailSteps),
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
