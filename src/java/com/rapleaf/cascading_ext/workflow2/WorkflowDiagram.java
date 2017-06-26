package com.rapleaf.cascading_ext.workflow2;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import com.google.common.base.Joiner;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

import com.liveramp.workflow_core.runner.BaseMultiStepAction;
import com.liveramp.workflow_core.runner.BaseStep;


public class WorkflowDiagram {

  public static <T> DirectedGraph<BaseStep<T>, DefaultEdge> dependencyGraphFromTailSteps(Set<? extends BaseStep<T>> tailSteps) {
    verifyNoOrphanedTailSteps(tailSteps);
    return dependencyGraphFromTailStepsNoVerification(tailSteps);
  }

  private static <T> DirectedGraph<BaseStep<T>, DefaultEdge> dependencyGraphFromTailStepsNoVerification(Set<? extends BaseStep<T>> tailSteps) {

    Set<BaseStep<T>> tailsAndDependencies = addDependencies(tailSteps);
    Queue<BaseStep<T>> multiSteps = new LinkedList<BaseStep<T>>(filterMultiStep(tailsAndDependencies));
    verifyUniqueCheckpointTokens(tailsAndDependencies);
    DirectedGraph<BaseStep<T>, DefaultEdge> dependencyGraph = createGraph(tailsAndDependencies);

    // now, proceed through each MultiStepAction node in the dependency graph,
    // recursively flattening it out and merging it into the current dependency
    // graph.

    while (!multiSteps.isEmpty()) {
      BaseStep<T> s = multiSteps.poll();
      // If the dependency graph doesn't contain the vertex, then we've already
      // processed it
      if (dependencyGraph.containsVertex(s)) {
        BaseMultiStepAction<T> msa = (BaseMultiStepAction<T>)s.getAction();

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

  private static <T> DirectedGraph<BaseStep<T>, DefaultEdge> createGraph(Set<BaseStep<T>> tailsAndDependencies) {
    DirectedGraph<BaseStep<T>, DefaultEdge> dependencyGraph = new SimpleDirectedGraph<>(DefaultEdge.class);
    for (BaseStep<T> step : tailsAndDependencies) {
      addStepAndDependencies(dependencyGraph, step);
    }
    return dependencyGraph;
  }

  public static <T> void verifyNoOrphanedTailSteps(Set<? extends BaseStep<T>> tailSteps) {
    Set<BaseStep<T>> orphans = getOrphanedTailSteps(tailSteps);
    if (orphans.size() != 0) {
      throw new RuntimeException("Orphaned tail steps:\n" + Joiner.on("\n").join(orphans));
    }
  }

  public static <T> Set<BaseStep<T>> reachableSteps(Set<? extends BaseStep<T>> steps) {
    Set<BaseStep<T>> reachableSteps = new HashSet<BaseStep<T>>();
    Stack<BaseStep<T>> toProcess = new Stack<BaseStep<T>>();
    toProcess.addAll(steps);
    while (!toProcess.isEmpty()) {
      BaseStep<T> step = toProcess.pop();
      if (!reachableSteps.contains(step)) {
        reachableSteps.add(step);
        toProcess.addAll(step.getChildren());
        toProcess.addAll(step.getDependencies());
      }
    }
    return reachableSteps;
  }

  static <T> Set<BaseStep<T>> getOrphanedTailSteps(Set<? extends BaseStep<T>> tailSteps) {
    return getOrphanedTailSteps(
        dependencyGraphFromTailStepsNoVerification(tailSteps),
        reachableSteps(tailSteps)
    );
  }

  private static <T> Set<BaseStep<T>> getOrphanedTailSteps(DirectedGraph<BaseStep<T>, DefaultEdge> dependencyGraph, Set<BaseStep<T>> allSteps) {
    Set<BaseStep<T>> tailSteps = new HashSet<BaseStep<T>>();
    for (BaseStep<T> step : allSteps) {
      if (!(step.getAction() instanceof BaseMultiStepAction)) {
        for (BaseStep<T> child : step.getChildren()) {
          if (!dependencyGraph.containsVertex(child) && !(child.getAction() instanceof BaseMultiStepAction)) {
            tailSteps.add(child);
          }
        }
      }
    }
    return tailSteps;
  }

  private static <T> void verifyUniqueCheckpointTokens(Iterable<BaseStep<T>> steps) {
    Set<String> tokens = new HashSet<String>();
    for (BaseStep<T> step : steps) {
      String token = step.getCheckpointToken();
      if (tokens.contains(token)) {
        throw new IllegalArgumentException(step.toString() + " has a non-unique checkpoint token!");
      }
      tokens.add(token);
    }
  }

  private static <T> Set<BaseStep<T>> addDependencies(Set<? extends BaseStep<T>> steps) {
    Queue<BaseStep<T>> toProcess = new LinkedList<>(steps);
    Set<BaseStep<T>> visited = new HashSet<>();
    while (!toProcess.isEmpty()) {
      BaseStep<T> step = toProcess.poll();
      if (!visited.contains(step)) {
        visited.add(step);
        for (BaseStep<T> dependency : step.getDependencies()) {
          toProcess.add(dependency);
        }
      }
    }
    return visited;
  }

  private static <T> Set<BaseStep<T>> filterMultiStep(Iterable<BaseStep<T>> steps) {
    Set<BaseStep<T>> multiSteps = new HashSet<>();
    for (BaseStep<T> step : steps) {
      if (step.getAction() instanceof BaseMultiStepAction) {
        multiSteps.add(step);
      }
    }
    return multiSteps;
  }

  private static <T> void copyOutgoingEdges(DirectedGraph<BaseStep<T>, DefaultEdge> dependencyGraph, BaseStep<T> s, BaseMultiStepAction<T> msa) {
    // next, the head steps of this multistep, which are naturally dependent
    // upon nothing, should depend on all the dependencies of the multistep
    for (DefaultEdge dependedUponEdge : dependencyGraph.outgoingEdgesOf(s)) {
      BaseStep<T> dependedUpon = dependencyGraph.getEdgeTarget(dependedUponEdge);
      for (BaseStep<T> headStep : msa.getHeadSteps()) {
        dependencyGraph.addVertex(headStep);
        dependencyGraph.addVertex(dependedUpon);
        dependencyGraph.addEdge(headStep, dependedUpon);
      }
    }
  }

  private static <T> void copyIncomingEdges(DirectedGraph<BaseStep<T>, DefaultEdge> dependencyGraph, BaseStep<T> s, BaseMultiStepAction<T> msa) {
    // anyone who was dependent on this multistep should instead be
    // dependent on the tail steps of the multistep
    for (DefaultEdge dependsOnThis : dependencyGraph.incomingEdgesOf(s)) {
      BaseStep<T> edgeSource = dependencyGraph.getEdgeSource(dependsOnThis);
      for (BaseStep<T> tailStep : msa.getTailSteps()) {
        dependencyGraph.addVertex(tailStep);
        dependencyGraph.addEdge(edgeSource, tailStep);
      }
    }
  }

  private static <T> void pullUpSubstepsAndAdjustCheckpointTokens(
      DirectedGraph<BaseStep<T>, DefaultEdge> dependencyGraph, Queue<BaseStep<T>> multiSteps,
      BaseStep<T> s, BaseMultiStepAction<T> msa) {
    // take all the substeps out of the multistep and put them into the top
    // level dependency graph, making sure to add their dependencies.
    // this is certain to visit some vertices repeatedly, and could probably
    // be done more elegantly with a tail-first traversal.
    // TODO, perhaps?
    for (BaseStep substep : msa.getSubSteps()) {
      // if we encounter another multistep, put it on the queue to be expanded
      if (substep.getAction() instanceof BaseMultiStepAction) {
        multiSteps.add(substep);
      }

      addStepAndDependencies(dependencyGraph, substep);
    }
  }

  private static <T> void addStepAndDependencies(DirectedGraph<BaseStep<T>, DefaultEdge> dependencyGraph, BaseStep<T> step) {
    dependencyGraph.addVertex(step);
    for (BaseStep dep : step.getDependencies()) {
      dependencyGraph.addVertex(dep);
      dependencyGraph.addEdge(step, dep);
    }
  }
}
