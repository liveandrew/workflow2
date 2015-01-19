package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.junit.Test;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.datastore.BytesDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.WorkflowDiagram.Vertex;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

public class TestWorkflowDiagram extends CascadingExtTestCase {

  public static class FakeAction extends Action {

    public FakeAction(String token, DataStore[] inputs, DataStore[] outputs) throws IOException {
      super(token);

      for (DataStore input : inputs) {
        readsFrom(input);
      }
      for (DataStore output : outputs) {
        writesTo(output);
      }
    }

    @Override
    public void execute() {
      try {
        Thread.sleep(1000000L);
      } catch (InterruptedException e) {
        // no-op
      }
    }
  }

  public static final class FakeMultistepAction extends MultiStepAction {
    public FakeMultistepAction(String checkpointToken, Step[] steps) {
      super(checkpointToken, Arrays.asList(steps));
    }
  }

  private Map<String, Vertex> idToVertex;
  DirectedGraph<Vertex, DefaultEdge> graph;

  @Test
  public void testVerifyNoOrphanedTailStep() throws Exception {
    DataStore ds = getFakeDS("ds");

    Step s1 = new Step(new FakeAction("s1", new DataStore[]{ds}, new DataStore[]{ds}));
    Step s2 = new Step(new FakeAction("s2", new DataStore[]{ds}, new DataStore[]{ds}), s1);
    Step s3 = new Step(new FakeAction("s3", new DataStore[]{ds}, new DataStore[]{ds}), s2);
    assertTrue(WorkflowDiagram.getOrphanedTailSteps(Collections.singleton(s3)).isEmpty());
  }

  @Test
  public void testVerifyNoOrphanedTailStepWithMultistep() throws Exception {
    DataStore ds = getFakeDS("ds");

    Step s1 = new Step(new FakeAction("s1", new DataStore[]{ds}, new DataStore[]{ds}));
    Step s2 = new Step(new FakeAction("s2", new DataStore[]{ds}, new DataStore[]{ds}), s1);
    Step s3 = new Step(new FakeMultistepAction("s3", new Step[]{}), s2);
    Step s4 = new Step(new FakeAction("s4", new DataStore[]{ds}, new DataStore[]{ds}), s3);
    Set<Step> orphans = WorkflowDiagram.getOrphanedTailSteps(Collections.singleton(s4));
    assertTrue(orphans.isEmpty());
  }

  @Test
  public void testVerifyNoOrphanedTailStepWithMultistepTail() throws Exception {
    DataStore ds = getFakeDS("ds");

    Step s1 = new Step(new FakeAction("s1", new DataStore[]{ds}, new DataStore[]{ds}));
    Step s2 = new Step(new FakeAction("s2", new DataStore[]{ds}, new DataStore[]{ds}), s1);
    Step s3 = new Step(new FakeMultistepAction("s3", new Step[]{}), s2);
    Set<Step> orphans = WorkflowDiagram.getOrphanedTailSteps(Collections.singleton(s3));
    assertTrue(orphans.isEmpty());
  }

  @Test
  public void testComplexNestedAllExpanded() throws Exception {
    Step tail = getComplexNestedWorkflowTail();
    WorkflowDiagram wfd = getWorkflowDiagramFromTails(tail);
    setupWorkflowGraph(wfd);

    verifyNumVertices(13);
    verifyVertexInGraph("s1");
    verifyVertexInGraph("s2");
    verifyVertexInGraph("s3");
    verifyVertexInGraph("s4__1");
    verifyVertexInGraph("s4__2");
    verifyVertexInGraph("s4__3");
    verifyVertexInGraph("s5__1__1");
    verifyVertexInGraph("s5__1__2");
    verifyVertexInGraph("s5__2");
    verifyVertexInGraph("s5__3");
    verifyVertexInGraph("s5__4");
    verifyVertexInGraph("s6");
    verifyVertexInGraph("s7");

    verifyNumEdges(17);
    verifyEdgeInGraph("s1", "s2");
    verifyEdgeInGraph("s2", "s4__1");
    verifyEdgeInGraph("s2", "s4__2");
    verifyEdgeInGraph("s4__1", "s4__3");
    verifyEdgeInGraph("s4__2", "s4__3");
    verifyEdgeInGraph("s4__3", "s7");
    verifyEdgeInGraph("s1", "s3");
    verifyEdgeInGraph("s2", "s5__1__1");
    verifyEdgeInGraph("s5__1__1", "s5__1__2");
    verifyEdgeInGraph("s5__1__2", "s5__4");
    verifyEdgeInGraph("s5__4", "s6");
    verifyEdgeInGraph("s3", "s5__1__1");
    verifyEdgeInGraph("s3", "s5__2");
    verifyEdgeInGraph("s5__2", "s5__4");
    verifyEdgeInGraph("s5__2", "s5__3");
    verifyEdgeInGraph("s5__3", "s6");
    verifyEdgeInGraph("s6", "s7");
  }

  @Test
  public void testNoOrphanedTails() throws Exception {
    Step realTail = getComplexNestedWorkflowTail();
    Set<Step> allSteps = WorkflowDiagram.reachableSteps(Collections.singleton(realTail));
    for (Step badTail : allSteps) {
      if (badTail != realTail) {
        try {
          WorkflowDiagram.verifyNoOrphanedTailStep(badTail);
          fail("badTail: " + badTail);
        } catch (RuntimeException e) {
          // pass
        }
      }
    }
  }

  private Step getComplexNestedWorkflowTail() throws Exception {
    DataStore d1 = getFakeDS("d1");
    DataStore d2 = getFakeDS("d2");
    DataStore d3 = getFakeDS("d3");
    DataStore d4 = getFakeDS("d4");
    DataStore d5 = getFakeDS("d5");
    DataStore d6 = getFakeDS("d6");
    DataStore d7 = getFakeDS("d7");
    DataStore id1 = getFakeDS("id1");
    DataStore id2 = getFakeDS("id2");
    DataStore id3 = getFakeDS("id3");
    DataStore id4 = getFakeDS("id4");

    Step s1 = new Step(new FakeAction("s1", new DataStore[0], new DataStore[0]));
    Step s2 = new Step(new FakeAction("s2", new DataStore[0], new DataStore[]{d1, d2}), s1);
    Step s3 = new Step(new FakeAction("s3", new DataStore[0], new DataStore[]{d3}), s1);

    Step s4_1 = new Step(new FakeAction("1", new DataStore[]{d1}, new DataStore[]{d1, id1}));
    Step s4_2 = new Step(new FakeAction("2", new DataStore[]{d2}, new DataStore[]{id2}));
    Step s4_3 = new Step(new FakeAction("3", new DataStore[]{d1, id1, id2},
      new DataStore[]{d4}), s4_1, s4_2);
    Step s4 = new Step(new FakeMultistepAction("s4", new Step[]{s4_1, s4_2, s4_3}), s2);

    Step s5_1_1 = new Step(new FakeAction("1", new DataStore[]{d2, d3}, new DataStore[]{d3}));
    Step s5_1_2 = new Step(new FakeAction("2", new DataStore[]{d3}, new DataStore[]{id3}),
      s5_1_1);
    Step s5_1 = new Step(new FakeMultistepAction("1", new Step[]{s5_1_1, s5_1_2}));

    Step s5_2 = new Step(new FakeAction("2", new DataStore[]{d3}, new DataStore[]{id4}), s3);
    Step s5_3 = new Step(new FakeAction("3", new DataStore[]{id4}, new DataStore[]{d6}), s5_2);
    Step s5_4 = new Step(new FakeAction("4", new DataStore[]{id3, id4}, new DataStore[]{d5}),
      s5_1, s5_2);
    Step s5 = new Step(new FakeMultistepAction("s5", new Step[]{s5_1, s5_2, s5_3, s5_4}), s2, s3);

    Step s6 = new Step(new FakeAction("s6", new DataStore[]{d5, d6}, new DataStore[]{d7}), s5);

    return new Step(new FakeAction("s7", new DataStore[]{d1, d4, d7}, new DataStore[0]), s4,
      s6);
  }

  private void setupWorkflowGraph(WorkflowDiagram wfd) {
    graph = wfd.getDiagramGraph();
    populateNameToVertex(graph);
  }

  private WorkflowDiagram getWorkflowDiagramFromTails(Step first, Step... rest) {
    WorkflowRunner wfr = new WorkflowRunner("Test Workflow", getTestRoot() + "/test_workflow",
        new TestWorkflowOptions().setMaxConcurrentSteps(1),
        first, rest);
    return new WorkflowDiagram(wfr, wfr.getPersistence());
  }

  private static DataStore getFakeDS(String name) throws Exception {
    return new BytesDataStore(null, name, "/tmp/", name);
  }

  private void populateNameToVertex(DirectedGraph<Vertex, DefaultEdge> graph) {
    idToVertex = new HashMap<String, Vertex>();
    for (Vertex v : graph.vertexSet()) {
      idToVertex.put(v.getId(), v);
    }
  }

  private void verifyNumVertices(int expectedNumVertices) {
    assertEquals("Wrong number of vertices in workflow graph.", expectedNumVertices,
      graph.vertexSet().size());
  }

  private void verifyNumEdges(int expectedNumEdges) {
    assertEquals("Wrong number of edges in workflow graph.", expectedNumEdges,
      graph.edgeSet().size());
  }

  private void verifyVertexInGraph(String vname) {
    assertTrue("Vertex " + vname + " should exist in graph", idToVertex.containsKey(vname));
  }

  private void verifyEdgeInGraph(String sourceName, String targetName) {
    boolean edgeExists = false;
    Set<DefaultEdge> outgoingEdges = graph.outgoingEdgesOf(idToVertex.get(sourceName));
    for (DefaultEdge edge : outgoingEdges) {
      if (graph.getEdgeTarget(edge).equals(idToVertex.get(targetName))) {
        edgeExists = true;
      }
    }
    assertTrue("Edge " + sourceName + ", " + targetName + " should exist in graph", edgeExists);
  }

}
