package com.rapleaf.cascading_ext.workflow2;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.liveramp.cascading_ext.assembly.Increment;
import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.formats.test.TupleDataStoreHelper;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

public class TestCheckpointedCascadingAction2 extends CascadingExtTestCase {

  public static class SimpleExampleCheckpointedAction extends CheckpointedCascadingAction2 {
    public SimpleExampleCheckpointedAction(String checkpointToken, String tmpRoot,
                                           DataStore input, DataStore output) throws IOException {
      super(checkpointToken, tmpRoot, Maps.newHashMap());

      Pipe source = bindSource("source", input);
      source = addCheckpoint(source, "intermediate");

      complete("step", source, output);
    }
  }

  public static class SimpleTwoSinkCheckpointedAction extends CheckpointedCascadingAction2 {
    public SimpleTwoSinkCheckpointedAction(String checkpointToken, String tmpRoot,
                                           DataStore input, DataStore output1, DataStore output2) throws IOException {
      super(checkpointToken, tmpRoot, Maps.newHashMap());

      Pipe source = bindSource("source", input);
      source = new Increment(source, "Group", "Counter");
      source = new GroupBy(source, new Fields("field"));

      Pipe sink1 = new Pipe("sink1", source);

      Pipe sink2 = new Pipe("sink2", source);

      complete("tail-step", Lists.newArrayList(
          new SinkBinding(sink1, output1),
          new SinkBinding(sink2, output2)));
    }
  }

  public static class Fail1 extends CheckpointedCascadingAction2 {

    public Fail1(String checkpointToken, String tmpRoot,
                                           DataStore input, DataStore output1, DataStore output2) throws IOException {
      super(checkpointToken, tmpRoot, Maps.newHashMap());

      Pipe source = bindSource("source", input);
      source = new Increment(source, "Group", "Counter");
      source = new GroupBy(source, new Fields("field"));

      Pipe sink1 = new Each(source, new Identity());
      Pipe sink2 = new Each(source, new Identity());

      complete("tail-step", Lists.newArrayList(
          new SinkBinding(sink1, output1),
          new SinkBinding(sink2, output2)));
    }
  }

  @Test
  public void testIt() throws IOException {

    TupleDataStore input = builder().getTupleDataStore("input", new Fields("field"));
    TupleDataStore output = builder().getTupleDataStore("output", new Fields("field"));

    List<Tuple> data = Lists.<Tuple>newArrayList(new Tuple("data"));
    TupleDataStoreHelper.writeToStore(input, data);

    executeWorkflow(new SimpleExampleCheckpointedAction("token", getTestRoot() + "/tmp", input, output));

    assertCollectionEquivalent(data, HRap.getAllTuples(input.getTap()));
    assertCollectionEquivalent(data, HRap.getAllTuples(output.getTap()));
  }

  @Test
  public void testTwoSink() throws IOException {

    TupleDataStore input = builder().getTupleDataStore("input", new Fields("field"));
    TupleDataStore output = builder().getTupleDataStore("output1", new Fields("field"));
    TupleDataStore output1 = builder().getTupleDataStore("output2", new Fields("field"));

    List<Tuple> data = Lists.<Tuple>newArrayList(new Tuple("data"));
    TupleDataStoreHelper.writeToStore(input, data);

    WorkflowRunner token = executeWorkflow(new SimpleTwoSinkCheckpointedAction("token", getTestRoot() + "/tmp", input, output, output1));

    //  make sure counter only got incremented once
    assertEquals(Collections.<String, Long>singletonMap("Counter", 1l), token.getCounterMap().get("Group"));

    assertCollectionEquivalent(data, HRap.getAllTuples(output.getTap()));
    assertCollectionEquivalent(data, HRap.getAllTuples(output1.getTap()));
  }

  @Test
  public void testCheckTailNames() throws IOException {

    final TupleDataStore input = builder().getTupleDataStore("input", new Fields("field"));
    final TupleDataStore output = builder().getTupleDataStore("output1", new Fields("field"));
    final TupleDataStore output1 = builder().getTupleDataStore("output2", new Fields("field"));

    Exception token = getException(new Callable() {
      @Override
      public Object call() throws Exception {
        executeWorkflow(new Fail1("token", getTestRoot() + "/tmp", input, output, output1));
        return null;
      }
    });

    assertEquals("Pipe with name source already exists!", token.getMessage());
  }
}
