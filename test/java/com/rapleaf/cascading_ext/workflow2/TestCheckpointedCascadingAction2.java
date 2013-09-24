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
import com.liveramp.collections.list.ListBuilder;
import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.function.ExpandThrift;
import com.rapleaf.cascading_ext.msj_tap.MSJFixtures;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.formats.test.TupleDataStoreHelper;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.IdentitySumm;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

  public static class SimpleMSJAction extends CheckpointedCascadingAction2 {

    public SimpleMSJAction(String checkpointToken, String tmpRoot,
                           BucketDataStore<DustinInternalEquiv> store1,
                           BucketDataStore<IdentitySumm> store2,
                           BucketDataStore<IdentitySumm> output,
                           Map<Object, Object> flowProperties) throws IOException {
      super(checkpointToken, tmpRoot, flowProperties);

      Pipe input = msj("msj-source", new ListBuilder<MSJBinding<BytesWritable>>()
          .add(new SourceMSJBinding<BytesWritable>(MSJFixtures.DIE_EID_EXTRACTOR, store1))
          .add(new SourceMSJBinding<BytesWritable>(MSJFixtures.ID_SUMM_EID_EXTRACTOR, store2)).get(),
          new ExampleMultiJoiner());

      input = new Increment(input, "COUNTER", "INCREMENT");

      complete("mjs-plus-cascading", input, output);
    }
  }

  public static class MidMSJAction extends CheckpointedCascadingAction2 {

    public MidMSJAction(String checkpointToken,
                        BucketDataStore store1,
                        BucketDataStore store2,
                        BucketDataStore sink,
                        String tmpRoot, Map<Object, Object> flowProperties) throws IOException {
      super(checkpointToken, tmpRoot, flowProperties);

      Pipe pipe1 = bindSource("pipe1", store1);
      pipe1 = new Increment(pipe1, "DIES", "COUNT");
      pipe1 = new Each(pipe1, new Fields("die"),
          new ExpandThrift(DustinInternalEquiv.class),
          new Fields("die", "eid"));
      pipe1 = new GroupBy(pipe1, new Fields("eid"));

      Pipe pipe2 = bindSource("pipe2", store2);
      pipe2 = new Increment(pipe2, "SUMMS", "COUNT");
      pipe2 = new Each(pipe2, new Fields("identity-summ"),
          new ExpandThrift(IdentitySumm.class),
          new Fields("identity-summ", "eid"));
      pipe2 = new GroupBy(pipe2, new Fields("eid"));

      Pipe summ = msj("msj-step", new ListBuilder<MSJBinding<BytesWritable>>()
          .add(new FlowMSJBinding<BytesWritable>(MSJFixtures.DIE_EID_EXTRACTOR, pipe1, "die", DustinInternalEquiv.class))
          .add(new FlowMSJBinding<BytesWritable>(MSJFixtures.ID_SUMM_EID_EXTRACTOR, pipe2, "identity-summ", IdentitySumm.class)).get(),
          new ExampleMultiJoiner());
      summ = new Increment(summ, "AFTER", "COUNT");

      complete("thestep", summ, sink);

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

  @Test
  public void testMSJ() throws Exception {

    BucketDataStore<DustinInternalEquiv> store1 = builder().getBucketDataStore("base", DustinInternalEquiv.class);
    BucketDataStore<IdentitySumm> store2 = builder().getBucketDataStore("delta", IdentitySumm.class);
    BucketDataStore<IdentitySumm> output = builder().getBucketDataStore("output", IdentitySumm.class);

    ThriftBucketHelper.writeToBucketAndSort(store1.getBucket(), MSJFixtures.DIE_EID_COMPARATOR,
        MSJFixtures.die1,
        MSJFixtures.die3
    );

    ThriftBucketHelper.writeToBucketAndSort(store2.getBucket(), MSJFixtures.ID_SUMM_EID_COMPARATOR,
        MSJFixtures.SUMM
    );

    WorkflowRunner token = executeWorkflow(new SimpleMSJAction("token", getTestRoot() + "/tmp", store1, store2, output, Collections.emptyMap()));

    assertEquals(new Long(1l), token.getCounterMap().get("COUNTER").get("INCREMENT"));
    assertCollectionEquivalent(Lists.<IdentitySumm>newArrayList(MSJFixtures.SUMM_AFTER), HRap.<IdentitySumm>getValuesFromBucket(output));

  }

  @Test
  public void testMidMSJ() throws Exception {

    BucketDataStore<DustinInternalEquiv> store1 = builder().getBucketDataStore("base", DustinInternalEquiv.class);
    BucketDataStore<IdentitySumm> store2   = builder().getBucketDataStore("delta", IdentitySumm.class);
    BucketDataStore<IdentitySumm> output = builder().getBucketDataStore("output", IdentitySumm.class);

    ThriftBucketHelper.writeToBucket(store1.getBucket(),
        MSJFixtures.die1,
        MSJFixtures.die3
    );

    ThriftBucketHelper.writeToBucket(store2.getBucket(),
        MSJFixtures.SUMM
    );

    WorkflowRunner output1 = executeWorkflow(new MidMSJAction("token", store1, store2, output, getTestRoot()+"/tmp", Collections.emptyMap()));

    assertEquals(new Long(2), output1.getCounterMap().get("DIES").get("COUNT"));
    assertEquals(new Long(1), output1.getCounterMap().get("SUMMS").get("COUNT"));
    assertEquals(new Long(1), output1.getCounterMap().get("AFTER").get("COUNT"));

    assertCollectionEquivalent(Lists.<IdentitySumm>newArrayList(MSJFixtures.SUMM_AFTER), HRap.<IdentitySumm>getValuesFromBucket(output));
  }
}
