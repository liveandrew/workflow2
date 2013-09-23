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
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.map_side_join.extractors.TByteArrayExtractor;
import com.rapleaf.cascading_ext.test.TExtractorComparator;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.formats.test.TupleDataStoreHelper;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.IdentitySumm;
import com.rapleaf.types.new_person_data.PIN;
import com.rapleaf.types.new_person_data.PINAndOwners;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

public class TestCheckpointedCascadingAction2 extends CascadingExtTestCase {

  private static final TByteArrayExtractor DIE_EID_EXTRACTOR =
      new TByteArrayExtractor(DustinInternalEquiv._Fields.EID);

  private static final TExtractorComparator<DustinInternalEquiv, BytesWritable> DIE_EID_COMPARATOR =
      new TExtractorComparator<DustinInternalEquiv, BytesWritable>(DIE_EID_EXTRACTOR);

  private static final TByteArrayExtractor ID_SUMM_EID_EXTRACTOR =
      new TByteArrayExtractor(IdentitySumm._Fields.EID);

  private static final TExtractorComparator<IdentitySumm, BytesWritable> ID_SUMM_EID_COMPARATOR =
      new TExtractorComparator<IdentitySumm, BytesWritable>(ID_SUMM_EID_EXTRACTOR);

  private static final PIN PIN1 = PIN.email("ben@gmail.com");
  private static final PIN PIN2 = PIN.email("ben@liveramp.com");
  private static final PIN PIN3 = PIN.email("ben@yahoo.com");

  private static final DustinInternalEquiv die1 = new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN1, 0);
  private static final DustinInternalEquiv die2 = new DustinInternalEquiv(ByteBuffer.wrap("2".getBytes()), PIN2, 0);
  private static final DustinInternalEquiv die3 = new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN3, 0);

  private static final IdentitySumm SUMM = new IdentitySumm(ByteBuffer.wrap("1".getBytes()), Lists.<PINAndOwners>newArrayList());
  private static final IdentitySumm SUMM_AFTER = new IdentitySumm(ByteBuffer.wrap("1".getBytes()), Lists.<PINAndOwners>newArrayList(
      new PINAndOwners(PIN1),
      new PINAndOwners(PIN3)
  ));

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
                           Map<Object, Object> flowProperties) {
      super(checkpointToken, tmpRoot, flowProperties);

      Pipe input = bindMSJ("msj-source", Lists.newArrayList(
          new SourceMSJBinding<BytesWritable>(DIE_EID_EXTRACTOR, store1),
          new SourceMSJBinding<BytesWritable>(ID_SUMM_EID_EXTRACTOR, store2)),
        new ExampleMultiJoiner());

      input = new Increment(input, "COUNTER", "INCREMENT");

      complete("mjs-plus-cascading", input, output);
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
    BucketDataStore<IdentitySumm> store2   = builder().getBucketDataStore("delta", IdentitySumm.class);
    BucketDataStore<IdentitySumm> output = builder().getBucketDataStore("output", IdentitySumm.class);

    ThriftBucketHelper.writeToBucketAndSort(store1.getBucket(), DIE_EID_COMPARATOR, die1, die3);
    ThriftBucketHelper.writeToBucketAndSort(store2.getBucket(), ID_SUMM_EID_COMPARATOR, SUMM);

    WorkflowRunner token = executeWorkflow(new SimpleMSJAction("token", getTestRoot() + "/tmp", store1, store2, output, Collections.emptyMap()));

    assertEquals(new Long(1l), token.getCounterMap().get("COUNTER").get("INCREMENT"));
    assertCollectionEquivalent(Lists.<IdentitySumm>newArrayList(SUMM_AFTER), HRap.<IdentitySumm>getValuesFromBucket(output));

  }
}
