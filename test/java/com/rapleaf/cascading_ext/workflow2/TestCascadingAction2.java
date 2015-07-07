package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.assembly.Increment;
import com.liveramp.commons.collections.list.ListBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.function.ExpandThrift;
import com.rapleaf.cascading_ext.map_side_join.extractors.TByteArrayExtractor;
import com.rapleaf.cascading_ext.test.TExtractorComparator;
import com.rapleaf.cascading_ext.workflow2.SinkBinding.DSSink;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilters;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.formats.test.TupleDataStoreHelper;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.IdentitySumm;
import com.rapleaf.types.new_person_data.PIN;
import com.rapleaf.types.new_person_data.PINAndOwners;

import static org.junit.Assert.assertEquals;

public class TestCascadingAction2 extends WorkflowTestCase {

  public static final TByteArrayExtractor DIE_EID_EXTRACTOR = new TByteArrayExtractor(DustinInternalEquiv._Fields.EID);
  public static final TByteArrayExtractor ID_SUMM_EID_EXTRACTOR = new TByteArrayExtractor(IdentitySumm._Fields.EID);
  public static final TExtractorComparator<DustinInternalEquiv, BytesWritable> DIE_EID_COMPARATOR =
      new TExtractorComparator<DustinInternalEquiv, BytesWritable>(DIE_EID_EXTRACTOR);
  public static final TExtractorComparator<IdentitySumm, BytesWritable> ID_SUMM_EID_COMPARATOR =
      new TExtractorComparator<IdentitySumm, BytesWritable>(ID_SUMM_EID_EXTRACTOR);

  public static final PIN PIN1 = PIN.email("ben@gmail.com");
  public static final PIN PIN2 = PIN.email("ben@liveramp.com");
  public static final PIN PIN3 = PIN.email("ben@yahoo.com");


  public static final DustinInternalEquiv die1 = new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN1, 0);
  public static final DustinInternalEquiv die2 = new DustinInternalEquiv(ByteBuffer.wrap("2".getBytes()), PIN2, 0);
  public static final DustinInternalEquiv die3 = new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN3, 0);

  public static final IdentitySumm SUMM = new IdentitySumm(ByteBuffer.wrap("1".getBytes()), Lists.<PINAndOwners>newArrayList());
  public static final IdentitySumm SUMM_AFTER = new IdentitySumm(ByteBuffer.wrap("1".getBytes()), Lists.<PINAndOwners>newArrayList(
      new PINAndOwners(PIN1),
      new PINAndOwners(PIN3)
  ));


  public static class SimpleExampleAction extends CascadingAction2 {
    public SimpleExampleAction(String checkpointToken, String tmpRoot,
                               DataStore input, DataStore output) throws IOException, ClassNotFoundException {
      super(checkpointToken, tmpRoot, Maps.newHashMap());

      Pipe source = bindSource("source", input);
      source = addCheckpoint(source, "intermediate");

      complete("step", source, output);
    }
  }

  public static class SimpleTwoSinkAction extends CascadingAction2 {
    public SimpleTwoSinkAction(String checkpointToken, String tmpRoot,
                               DataStore input, DataStore output1, DataStore output2) throws IOException {
      super(checkpointToken, tmpRoot, Maps.newHashMap());

      Pipe source = bindSource("source", input);
      source = new Increment(source, "Group", "Counter");
      source = new GroupBy(source, new Fields("field"));

      Pipe sink1 = new Pipe("sink1", source);

      Pipe sink2 = new Pipe("sink2", source);

      complete("tail-step", Lists.newArrayList(
          new DSSink(sink1, output1),
          new DSSink(sink2, output2)));
    }
  }

  public static class SimpleMSJAction extends CascadingAction2 {

    public SimpleMSJAction(String checkpointToken, String tmpRoot,
                           BucketDataStore<DustinInternalEquiv> store1,
                           BucketDataStore<IdentitySumm> store2,
                           BucketDataStore<IdentitySumm> output,
                           Map<Object, Object> flowProperties) throws IOException {
      super(checkpointToken, tmpRoot, flowProperties);

      Pipe input = msj("msj-source", new ListBuilder<MSJBinding<BytesWritable>>()
          .add(new SourceMSJBinding<>(DIE_EID_EXTRACTOR, store1))
          .add(new SourceMSJBinding<>(ID_SUMM_EID_EXTRACTOR, store2)).get(),
          new ExampleMultiJoiner());

      input = new Increment(input, "COUNTER", "INCREMENT");

      complete("mjs-plus-cascading", input, output);
    }
  }

  public static class MidMSJAction extends CascadingAction2 {

    public MidMSJAction(String checkpointToken,
                        BucketDataStore store1,
                        BucketDataStore store2,
                        BucketDataStore sink,
                        String tmpRoot, Map<Object, Object> flowProperties) throws IOException {
      super(checkpointToken, tmpRoot, flowProperties);

      Pipe pipe1 = bindSource("pipe1", store1);
      pipe1 = new Increment(pipe1, "DIES", "COUNT");
      pipe1 = new Each(pipe1, new Fields("dustin_internal_equiv"),
          new ExpandThrift(DustinInternalEquiv.class),
          new Fields("dustin_internal_equiv", "eid"));
      pipe1 = new GroupBy(pipe1, new Fields("eid"));

      Pipe pipe2 = bindSource("pipe2", store2);
      pipe2 = new Increment(pipe2, "SUMMS", "COUNT");
      pipe2 = new Each(pipe2, new Fields("identity_summ"),
          new ExpandThrift(IdentitySumm.class),
          new Fields("identity_summ", "eid"));
      pipe2 = new GroupBy(pipe2, new Fields("eid"));

      Pipe summ = msj("msj-step", new ListBuilder<MSJBinding<BytesWritable>>()
          .add(new FlowMSJBinding<>(DIE_EID_EXTRACTOR, pipe1, "dustin_internal_equiv", DustinInternalEquiv.class))
          .add(new FlowMSJBinding<>(ID_SUMM_EID_EXTRACTOR, pipe2, "identity_summ", IdentitySumm.class)).get(),
          new ExampleMultiJoiner());
      summ = new Increment(summ, "AFTER", "COUNT");

      complete("thestep", summ, sink);

    }
  }

  public static class Fail1 extends CascadingAction2 {

    public Fail1(String checkpointToken, String tmpRoot,
                 DataStore input, DataStore output1, DataStore output2) throws IOException {
      super(checkpointToken, tmpRoot, Maps.newHashMap());

      Pipe source = bindSource("source", input);
      source = new Increment(source, "Group", "Counter");
      source = new GroupBy(source, new Fields("field"));

      Pipe sink1 = new Each(source, new Identity());
      Pipe sink2 = new Each(source, new Identity());

      complete("tail-step", Lists.newArrayList(
          new DSSink(sink1, output1),
          new DSSink(sink2, output2)));
    }
  }

  @Test
  public void testIt() throws IOException, ClassNotFoundException {

    TupleDataStore input = builder().getTupleDataStore("input", new Fields("field"));
    TupleDataStore output = builder().getTupleDataStore("output", new Fields("field"));

    List<Tuple> data = Lists.<Tuple>newArrayList(new Tuple("data"));
    TupleDataStoreHelper.writeToStore(input, data);

    execute(new SimpleExampleAction("token", getTestRoot() + "/tmp", input, output),
        new TestWorkflowOptions().setCounterFilter(CounterFilters.all()));

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

    WorkflowRunner token = execute(new SimpleTwoSinkAction("token", getTestRoot() + "/tmp", input, output, output1),
        new TestWorkflowOptions().setCounterFilter(CounterFilters.all()));

    //  make sure counter only got incremented once
    assertEquals(Collections.singletonMap("Counter", 1l), token.getPersistence().getFlatCounters().get("Group"));

    assertCollectionEquivalent(data, HRap.getAllTuples(output.getTap()));
    assertCollectionEquivalent(data, HRap.getAllTuples(output1.getTap()));
  }

  @Test
  public void testCheckTailNames() throws IOException {

    final TupleDataStore input = builder().getTupleDataStore("input", new Fields("field"));
    final TupleDataStore output = builder().getTupleDataStore("output1", new Fields("field"));
    final TupleDataStore output1 = builder().getTupleDataStore("output2", new Fields("field"));

    Exception token = getException(new Runnable2() {
      @Override
      public void run() throws Exception {
        execute(new Fail1("token", getTestRoot() + "/tmp", input, output, output1));
      }
    });

    assertEquals("Pipe with name source already exists!", token.getMessage());
  }

  @Test
  public void testMSJ() throws Exception {

    BucketDataStore<DustinInternalEquiv> store1 = builder().getBucketDataStore("base", DustinInternalEquiv.class);
    BucketDataStore<IdentitySumm> store2 = builder().getBucketDataStore("delta", IdentitySumm.class);
    BucketDataStore<IdentitySumm> output = builder().getBucketDataStore("output", IdentitySumm.class);

    ThriftBucketHelper.writeToBucketAndSort(store1.getBucket(), DIE_EID_COMPARATOR,
        die1,
        die3
    );

    ThriftBucketHelper.writeToBucketAndSort(store2.getBucket(), ID_SUMM_EID_COMPARATOR,
        SUMM
    );

    WorkflowRunner token = execute(new SimpleMSJAction("token", getTestRoot() + "/tmp", store1, store2, output, Collections.emptyMap()),
        new TestWorkflowOptions().setCounterFilter(CounterFilters.all()));

    assertEquals(new Long(1l), token.getPersistence().getFlatCounters().get("COUNTER").get("INCREMENT"));
    assertCollectionEquivalent(Lists.newArrayList(SUMM_AFTER), HRap.getValuesFromBucket(output));

  }

  @Test
  public void testMidMSJ() throws Exception {

    BucketDataStore<DustinInternalEquiv> store1 = builder().getBucketDataStore("base", DustinInternalEquiv.class);
    BucketDataStore<IdentitySumm> store2   = builder().getBucketDataStore("delta", IdentitySumm.class);
    BucketDataStore<IdentitySumm> output = builder().getBucketDataStore("output", IdentitySumm.class);

    ThriftBucketHelper.writeToBucket(store1.getBucket(),
        die1,
        die3
    );

    ThriftBucketHelper.writeToBucket(store2.getBucket(),
        SUMM
    );

    WorkflowRunner output1 = execute(new MidMSJAction("token", store1, store2, output, getTestRoot()+"/tmp", Collections.emptyMap()),
        new TestWorkflowOptions().setCounterFilter(CounterFilters.all()));

    TwoNestedMap<String, String, Long> counters = output1.getPersistence().getFlatCounters();
    assertEquals(new Long(2), counters.get("DIES").get("COUNT"));
    assertEquals(new Long(1), counters.get("SUMMS").get("COUNT"));
    assertEquals(new Long(1), counters.get("AFTER").get("COUNT"));

    assertCollectionEquivalent(Lists.newArrayList(SUMM_AFTER), HRap.getValuesFromBucket(output));
  }


}
