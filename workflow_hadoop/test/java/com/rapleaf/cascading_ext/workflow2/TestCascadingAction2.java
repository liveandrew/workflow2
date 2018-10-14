package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TException;
import org.junit.Test;

import cascading.flow.FlowProcess;
import cascading.operation.Identity;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.liveramp.cascading_ext.assembly.Increment;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.filter.DirectFilter;
import com.rapleaf.cascading_ext.map_side_join.extractors.TByteArrayExtractor;
import com.rapleaf.cascading_ext.pipe.PipeFactory;
import com.rapleaf.cascading_ext.tap.TapFactory;
import com.rapleaf.cascading_ext.tap.bucket2.BucketTap2;
import com.rapleaf.cascading_ext.tap.bucket2.ThriftBucketScheme;
import com.rapleaf.cascading_ext.test.TExtractorComparator;
import com.rapleaf.cascading_ext.workflow2.SinkBinding.DSSink;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;
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
        HadoopWorkflowOptions.test());

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
        HadoopWorkflowOptions.test());

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
  public void testPreExecuteHook() throws IOException, TException {

    BucketDataStore<PIN> input = builder().getBucketDataStore("input", PIN.class);
    BucketDataStore<PIN> sideOutput = builder().getBucketDataStore("side-output", PIN.class);
    BucketDataStore<PIN> realOutput = builder().getBucketDataStore("real-output", PIN.class);

    ThriftBucketHelper.writeToBucket(input.getBucket(), PIN.email("good@gmail.com"));
    //  this'll write to some part-1231313214142141 file so we'll get 2 in output if creates() doesn't work
    ThriftBucketHelper.writeToBucket(sideOutput.getBucket(), PIN.email("bad@gmail.com"));

    execute(new MyFancyAction(
        "somestep",
        getTestRoot() + "/tmp",
        input,
        sideOutput,
        realOutput
    ));

    assertEquals(Lists.newArrayList(PIN.email("good@gmail.com")), HRap.getValuesFromBucket(sideOutput));
  }

  @Test
  public void testDoubleBinding() throws IOException, TException {
    BucketDataStore<PIN> input1 = builder().getBucketDataStore("input1", PIN.class);
    BucketDataStore<PIN> input2 = builder().getBucketDataStore("input2", PIN.class);
    BucketDataStore<PIN> output = builder().getBucketDataStore("real-output", PIN.class);

    ThriftBucketHelper.writeToBucket(input1.getBucket(), PIN.email("good@gmail.com"));
    ThriftBucketHelper.writeToBucket(input2.getBucket(), PIN.email("other@gmail.com"));

    execute(new MyDoubleAction(
        "somestep",
        getTestRoot() + "/tmp",
        Sets.<TypedStoreBinding>newHashSet(
            new TypedStoreBinding<>(Collections.singleton(input1), new TapFactory.SimpleFactory(input1), new PipeFactory.Fresh()),
            new TypedStoreBinding<>(Collections.singleton(input2), new TapFactory.SimpleFactory(input2), new PipeFactory.Fresh())),
        output
    ));

    assertCollectionEquivalent(Sets.newHashSet(PIN.email("good@gmail.com"), PIN.email("other@gmail.com")), HRap.getValuesFromBucket(output));
  }

  private static class MyDoubleAction extends CascadingAction2 {
    public MyDoubleAction(String checkpointToken, String tmpRoot,
                          Collection<TypedStoreBinding> ssbs,
                          final BucketDataStore<PIN> output) {
      super(checkpointToken, tmpRoot);

      Pipe pipe = bindSources("in", ssbs);
      pipe = new Each(pipe, new Identity());
      complete("out", pipe, output);
    }
  }

  public static class MyFancyAction extends CascadingAction2 {

    public MyFancyAction(String checkpointToken, String tmpRoot,
                         BucketDataStore<PIN> input1,
                         final BucketDataStore<PIN> sideOutput,
                         BucketDataStore<PIN> realOutput) {
      super(checkpointToken, tmpRoot);

      final SomeFunction myFunc = new SomeFunction(sideOutput.getPath());

      Pipe pipe = bindSource("input", input1, new ActionCallback.Default() {

        @Override
        public void construct(Action.ConstructContext context) {
          context.creates(sideOutput);
        }

        @Override
        public void prepare(Action.PreExecuteContext context) throws IOException {
          sideOutput.getBucket();
          myFunc.allowPass();
        }

      });

      pipe = new Each(pipe, new Fields("pin"), myFunc);
      complete("step", pipe, realOutput);

    }
  }

  private static class SomeFunction extends DirectFilter<PIN> {

    private final String output;

    private boolean fail = true;
    private TupleEntryCollector coll;

    public void allowPass(){
      fail = false;
    }

    public SomeFunction(String output) {
      this.output = output;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
      try {
        coll = new BucketTap2<>(output, new ThriftBucketScheme<>(PIN.class)).disableBucketCreation().openForWrite(flowProcess);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean isRemove(PIN value) {
      if(fail){
        throw new RuntimeException("Hook didn't get called");
      }
      coll.add(new Tuple(value));
      return true;
    }

    @Override
    public void flush(FlowProcess flowProcess, OperationCall operationCall) {
      coll.close();
    }

  }

}
