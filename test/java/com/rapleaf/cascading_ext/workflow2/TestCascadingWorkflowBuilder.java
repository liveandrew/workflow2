package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.assembly.BloomJoin;
import com.liveramp.cascading_ext.assembly.Increment;
import com.liveramp.cascading_tools.EmptyListener;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.assembly.Distinct;
import com.rapleaf.cascading_ext.assembly.FastSum;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.CategoryBucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.SplitBucketDataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedThriftBucketDataStoreHelper;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.map_side_join.extractors.TByteArrayExtractor;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.TMSJDataStore;
import com.rapleaf.cascading_ext.pipe.PipeFactory;
import com.rapleaf.cascading_ext.tap.TapFactory;
import com.rapleaf.cascading_ext.tap.bucket2.ThriftBucketScheme;
import com.rapleaf.cascading_ext.test.TExtractorComparator;
import com.rapleaf.cascading_ext.workflow2.SinkBinding.DSSink;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.formats.test.TupleDataStoreHelper;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.IdentitySumm;
import com.rapleaf.types.new_person_data.PIN;
import com.rapleaf.types.new_person_data.PINAndOwners;
import com.rapleaf.types.new_person_data.StringList;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestCascadingWorkflowBuilder extends WorkflowTestCase {

  public static final TByteArrayExtractor DIE_EID_EXTRACTOR = new TByteArrayExtractor(DustinInternalEquiv._Fields.EID);
  public static final TByteArrayExtractor ID_SUMM_EID_EXTRACTOR = new TByteArrayExtractor(IdentitySumm._Fields.EID);
  public static final TExtractorComparator<DustinInternalEquiv, BytesWritable> DIE_EID_COMPARATOR =
      new TExtractorComparator<>(DIE_EID_EXTRACTOR);

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

  private TupleDataStore input;
  private TupleDataStore input2;

  private static final List<Tuple> TUPLES1 = Lists.newArrayList(new Tuple("red", 3),
      new Tuple("red", 5),
      new Tuple("green", 8),
      new Tuple("blue", 7),
      new Tuple("blue", 3),
      new Tuple("blue", 5));

  private static final List<Tuple> TUPLES2 = Lists.newArrayList(new Tuple("red"),
      new Tuple("red"),
      new Tuple("green"),
      new Tuple("blue"),
      new Tuple("blue"),
      new Tuple("blue"));

  private static final List<Tuple> TUPLE_SUMS = Lists.newArrayList(
      new Tuple("blue", 15l),
      new Tuple("green", 8l),
      new Tuple("red", 8l)
  );

  private static final List<Tuple> TUPLE_NAMES = Lists.newArrayList(
      new Tuple("blue"),
      new Tuple("green"),
      new Tuple("red")
  );

  @Before
  public void prepare() throws Exception {
    DataStoreBuilder builder = new DataStoreBuilder(getTestRoot() + "/insAndOuts");
    input = builder.getTupleDataStore("input", new Fields("field1", "field2"));
    input2 = builder.getTupleDataStore("input2", new Fields("field3"));

    TupleDataStoreHelper.writeToStore(input, TUPLES1);
    TupleDataStoreHelper.writeToStore(input2, TUPLES2);
  }

  @Test
  public void testStraightPipe() throws IOException {
    TupleDataStore store1 = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1", "sum"));
    TupleDataStore store2 = builder().getTupleDataStore(getTestRoot() + "/store2", new Fields("field1", "sum"));

    //  fill in stores to make sure creates() works
    TupleDataStoreHelper.writeToStore(store1, new Tuple("test tuple", 80085l));
    TupleDataStoreHelper.writeToStore(store2, new Tuple("test tuple", 80085l));

    CascadingWorkflowBuilder workflow = new CascadingWorkflowBuilder(getTestRoot() + "/e-workflow", "Test");

    Pipe pipe = workflow.bindSource("pipe", new SourceStoreBinding(Lists.newArrayList(input),
            new TapFactory.SimpleFactory(input), new PipeFactory.Fresh()),
        new ActionCallback.Default()
    );

    pipe = new Each(pipe, new Insert(new Fields("field3"), 3), Fields.ALL);
    pipe = new Increment(pipe, "Test", "Tuples");
    pipe = workflow.addCheckpoint(pipe);

    pipe = new Each(pipe, new Insert(new Fields("field4"), "four"), Fields.ALL);
    pipe = new Increment(pipe, "Test", "Tuples2");
    pipe = workflow.addCheckpoint(pipe);

    pipe = new Retain(pipe, new Fields("field1", "field2"));
    pipe = workflow.addCheckpoint(pipe);

    Pipe pipe2 = new Pipe("pipe2", pipe);
    pipe2 = new FastSum(pipe2, new Fields("field1"), new Fields("field2"));
    pipe2 = new Increment(pipe2, "Test", "Tuples4");

    Pipe pipe3 = new Pipe("pipe3", pipe);
    pipe3 = new FastSum(pipe3, new Fields("field1"), new Fields("field2"));
    pipe3 = new Increment(pipe3, "Test", "Tuples5");

    Step step = workflow.buildTail("last-step",
        Lists.newArrayList(new DSSink(pipe3, store1), new DSSink(pipe2, store2)));

    execute(step);

    List<Tuple> allTuples = HRap.getAllTuples(store1.getTap());
    List<Tuple> allTuples2 = HRap.getAllTuples(store2.getTap());

    assertCollectionEquivalent(TUPLE_SUMS, allTuples);
    assertCollectionEquivalent(TUPLE_SUMS, allTuples2);
  }

  @Test
  public void testMultiSourcePipes() throws Exception {
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1"));

    execute(buildComplex(output));

    assertEquals(TUPLE_NAMES, HRap.getAllTuples(output.getTap()));
  }

  @Test
  public void testBuildStep() throws IOException {
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1"));

    execute(buildComplex(output));

    assertEquals(TUPLE_NAMES, HRap.getAllTuples(output.getTap()));
  }

  @Test
  public void testTapVsDs() throws IOException, TException {

    final CategoryBucketDataStore<StringList> inputSplit =
        builder().getCategoryBucketDataStore("split_store", new ThriftBucketScheme<>(StringList.class));
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("string_list"));

    StringList prevList = new StringList(Lists.newArrayList("a"));
    StringList keepList = new StringList(Lists.newArrayList("b"));


    ThriftBucketHelper.writeToBucket(inputSplit.getCategory("1").getBucket(),
        prevList);

    ThriftBucketHelper.writeToBucket(inputSplit.getCategory("2").getBucket(),
        keepList);

    CascadingWorkflowBuilder workflow = new CascadingWorkflowBuilder(getTestRoot() + "/e-workflow", "Test");

    Pipe pipe1 = workflow.bindSource("pipe1", new SourceStoreBinding(Lists.newArrayList(inputSplit),
            new TapFactory() {
              @Override
              public Tap createTap() throws IOException {
                return inputSplit.getCategory("2").getTap();
              }
            }, new PipeFactory.Fresh()),
        new ActionCallback.Default()
    );

    execute(workflow.buildTail(pipe1, output));

    assertCollectionEquivalent(Lists.<Tuple>newArrayList(new Tuple(keepList)),
        HRap.getAllTuples(output.getTap()));

    assertCollectionEquivalent(Lists.newArrayList(keepList, prevList),
        HRap.getValuesFromBucket(inputSplit));

  }

  @Test
  public void testCallback() throws IOException {
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1", "field2"));
    CascadingWorkflowBuilder workflow = new CascadingWorkflowBuilder(getTestRoot() + "/e-workflow", "Test");

    Pipe pipe1 = workflow.bindSource("pipe1", new SourceStoreBinding(Lists.newArrayList(input),
            new TapFactory.SimpleFactory(input),
            new PipeFactory.Fresh()),
        new ActionCallback.Default()
    );

    final AtomicBoolean isCompleted = new AtomicBoolean(false);

    execute(workflow.buildTail("tail-step", pipe1, output, new EmptyListener() {
      @Override
      public void onCompleted(Flow flow) {
        isCompleted.set(true);
      }
    }));

    assertTrue(isCompleted.get());
  }

  private Step buildComplex(DataStore output) throws IOException {
    CascadingWorkflowBuilder workflow = new CascadingWorkflowBuilder(getTestRoot() + "/e-workflow", "Test");

    Pipe pipe = workflow.bindSource("pipe1", new SourceStoreBinding(Lists.newArrayList(input),
            new TapFactory.SimpleFactory(input),
            new PipeFactory.Fresh()),
        new ActionCallback.Default()
    );

    Pipe pipe2 = workflow.bindSource("pipe2", new SourceStoreBinding(Lists.newArrayList(input2),
            new TapFactory.SimpleFactory(input2),
            new PipeFactory.Fresh()),
        new ActionCallback.Default()
    );

    pipe2 = new Distinct(pipe2);
    pipe2 = workflow.addCheckpoint(pipe2, "distinct");

    Pipe pipe3 = new BloomJoin(pipe, new Fields("field1"), pipe2, new Fields("field3"));
    pipe3 = new Increment(pipe3, "Test", "Tuples1");
    pipe3 = workflow.addCheckpoint(pipe3, "group");

    pipe3 = new Distinct(pipe3, new Fields("field1"));
    pipe3 = new Retain(pipe3, new Fields("field1"));

    Pipe finalPipe = new Pipe("final", pipe3);

    return workflow.buildTail(finalPipe, output);
  }

  // simplest case - start of a flow do a MSJ
  @Test
  public void testMSJ1() throws IOException, TException, InterruptedException {

    BucketDataStore<DustinInternalEquiv> baseStore = builder().getBucketDataStore("base", DustinInternalEquiv.class);
    BucketDataStore<DustinInternalEquiv> deltaStore = builder().getBucketDataStore("delta", DustinInternalEquiv.class);
    BucketDataStore<DustinInternalEquiv> output = builder().getBucketDataStore("output", DustinInternalEquiv.class);

    ThriftBucketHelper.writeToBucketAndSort(baseStore.getBucket(), DIE_EID_COMPARATOR,
        die1
    );

    ThriftBucketHelper.writeToBucketAndSort(deltaStore.getBucket(), DIE_EID_COMPARATOR,
        die2
    );

    MSJDataStore msjStore = new TMSJDataStore<DustinInternalEquiv>(getTestRoot() + "/msj_store", DustinInternalEquiv.class, DIE_EID_EXTRACTOR, 100.0);

    msjStore.commitBase(baseStore.getPath());
    msjStore.commitDelta(deltaStore.getPath());


    //  run workflow
    CascadingWorkflowBuilder builder = new CascadingWorkflowBuilder(getTestRoot() + "/tmp", "Test");

    Pipe pipe = builder.bindSource("pipe1", new SourceStoreBinding(Lists.newArrayList(msjStore),
            new TapFactory.SimpleFactory(msjStore),
            new PipeFactory.Fresh()),
        new ActionCallback.Default()
    );

    pipe = new Increment(pipe, "counter_group", "counter1");

    pipe = builder.addCheckpoint(pipe);
    pipe = new Increment(pipe, "counter_group", "counter2");

    execute(builder.buildTail(pipe, output));

    assertCollectionEquivalent(Lists.newArrayList(die1, die2),
        HRap.<DustinInternalEquiv>getValuesFromBucket(output));

  }

  @Test
  public void testDelayedTapCreation() throws Exception {
    BucketDataStore output = builder().getBucketDataStore("output", PIN.class);
    PIN email = PIN.email("ben@gmail.com");

    //  empty version
    VersionedBucketDataStore store = builder().getVersionedBucketDataStore("store", PIN.class);
    VersionedThriftBucketDataStoreHelper.writeToNewVersion(store);

    //  put together workflow
    CascadingWorkflowBuilder builder = new CascadingWorkflowBuilder(getTestRoot() + "/tmp", "Test");

    Pipe source = builder.bindSource("pipe1", new SourceStoreBinding(Lists.newArrayList(store),
            new TapFactory.SimpleFactory(store),
            new PipeFactory.Fresh()),
        new ActionCallback.Default()
    );

    Step tail = builder.buildTail("tail", source, output);

    //  actual version
    VersionedThriftBucketDataStoreHelper.writeToNewVersion(store, email);

    execute(tail);

    assertCollectionEquivalent(Lists.newArrayList(email), HRap.<PIN>getValuesFromBucket(output));
  }

  @Test
  public void testNullSink() throws Exception {

    BucketDataStore<DustinInternalEquiv> store1 = builder().getBucketDataStore("base", DustinInternalEquiv.class);

    ThriftBucketHelper.writeToBucket(store1.getBucket(),
        die1,
        die3
    );

    CascadingWorkflowBuilder builder = new CascadingWorkflowBuilder(getTestRoot() + "/tmp", "Test");

    Pipe pipe1 = builder.bindSource("pipe1", new SourceStoreBinding(Lists.newArrayList(store1),
            new TapFactory.SimpleFactory(store1),
            new PipeFactory.Fresh()),
        new ActionCallback.Default()
    );

    pipe1 = new Increment(pipe1, "DIES", "COUNT");

    WorkflowRunner output1 = execute(builder.buildNullTail(pipe1),
        WorkflowOptions.test());

    TwoNestedMap<String, String, Long> counters = output1.getPersistence().getFlatCounters();
    assertEquals(new Long(2), counters.get("DIES").get("COUNT"));

  }

}
