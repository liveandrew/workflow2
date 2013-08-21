package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.Flow;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.collect.Lists;
import com.liveramp.cascading_ext.assembly.BloomJoin;
import com.liveramp.cascading_ext.assembly.Increment;
import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.assembly.Distinct;
import com.rapleaf.cascading_ext.assembly.FastSum;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.DataStores;
import com.rapleaf.cascading_ext.datastore.SplitBucketDataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.formats.test.TupleDataStoreHelper;
import com.rapleaf.formats.test.VersionedSplitBucketDataStoreHelper;
import com.rapleaf.support.test.NPDH;
import com.rapleaf.types.importer.DUVUExtractorSpec;
import com.rapleaf.types.new_person_data.DataUnit;
import com.rapleaf.types.new_person_data.DataUnitValueUnion;
import com.rapleaf.types.new_person_data.DataUnitValueUnion._Fields;
import com.rapleaf.types.person_data.GenderType;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestEasyWorkflow2 extends CascadingExtTestCase {

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

  public void setUp() throws Exception {
    super.setUp();
    DataStoreBuilder builder = new DataStoreBuilder(getTestRoot() + "/insAndOuts");
    input = builder.getTupleDataStore("input", new Fields("field1", "field2"));
    input2 = builder.getTupleDataStore("input2", new Fields("field3"));

    TupleDataStoreHelper.writeToStore(input, TUPLES1);
    TupleDataStoreHelper.writeToStore(input2,TUPLES2);
  }

  public void testStraightPipe() throws IOException {
    TupleDataStore store1 = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1", "sum"));
    TupleDataStore store2 = builder().getTupleDataStore(getTestRoot() + "/store2", new Fields("field1", "sum"));

    //  fill in stores to make sure creates() works
    TupleDataStoreHelper.writeToStore(store1, new Tuple("test tuple", 80085l));
    TupleDataStoreHelper.writeToStore(store2, new Tuple("test tuple", 80085l));

    EasyWorkflow2 workflow = new EasyWorkflow2("Test Workflow", getTestRoot() + "/e-workflow");

    Pipe pipe = workflow.bindSource("pipe", input);

    pipe = new Each(pipe, new Insert(new Fields("field3"), 3), Fields.ALL);
    pipe = new Increment(pipe, "Test", "Tuples");
    pipe = workflow.addCheckpoint(pipe);

    pipe = new Each(pipe, new Insert(new Fields("field4"), "four"), Fields.ALL);
    pipe = new Increment(pipe, "Test", "Tuples2");
    pipe = workflow.addCheckpoint(pipe);

    pipe = new Retain(pipe, new Fields("field1", "field2"));
    pipe = workflow.addCheckpoint(pipe);

    Pipe pipe2 = new FastSum(pipe, new Fields("field1"), new Fields("field2"));
    pipe2 = new Increment(pipe2, "Test", "Tuples4");

    Pipe pipe3 = new FastSum(pipe, new Fields("field1"), new Fields("field2"));
    pipe3 = new Increment(pipe3, "Test", "Tuples5");

    workflow.bindSink("last-step", pipe3, store1);
    workflow.bindSink("other-last-step", pipe2, store2);

    WorkflowRunner workflowRunner = workflow.buildWorkflow();
    workflowRunner.run();

    List<Tuple> allTuples = HRap.getAllTuples(store1.getTap());
    List<Tuple> allTuples2 = HRap.getAllTuples(store2.getTap());

    assertCollectionEquivalent(TUPLE_SUMS, allTuples);
    assertCollectionEquivalent(TUPLE_SUMS, allTuples2);
  }

  public void testMultiSourcePipes() throws Exception {
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1"));

    buildComplex(output)
        .buildWorkflow()
        .run();

    assertEquals(TUPLE_NAMES, HRap.getAllTuples(output.getTap()));
  }

  public void testBuildStep() throws IOException {
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1"));

    executeWorkflow(buildComplex(output)
        .buildStep("Parent step"));

    assertEquals(TUPLE_NAMES, HRap.getAllTuples(output.getTap()));
  }

  public void testTapVsDs() throws IOException, TException {

    SplitBucketDataStore<DataUnit, _Fields> inputSplit =
        builder().getSplitBucketDataStore("split_store", DataUnit.class);
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("dataunit"));

    DataUnit prevDU = NPDH.getAgeDataUnit((byte) 12);
    DataUnit keepDU = NPDH.getGenderDataUnit(GenderType.MALE);

    ThriftBucketHelper.writeToBucket(inputSplit.getAttributeBucket().getBucket(_Fields.AGE.getThriftFieldId()),
        prevDU);

    ThriftBucketHelper.writeToBucket(inputSplit.getAttributeBucket().getBucket(_Fields.GENDER.getThriftFieldId()),
        keepDU);

    EasyWorkflow2 workflow = new EasyWorkflow2("Test Workflow", getTestRoot() + "/e-workflow");

    Pipe pipe1 = workflow.bindSource("pipe1", inputSplit, inputSplit.getTap(EnumSet.of(_Fields.GENDER)));

    workflow.bindSink("flo0w", pipe1, output);

    WorkflowRunner workflowRunner = workflow.buildWorkflow();
    workflowRunner.run();

    assertCollectionEquivalent(Lists.<Tuple>newArrayList(new Tuple(keepDU)),
        HRap.getAllTuples(output.getTap()));

    assertCollectionEquivalent(Lists.<DataUnit>newArrayList(keepDU, prevDU),
        HRap.<DataUnit>getValuesFromBucket(inputSplit));


  }

  public void testCallback() throws IOException {
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1", "field2"));
    EasyWorkflow2 workflow = new EasyWorkflow2("Test Workflow", getTestRoot() + "/e-workflow");

    Pipe pipe1 = workflow.bindSource("pipe1", input);

    final AtomicBoolean isCompleted = new AtomicBoolean(false);

    workflow.bindSink("output", pipe1, output, new EmptyListener(){
      @Override
      public void onCompleted(Flow flow) {
        isCompleted.set(true);
      }
    });

    WorkflowRunner workflowRunner = workflow.buildWorkflow();
    workflowRunner.run();

    assertTrue(isCompleted.get());
  }


  private EasyWorkflow2 buildComplex(DataStore output) throws IOException {
    EasyWorkflow2 workflow = new EasyWorkflow2("Test Workflow", getTestRoot()+"/e-workflow");

    Pipe pipe = workflow.bindSource("pipe1", input);
    Pipe pipe2 = workflow.bindSource("pipe2", input2);

    pipe2 = new Distinct(pipe2);
    pipe2 = workflow.addCheckpoint(pipe2, "distinct");

    Pipe pipe3 = new BloomJoin(pipe, new Fields("field1"), pipe2, new Fields("field3"));
    pipe3 = new Increment(pipe3, "Test", "Tuples1");
    pipe3 = workflow.addCheckpoint(pipe3, "group");

    pipe3 = new Distinct(pipe3, new Fields("field1"));
    pipe3 = new Retain(pipe3, new Fields("field1"));

    Pipe finalPipe = new Pipe("final", pipe3);

    workflow.bindSink("final-step", finalPipe, output);

    return workflow;
  }
}
