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
import com.rapleaf.cascading_ext.datastore.SplitBucketDataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.formats.test.TupleDataStoreHelper;
import com.rapleaf.support.test.NPDH;
import com.rapleaf.types.new_person_data.DataUnit;
import com.rapleaf.types.new_person_data.DataUnitValueUnion._Fields;
import com.rapleaf.types.person_data.GenderType;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestCascadingWorkflowBuilder extends CascadingExtTestCase {

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
    TupleDataStoreHelper.writeToStore(input2,TUPLES2);
  }

  @Test
  public void testStraightPipe() throws IOException {
    TupleDataStore store1 = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1", "sum"));
    TupleDataStore store2 = builder().getTupleDataStore(getTestRoot() + "/store2", new Fields("field1", "sum"));

    //  fill in stores to make sure creates() works
    TupleDataStoreHelper.writeToStore(store1, new Tuple("test tuple", 80085l));
    TupleDataStoreHelper.writeToStore(store2, new Tuple("test tuple", 80085l));

    CascadingWorkflowBuilder workflow = new CascadingWorkflowBuilder(getTestRoot() + "/e-workflow");

    Pipe pipe = workflow.bindSource("pipe", input);

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
        Lists.newArrayList(new SinkBinding(pipe3, store1), new SinkBinding(pipe2, store2)));

    executeWorkflow(step);

    List<Tuple> allTuples = HRap.getAllTuples(store1.getTap());
    List<Tuple> allTuples2 = HRap.getAllTuples(store2.getTap());

    assertCollectionEquivalent(TUPLE_SUMS, allTuples);
    assertCollectionEquivalent(TUPLE_SUMS, allTuples2);
  }

  @Test
  public void testMultiSourcePipes() throws Exception {
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1"));

    executeWorkflow(buildComplex(output));

    assertEquals(TUPLE_NAMES, HRap.getAllTuples(output.getTap()));
  }

  @Test
  public void testBuildStep() throws IOException {
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1"));

    executeWorkflow(buildComplex(output));

    assertEquals(TUPLE_NAMES, HRap.getAllTuples(output.getTap()));
  }

  @Test
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

    CascadingWorkflowBuilder workflow = new CascadingWorkflowBuilder(getTestRoot() + "/e-workflow");

    Pipe pipe1 = workflow.bindSource("pipe1", inputSplit, inputSplit.getTap(EnumSet.of(_Fields.GENDER)));

    executeWorkflow(workflow.buildTail(pipe1, output));

    assertCollectionEquivalent(Lists.<Tuple>newArrayList(new Tuple(keepDU)),
        HRap.getAllTuples(output.getTap()));

    assertCollectionEquivalent(Lists.<DataUnit>newArrayList(keepDU, prevDU),
        HRap.<DataUnit>getValuesFromBucket(inputSplit));


  }

  @Test
  public void testCallback() throws IOException {
    TupleDataStore output = builder().getTupleDataStore(getTestRoot() + "/store1", new Fields("field1", "field2"));
    CascadingWorkflowBuilder workflow = new CascadingWorkflowBuilder(getTestRoot() + "/e-workflow");

    Pipe pipe1 = workflow.bindSource("pipe1", input);

    final AtomicBoolean isCompleted = new AtomicBoolean(false);

    executeWorkflow(workflow.buildTail("tail-step", pipe1, output, new EmptyListener() {
      @Override
      public void onCompleted(Flow flow) {
        isCompleted.set(true);
      }
    }));

    assertTrue(isCompleted.get());
  }


  private Step buildComplex(DataStore output) throws IOException {
    CascadingWorkflowBuilder workflow = new CascadingWorkflowBuilder(getTestRoot()+"/e-workflow");

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

    return workflow.buildTail(finalPipe, output);
  }
}
