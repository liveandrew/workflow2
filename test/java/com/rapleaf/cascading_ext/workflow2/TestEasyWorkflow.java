package com.rapleaf.cascading_ext.workflow2;

import cascading.operation.Debug;
import cascading.operation.Insert;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.assembly.Increment;
import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.assembly.Distinct;
import com.rapleaf.cascading_ext.assembly.FastSum;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.formats.test.TupleDataStoreHelper;

import java.io.IOException;

public class TestEasyWorkflow extends CascadingExtTestCase {

  private TupleDataStore input;
  private TupleDataStore input2;
  private TupleDataStore output;
  private TupleDataStore output2;

  public void setUp() throws Exception {
    super.setUp();
    DataStoreBuilder builder = new DataStoreBuilder(getTestRoot() + "/insAndOuts");
    input = builder.getTupleDataStore("input", new Fields("field1", "field2"));
    input2 = builder.getTupleDataStore("input2", new Fields("field3"));
    output = builder.getTupleDataStore("output", new Fields("field1", "sum"));
    output2 = builder.getTupleDataStore("output2", new Fields("field1", "field2", "field3"));

    TupleDataStoreHelper.writeToStore(
        input,
        new Tuple("red", 3),
        new Tuple("red", 5),
        new Tuple("green", 8),
        new Tuple("blue", 7),
        new Tuple("blue", 3),
        new Tuple("blue", 5)
    );

    TupleDataStoreHelper.writeToStore(
        input2,
        new Tuple("red"),
        new Tuple("red"),
        new Tuple("green"),
        new Tuple("blue"),
        new Tuple("blue"),
        new Tuple("blue")
    );
  }

  public void testStraightPipe() throws IOException {

    String workingDir = getTestRoot() + "/e-workflow";
    EasyWorkflow workflow = EasyWorkflow.create("Test Workflow", workingDir);

    workflow.addInput(input);
    workflow.addOutput(output);
    workflow.addSourceTap("pipe", input.getTap());
    workflow.addSinkTap("final", output.getTap());

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, new Insert(new Fields("field3"), new Integer(3)), Fields.ALL);
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

    WorkflowRunner runner = workflow.completeAsWorkflow("final", pipe2, pipe3);
    runner.run();
  }

  public void testMultiSourcePipes() throws Exception {
    String workingDir = getTestRoot() + "/e-workflow";
    EasyWorkflow workflow = EasyWorkflow.create("Test Workflow", workingDir);

    workflow.addInput(input);
    workflow.addInput(input2);
    workflow.addOutput(output2);
    workflow.addSourceTap("pipe", input.getTap());
    workflow.addSourceTap("pipe2", input2.getTap());
    workflow.addSinkTap("final", output2.getTap());

    Pipe pipe = new Pipe("pipe");

    Pipe pipe2 = new Pipe("pipe2");
    pipe2 = new Distinct(pipe2);
    pipe2 = workflow.addCheckpoint(pipe2, "distinct");

    Pipe pipe3 = new CoGroup(pipe, new Fields("field1"), pipe2, new Fields("field3"));
    pipe3 = new Increment(pipe3, "Test", "Tuples1");
    pipe3 = workflow.addCheckpoint(pipe3, "group");

    pipe3 = new Distinct(pipe3, new Fields("field1"));

    Pipe finalPipe = new Pipe("final", pipe3);

    workflow.completeAsWorkflow("final", finalPipe).run();
  }
}
