package com.rapleaf.cascading_ext.workflow2;

import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.assembly.Increment;
import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.assembly.FastSum;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.formats.test.TupleDataStoreHelper;

import java.io.IOException;

public class TestEasyWorkflow extends CascadingExtTestCase {

  private TupleDataStore input;
  private TupleDataStore output;

  public void setUp() throws Exception {
    super.setUp();
    DataStoreBuilder builder = new DataStoreBuilder(getTestRoot() + "/insAndOuts");
    input = builder.getTupleDataStore("input", new Fields("field1", "field2"));
    output = builder.getTupleDataStore("input", new Fields("field1", "sum"));

    TupleDataStoreHelper.writeToStore(
        input,
        new Tuple("red", 3),
        new Tuple("red", 5),
        new Tuple("green", 8),
        new Tuple("blue", 7),
        new Tuple("blue", 3),
        new Tuple("blue", 5)
    );


  }


  public void testIt() throws IOException {

    String workingDir = getTestRoot() + "/e-workflow";
    EasyWorkflow workflow = new EasyWorkflow(workingDir);

    workflow.addInput(input);
    workflow.addOutput(output);
    workflow.addSourceTap("pipe", input.getTap());
    workflow.addSinkTap("final", output.getTap());


    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, new Insert(new Fields("field3"), new Integer(3)), Fields.ALL);
    pipe = new Increment(pipe, "Test", "Tuples");
    pipe = workflow.addCheckpoint(pipe, "pipe2", new Fields("field1", "field2", "field3"), "check1");

    pipe = new Each(pipe, new Insert(new Fields("field4"), "four"), Fields.ALL);
    pipe = new Increment(pipe, "Test", "Tuples2");
    pipe = workflow.addCheckpoint(pipe, "pipe3", new Fields("field1", "field2", "field3", "field4"), "check2");

    pipe = new Retain(pipe, new Fields("field1", "field2"));
    pipe = new Increment(pipe, "Test", "Tuples3");
    pipe = workflow.addCheckpoint(pipe, "final", new Fields("field1", "field2"), "check3");

    pipe = new FastSum(pipe, new Fields("field1"), new Fields("field2"));
    pipe = new Increment(pipe, "Test", "Tuples4");

    workflow.addTail(pipe);
    Step step = workflow.complete(pipe, "final");

    WorkflowRunner runner = new WorkflowRunner("testWF", workingDir + "/checkpoints", 1, 0, step);
    runner.run();


  }


}
