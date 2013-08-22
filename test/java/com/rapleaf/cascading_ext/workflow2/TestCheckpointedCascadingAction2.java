package com.rapleaf.cascading_ext.workflow2;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.formats.test.TupleDataStoreHelper;

import java.io.IOException;
import java.util.List;

public class TestCheckpointedCascadingAction2 extends CascadingExtTestCase {

  public static class SimpleExampleCheckpointedAction extends CheckpointedCascadingAction2 {

    public SimpleExampleCheckpointedAction(String checkpointToken, String tmpRoot,
                                           DataStore input, DataStore output) throws IOException {
      super(checkpointToken, tmpRoot, Maps.newHashMap());

      Pipe source = bindSource("source", input);
      source = addCheckpoint(source, "intermediate");
      bindSink("step", source, output);

      complete();
    }
  }

  public void testIt() throws IOException {

    TupleDataStore input = builder().getTupleDataStore("input", new Fields("field"));
    TupleDataStore output = builder().getTupleDataStore("input", new Fields("field"));

    List<Tuple> data = Lists.<Tuple>newArrayList(new Tuple("data"));
    TupleDataStoreHelper.writeToStore(input, data);

    executeWorkflow(new SimpleExampleCheckpointedAction("token", getTestRoot() + "/tmp", input, output));

    assertCollectionEquivalent(data, HRap.getAllTuples(output.getTap()));
  }
}
