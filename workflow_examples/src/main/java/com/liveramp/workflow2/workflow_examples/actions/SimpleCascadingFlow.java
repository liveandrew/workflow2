package com.liveramp.workflow2.workflow_examples.actions;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;

import com.liveramp.workflow.state.DbHadoopWorkflow;
import com.liveramp.workflow_db_state.InitializedDbPersistence;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStoreImpl;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunners;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;

public class SimpleCascadingFlow {

  public static void main(String[] args) throws IOException {

    TupleDataStore store = new TupleDataStoreImpl("Temp Store",
        "/tmp/simple-cascading-flow/", "data",
        new Fields("line"),
        TextLine.class
    );

    WorkflowRunners.dbRun(
        SimpleCascadingFlow.class.getName(),
        HadoopWorkflowOptions.test(),
        dbHadoopWorkflow -> {

          Step writeFile = new Step(new SimpleWriteFile("write-data", store));

          Step readFile = new Step(new SimpleCascadingAction("read-data", store), writeFile);

          return Collections.singleton(readFile);
        }
    );

  }

}
