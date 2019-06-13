package com.liveramp.workflow2.workflow_examples;

import java.io.IOException;
import java.util.Collections;

import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;

import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.workflow2.workflow_examples.actions.SimpleCascadingAction;
import com.liveramp.workflow2.workflow_examples.actions.SimpleCascadingFlow;
import com.liveramp.workflow2.workflow_examples.actions.SimpleWriteFile;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStoreImpl;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunners;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;

public class CascadingFlowWithProperties {

  public static void main(String[] args) throws IOException {

    TupleDataStore store = new TupleDataStoreImpl("Temp Store",
        "/tmp/simple-cascading-flow/", "data",
        new Fields("line"),
        TextLine.class
    );

    WorkflowRunners.dbRun(
        CascadingFlowWithProperties.class.getName(),
        HadoopWorkflowOptions.test()
            .addWorkflowProperties(Collections.singletonMap("mapreduce.job.queuename", "root.default.custom"))
            .addWorkflowProperties(Collections.singletonMap("mapreduce.task.io.sort.mb", 300)),
        dbHadoopWorkflow -> {

          Step writeFile = new Step(new SimpleWriteFile("write-data", store));

          Step readFile = new Step(new SimpleCascadingAction("read-data", store), writeFile);

          return Collections.singleton(readFile);
        }
    );

  }

}
