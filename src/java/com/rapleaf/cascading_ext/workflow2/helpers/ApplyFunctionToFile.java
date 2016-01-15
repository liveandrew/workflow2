package com.rapleaf.cascading_ext.workflow2.helpers;

import java.io.IOException;

import com.google.common.base.Function;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.liveramp.cascading_ext.action.CopyToHdfs;
import com.liveramp.cascading_ext.action.CopyToNfs;
import com.liveramp.cascading_ext.function.DirectFn;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.rapleaf.cascading_ext.filter.SelectAllNotNull;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.FlowBuilder;
import com.rapleaf.cascading_ext.workflow2.ProductionWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.DbPersistenceFactory;

public class ApplyFunctionToFile {

  public static void transformNFS(String workflowName,
                                  String nfsInputPath,
                                  Function<String, String> fn,
                                  String nfsOutputPath,
                                  AlertsHandler alertsHandler,
                                  TeamList teamForPool) throws IOException {

    String tmpRoot = "/tmp/file-transformer/" + workflowName;
    String hdfsInputPath = tmpRoot + "/input";
    String hdfsOutputPath = tmpRoot + "/output";

    Step coptToHdfs = new Step(new CopyToHdfs("copy-to-hdfs", nfsInputPath, hdfsInputPath));

    Step transform = new Step(new TransformAction("transform", hdfsInputPath, hdfsOutputPath, fn), coptToHdfs);

    Step copyToNFS = new Step(new CopyToNfs("copy-to-nfs", hdfsOutputPath, nfsOutputPath), transform);

    WorkflowOptions options = new ProductionWorkflowOptions().setAlertsHandler(alertsHandler).configureTeam(teamForPool, "default");

    WorkflowRunner runner = new WorkflowRunner(workflowName, new DbPersistenceFactory(), options, copyToNFS);
    runner.run();

  }

  private static class TransformAction extends Action {

    private String hdfsInputPath;
    private String hdfsOutputPath;
    private Function<String, String> fn;

    public TransformAction(String checkpointToken, String hdfsInputPath, String hdfsOutputPath, Function<String, String> fn) {
      super(checkpointToken);
      this.hdfsInputPath = hdfsInputPath;
      this.hdfsOutputPath = hdfsOutputPath;
      this.fn = fn;
    }

    @Override
    protected void execute() throws Exception {
      Tap input = new Hfs(new TextLine(), hdfsInputPath);

      Pipe pipe = new Pipe("pipe");
      pipe = new Each(pipe, new Fields("line"), new DirectFn<>(new Fields("output"), fn), Fields.RESULTS);
      pipe = new Each(pipe, new Fields("output"), new SelectAllNotNull());
      Tap output = new Hfs(new TextLine(new Fields("line"), new Fields("output")), hdfsOutputPath, SinkMode.REPLACE);

      FlowBuilder.FlowClosure transformFile = buildFlow().connect("TransformFile", input, output, pipe);
      completeWithProgress(transformFile);
    }
  }
}
