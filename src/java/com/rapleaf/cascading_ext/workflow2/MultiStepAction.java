package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;

import com.liveramp.workflow_core.runner.BaseMultiStepAction;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;

public class MultiStepAction extends BaseMultiStepAction<WorkflowRunner.ExecuteConfig> {

  private final HdfsActionContext context;

  public MultiStepAction(String checkpointToken, String tmpRoot) {
    this(checkpointToken, tmpRoot, null);
  }

  public MultiStepAction(String checkpointToken, String tmpRoot, Collection<Step> steps) {
    super(checkpointToken, steps);
    this.context = new HdfsActionContext(tmpRoot, checkpointToken);
  }

  public final String getTmpRoot() {
    return context.getTmpRoot();
  }

  public DataStoreBuilder builder() {
    return context.getBuilder();
  }

  protected FileSystem getFS() throws IOException {
    return context.getFS();
  }

}
