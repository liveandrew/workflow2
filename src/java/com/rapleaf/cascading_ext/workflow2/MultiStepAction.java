package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.workflow_core.runner.BaseMultiStepAction;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;

public class MultiStepAction extends BaseMultiStepAction<WorkflowRunner.ExecuteConfig> {

  private final String tmpRoot;
  private final DataStoreBuilder builder;

  private final ResourceFactory resourceFactory;

  private FileSystem fs;

  public MultiStepAction(String checkpointToken, String tmpRoot) {
    this(checkpointToken, tmpRoot, null);
  }

  public MultiStepAction(String checkpointToken, String tmpRoot, Collection<Step> steps) {
    super(checkpointToken, steps);

    this.resourceFactory = new ResourceFactory(getActionId());

    if (tmpRoot != null) {
      this.tmpRoot = tmpRoot + "/" + checkpointToken + "-tmp-stores";
      this.builder = new DataStoreBuilder(getTmpRoot());
    } else {
      this.tmpRoot = null;
      this.builder = null;
    }

  }

  public final String getTmpRoot() {
    if (tmpRoot == null) {
      throw new RuntimeException("Temp root not set for action " + this.toString());
    }
    return tmpRoot;
  }

  public DataStoreBuilder builder() {
    return builder;
  }

  protected FileSystem getFS() throws IOException {
    if (fs == null) {
      fs = FileSystemHelper.getFS();
    }

    return fs;
  }


  protected <T> OldResource<T> resource(String name) {
    return resourceFactory().makeResource(name);
  }

  public ResourceFactory resourceFactory() {
    return resourceFactory;
  }


}
