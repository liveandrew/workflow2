package com.liveramp.workflow2.workflow_examples.actions;

import com.liveramp.cascading_ext.resource.Resource;
import com.rapleaf.cascading_ext.workflow2.Action;

public class ResourceWriteAction extends Action {

  public ResourceWriteAction(String checkpointToken,
                             Resource<String> toWrite) {
    super(checkpointToken);


  }

  @Override
  protected void execute() throws Exception {

  }
}
