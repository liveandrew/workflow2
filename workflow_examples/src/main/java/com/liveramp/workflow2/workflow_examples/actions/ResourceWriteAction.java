package com.liveramp.workflow2.workflow_examples.actions;

import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.cascading_ext.resource.WriteResource;
import com.rapleaf.cascading_ext.workflow2.Action;

public class ResourceWriteAction extends Action {

  private final WriteResource<String> writeResource;

  public ResourceWriteAction(String checkpointToken,
                             Resource<String> toWrite) {
    super(checkpointToken);
    this.writeResource = creates(toWrite);
  }

  @Override
  protected void execute() throws Exception {
    set(writeResource, "Hello World!");
  }

}
