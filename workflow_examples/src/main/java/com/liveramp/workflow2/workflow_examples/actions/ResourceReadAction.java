package com.liveramp.workflow2.workflow_examples.actions;

import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.Resource;
import com.rapleaf.cascading_ext.workflow2.Action;

public class ResourceReadAction extends Action {

  private final ReadResource<String> toRead;

  public ResourceReadAction(String checkpointToken, Resource<String> toRead) {
    super(checkpointToken);
    this.toRead = readsFrom(toRead);
  }

  @Override
  protected void execute() throws Exception {
    setStatusMessage("Found resource value: "+ get(toRead));
  }
}
