package com.rapleaf.cascading_ext.test;

import com.rapleaf.cascading_ext.workflow2.Action;
import com.liveramp.workflow_core.OldResource;

public class CreateResourceAction<T> extends Action {
  private final OldResource<T> resource;
  private final T value;

  public CreateResourceAction(String checkpointToken, String tmpRoot,
                              OldResource<T> resource,
                              T value) {
    super(checkpointToken, tmpRoot);
    this.resource = resource;
    this.value = value;

    creates(resource);
  }

  @Override
  protected void execute() throws Exception {
    set(resource, value);
  }
}
