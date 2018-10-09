package com.liveramp.cascading_ext.action;

import java.util.Set;

import com.rapleaf.cascading_ext.state.Container;
import com.rapleaf.cascading_ext.workflow2.Action;

public class DeleteContainers extends Action {

  private final Set<Container<?>>  containers;

  public DeleteContainers(String checkpointToken, Set<Container<?>> containers) {
    super(checkpointToken);
    this.containers = containers;
  }

  @Override
  protected void execute() throws Exception {
    for (Container<?> container : containers) {
      container.delete();
    }
  }
}
