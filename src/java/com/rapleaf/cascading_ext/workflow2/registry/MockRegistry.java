package com.rapleaf.cascading_ext.workflow2.registry;

import com.liveramp.types.workflow.LiveWorkflowMeta;

public class MockRegistry implements WorkflowRegistry{

  @Override
  public void register(String uuid, LiveWorkflowMeta meta) {
    // no op
  }

  @Override
  public void deregister() {
    // no op
  }

}
