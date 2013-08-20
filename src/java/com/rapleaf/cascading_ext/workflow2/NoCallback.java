package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.Flow;

import java.io.IOException;

public class NoCallback implements FlowCompletedCallback {
  @Override
  public void flowCompleted(Flow flow) throws IOException {
    // no op
  }
}
