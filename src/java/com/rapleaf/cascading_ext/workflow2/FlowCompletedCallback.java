package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.Flow;
import java.io.IOException;

public interface FlowCompletedCallback {
  public void flowCompleted(Flow flow) throws IOException;
}
