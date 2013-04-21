package com.rapleaf.cascading_ext.workflow2.state;

import com.liveramp.workflow_service.generated.StepStatus;
import com.rapleaf.cascading_ext.workflow2.Step;

import java.io.IOException;

public interface WorkflowStatePersistence {
  public StepStatus getStatus(Step step);
  public void updateStatus(Step step, StepStatus status) throws IOException;
  public void prepare() throws IOException;
  public void complete() throws IOException;
}
