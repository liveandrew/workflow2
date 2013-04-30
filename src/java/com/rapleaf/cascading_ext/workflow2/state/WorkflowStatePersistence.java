package com.rapleaf.cascading_ext.workflow2.state;

import com.liveramp.workflow_service.generated.ExecuteStatus;
import com.liveramp.workflow_service.generated.StepExecuteStatus;
import com.liveramp.workflow_service.generated.WorkflowDefinition;
import com.rapleaf.cascading_ext.workflow2.Step;

import java.io.IOException;
import java.util.Map;

public interface WorkflowStatePersistence {
  public StepExecuteStatus getStatus(Step step);
  public ExecuteStatus getFlowStatus();
  public void updateStatus(Step step, StepExecuteStatus status) throws IOException;
  public void prepare(WorkflowDefinition plannedFlow) throws IOException;
  public Map<String, StepExecuteStatus> getAllStepStatuses();
  public void setStatus(ExecuteStatus status);
}
