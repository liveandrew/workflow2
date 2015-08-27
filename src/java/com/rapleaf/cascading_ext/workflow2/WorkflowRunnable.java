package com.rapleaf.cascading_ext.workflow2;

import java.util.Set;

import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;

public interface WorkflowRunnable {

  Set<Step> getSteps() throws Exception;

  WorkflowOptions getOptions() throws Exception;

  void postWorkflow(WorkflowRunner runner) throws Exception;
}
