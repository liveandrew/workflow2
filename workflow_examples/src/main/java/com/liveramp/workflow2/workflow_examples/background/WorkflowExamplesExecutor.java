package com.liveramp.workflow2.workflow_examples.background;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.common.collect.Lists;

import com.liveramp.workflow_state.background_workflow.BackgroundWorkflowExecutor;
import com.liveramp.workflow_state.background_workflow.ErrorReporter;

public class WorkflowExamplesExecutor {

  public static void main(String[] args) throws UnknownHostException {

    new BackgroundWorkflowExecutor(
        Lists.<String>newArrayList(WorkflowExamplesExecutor.class.getName()),
        10000,
        5,
        new ErrorReporter.InMemoryReporter(),
        InetAddress.getLocalHost().getHostName()
    ).start();
  }

}