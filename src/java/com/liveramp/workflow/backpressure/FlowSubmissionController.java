package com.liveramp.workflow.backpressure;

import org.apache.hadoop.conf.Configuration;

import cascading.flow.Flow;

public interface FlowSubmissionController {

  void blockUntilSubmissionAllowed(Configuration flowConfig);

  public static class SubmitImmediately implements FlowSubmissionController {

    @Override
    public void blockUntilSubmissionAllowed(Configuration flowConfig) {
      //Don't block
    }
  }

}
