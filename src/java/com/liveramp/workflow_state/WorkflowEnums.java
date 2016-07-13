package com.liveramp.workflow_state;

import java.util.Set;

import com.google.common.collect.Sets;

import com.rapleaf.types.person_data.WorkflowAttemptStatus;

public class WorkflowEnums {

  private static final Set<WorkflowAttemptStatus> LIVE_ATTEMPT_STATUS = Sets.newHashSet(
      WorkflowAttemptStatus.RUNNING,
      WorkflowAttemptStatus.FAIL_PENDING,
      WorkflowAttemptStatus.SHUTDOWN_PENDING,
      WorkflowAttemptStatus.INITIALIZING
  );

  public static final Set<Integer> LIVE_ATTEMPT_STATUSES = Sets.newHashSet();
  static{
    for (WorkflowAttemptStatus status : LIVE_ATTEMPT_STATUS) {
      LIVE_ATTEMPT_STATUSES.add(status.ordinal());
    }

  }

}
