package com.liveramp.workflow_core;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import com.liveramp.workflow.types.StepStatus;
import com.liveramp.workflow.types.WorkflowAttemptStatus;

public class WorkflowEnums {

  private static final Set<WorkflowAttemptStatus> LIVE_ATTEMPT_STATUS = Sets.newHashSet(
      WorkflowAttemptStatus.RUNNING,
      WorkflowAttemptStatus.FAIL_PENDING,
      WorkflowAttemptStatus.SHUTDOWN_PENDING,
      WorkflowAttemptStatus.INITIALIZING
  );

  public static final Set<Integer> LIVE_ATTEMPT_STATUSES = Sets.newHashSet();

  static {
    for (WorkflowAttemptStatus status : LIVE_ATTEMPT_STATUS) {
      LIVE_ATTEMPT_STATUSES.add(status.ordinal());
    }

  }

  public static final Set<StepStatus> COMPLETED_STATUSES = EnumSet.of(
      StepStatus.COMPLETED, StepStatus.MANUALLY_COMPLETED
  );

  public static final Set<StepStatus> NON_BLOCKING_STEP_STATUSES = EnumSet.of(
      StepStatus.COMPLETED, StepStatus.SKIPPED, StepStatus.MANUALLY_COMPLETED
  );

  public static final Set<Integer> NON_BLOCKING_STEP_STATUS_IDS = Sets.newHashSet();

  static {
    for (StepStatus stepStatus : NON_BLOCKING_STEP_STATUSES) {
      NON_BLOCKING_STEP_STATUS_IDS.add(stepStatus.ordinal());
    }
  }

  public final static Multimap<StepStatus, StepStatus> VALID_STEP_STATUS_TRANSITIONS = HashMultimap.create();

  static {

    VALID_STEP_STATUS_TRANSITIONS.putAll(StepStatus.WAITING,
        Lists.newArrayList(StepStatus.RUNNING, StepStatus.MANUALLY_COMPLETED));

    VALID_STEP_STATUS_TRANSITIONS.putAll(StepStatus.RUNNING,
        Lists.newArrayList(StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.MANUALLY_COMPLETED));

    //  failed b/c of shutdown hook ordering
    VALID_STEP_STATUS_TRANSITIONS.putAll(StepStatus.COMPLETED,
        Lists.newArrayList(StepStatus.REVERTED, StepStatus.FAILED));

    VALID_STEP_STATUS_TRANSITIONS.putAll(StepStatus.REVERTED,
        Lists.newArrayList(StepStatus.MANUALLY_COMPLETED));

    VALID_STEP_STATUS_TRANSITIONS.putAll(StepStatus.FAILED,
        Lists.newArrayList(StepStatus.MANUALLY_COMPLETED));

    VALID_STEP_STATUS_TRANSITIONS.putAll(StepStatus.MANUALLY_COMPLETED,
        Lists.newArrayList(StepStatus.REVERTED));
  }

}
