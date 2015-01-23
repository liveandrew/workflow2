package com.rapleaf.cascading_ext.workflow2.state;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.Sets;

public enum StepStatus {
  WAITING,
  RUNNING,
  COMPLETED,
  FAILED,
  SKIPPED;

  public static final Set<StepStatus> NON_BLOCKING = EnumSet.of(
      COMPLETED, SKIPPED
  );

  public static final Set<Integer> NON_BLOCKING_IDS = Sets.newHashSet();
  static {
    for (StepStatus stepStatus : NON_BLOCKING) {
      NON_BLOCKING_IDS.add(stepStatus.ordinal());
    }
  }
}
