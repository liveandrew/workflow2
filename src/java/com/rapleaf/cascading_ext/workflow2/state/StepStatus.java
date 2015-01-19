package com.rapleaf.cascading_ext.workflow2.state;

import java.util.EnumSet;
import java.util.Set;

public enum StepStatus {
  WAITING,
  RUNNING,
  COMPLETED,
  FAILED,
  SKIPPED;
  public static final Set<StepStatus> NON_BLOCKING = EnumSet.of(
      COMPLETED, SKIPPED
  );
}
