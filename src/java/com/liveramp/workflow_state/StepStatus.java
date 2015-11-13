package com.liveramp.workflow_state;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public enum StepStatus {
  WAITING,
  RUNNING,
  COMPLETED,
  FAILED,
  SKIPPED,
  REVERTED;

  public static final Set<StepStatus> NON_BLOCKING = EnumSet.of(
      COMPLETED, SKIPPED
  );

  public static final Set<Integer> NON_BLOCKING_IDS = Sets.newHashSet();
  static {
    for (StepStatus stepStatus : NON_BLOCKING) {
      NON_BLOCKING_IDS.add(stepStatus.ordinal());
    }
  }

  public final static Multimap<StepStatus, StepStatus> VALID_TRANSITIONS = HashMultimap.create();

  static{

    VALID_TRANSITIONS.putAll(StepStatus.WAITING,
        Lists.newArrayList(RUNNING));

    VALID_TRANSITIONS.putAll(StepStatus.RUNNING,
        Lists.newArrayList(StepStatus.COMPLETED, StepStatus.FAILED));

    //  failed b/c of shutdown hook ordering
    VALID_TRANSITIONS.putAll(StepStatus.COMPLETED,
        Lists.newArrayList(StepStatus.REVERTED, StepStatus.FAILED));

  }

  public static StepStatus findByValue(int val){
    return values()[val];
  }

}
