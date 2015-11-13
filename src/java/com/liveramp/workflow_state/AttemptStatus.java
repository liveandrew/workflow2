package com.liveramp.workflow_state;

import java.util.Set;

import com.google.common.collect.Sets;

public enum AttemptStatus {
  RUNNING,
  FAIL_PENDING,
  SHUTDOWN_PENDING,
  FAILED,
  FINISHED,
  SHUTDOWN,
  INITIALIZING;

  private static final Set<AttemptStatus> LIVE = Sets.newHashSet(
      AttemptStatus.RUNNING,
      AttemptStatus.FAIL_PENDING,
      AttemptStatus.SHUTDOWN_PENDING,
      AttemptStatus.INITIALIZING
  );

  public static final Set<Integer> LIVE_STATUSES = Sets.newHashSet();
  static{
    for (AttemptStatus status : LIVE) {
      LIVE_STATUSES.add(status.ordinal());
    }

  }

  public static AttemptStatus findByValue(int val){
    return values()[val];
  }

}
