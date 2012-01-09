package com.rapleaf.support.workflow2;

public enum StepStatus {
  /** Haven't been started yet. Waiting for dependencies to be completed. */
  WAITING,
  /** The component has started and not yet completed. */
  RUNNING,
  /** The component started and finished successfully. */
  COMPLETED,
  /** The component was started, but failed. */
  FAILED,
  /**
   * The component was started and succeded in a previous run of the
   * workflow, and this time a checkpoint was found.
   */
  SKIPPED;
  
  /**
   * Returns true if the component was started at any point.
   * 
   * @return
   */
  public boolean wasStarted() {
    return this != WAITING;
  }
}
