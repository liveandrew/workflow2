package com.rapleaf.cascading_ext.workflow2;

import java.util.Collections;
import java.util.List;

class FixedTimedEvent implements TimedEvent {

  private final String name;
  private final long startTime;
  private final long endTime;


  public FixedTimedEvent(String name, long startTime, long endTime) {
    this.name = name;
    this.startTime = startTime;
    this.endTime = endTime;
    if (endTime < startTime) {
      throw new RuntimeException("Cannot create " + this.getClass().getSimpleName() +
          " with an end time that is before the start time.");
    }
  }

  @Override
  public String getEventName() {
    return name;
  }

  @Override
  public long getEventStartTime() {
    return startTime;
  }

  @Override
  public long getEventEndTime() {
    return endTime;
  }

  @Override
  public List<TimedEvent> getEventChildren() {
    return Collections.emptyList();
  }
}
