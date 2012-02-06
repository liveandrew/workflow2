package com.rapleaf.cascading_ext.workflow2;

import java.util.ArrayList;
import java.util.List;

class EventTimer implements TimedEvent {

  private final String name;
  private long startTimeMs = -1;
  private long endTimeMs = -1;
  private final List<TimedEvent> children = new ArrayList<TimedEvent>();

  public EventTimer(String name) {
    this.name = name;
  }

  public void start() {
    if (isStarted()) {
      throw new IllegalStateException("Cannot start timer '" + getEventName() + "' that has already been started.");
    }
    this.startTimeMs = System.currentTimeMillis();

  }

  public void stop() {
    if (!isStarted()) {
      throw new IllegalStateException("Cannot stop timer '" + getEventName() + "' that has not yet been started.");
    }
    if (isStopped()) {
      throw new IllegalStateException("Cannot stop timer '" + getEventName() + "' that has already been stopped.");
    }
    endTimeMs = System.currentTimeMillis();
  }

  public boolean isStopped() {
    return endTimeMs != -1;
  }

  public boolean isStarted() {
    return startTimeMs != -1;
  }

  @Override
  public String getEventName() {
    return name;
  }

  @Override
  public long getEventStartTime() {
    return startTimeMs;
  }

  @Override
  public long getEventEndTime() {
    return endTimeMs;
  }

  @Override
  public List<TimedEvent> getEventChildren() {
    return children;
  }

  public void addChild(TimedEvent event) {
    children.add(event);
  }
}
