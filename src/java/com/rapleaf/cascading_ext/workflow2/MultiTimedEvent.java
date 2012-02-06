package com.rapleaf.cascading_ext.workflow2;

import java.util.ArrayList;
import java.util.List;

class MultiTimedEvent implements TimedEvent {

  private final String name;
  private final List<TimedEvent> children = new ArrayList<TimedEvent>();

  public MultiTimedEvent(String name) {
    this.name = name;
  }

  @Override
  public String getEventName() {
    return name;
  }

  @Override
  public long getEventStartTime() {
    long result = -1;
    for (TimedEvent event : getEventChildren()) {
      long eventStartTime = event.getEventStartTime();
      if (result == -1 || eventStartTime < result) {
        result = eventStartTime;
      }
    }
    return result;
  }

  @Override
  public long getEventEndTime() {
    long result = -1;
    for (TimedEvent event : getEventChildren()) {
      long eventEndTime = event.getEventEndTime();
      if (result == -1 || eventEndTime > result) {
        result = eventEndTime;
      }
    }
    return result;
  }

  @Override
  public List<TimedEvent> getEventChildren() {
    return children;
  }

  public void addChild(TimedEvent event) {
    children.add(event);
  }
}
