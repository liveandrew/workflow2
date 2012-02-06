package com.rapleaf.cascading_ext.workflow2;

import java.util.List;

interface TimedEvent {

  public String getEventName();

  public long getEventStartTime();

  public long getEventEndTime();

  public List<TimedEvent> getEventChildren();
}
