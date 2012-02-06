package com.rapleaf.cascading_ext.workflow2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TimedEventHelper {

  public static long getEventDuration(TimedEvent event) {
    long endTime = event.getEventEndTime();
    long startTime = event.getEventStartTime();
    if (startTime < 0) {
      throw new IllegalStateException("Cannot compute event duration of '"
          + event.getEventName() + "' when the event's start time is unspecified.");
    }
    if (endTime < 0) {
      // Using current time instead (event is not finished)
      return System.currentTimeMillis();
    }
    if (endTime < startTime) {
      throw new IllegalStateException("Cannot compute event duration of '"
          + event.getEventName() + "' when the event's end time precedes its start time.");
    }
    return endTime - startTime;
  }

  public static boolean hasStarted(TimedEvent event) {
    return event.getEventStartTime() >= 0;
  }

  public static boolean hasEnded(TimedEvent event) {
    return event.getEventEndTime() >= 0;
  }

  private static interface Formatter {

    public void header(TimedEvent event, StringBuilder stringBuilder, int depth);

    public void beforeName(TimedEvent event, StringBuilder stringBuilder, int depth);

    public String escapeName(String name);

    public void afterName(TimedEvent event, StringBuilder stringBuilder, int depth);

    public void duration(TimedEvent event, StringBuilder stringBuilder, long parentDuration, int depth);

    public void beforeChildren(TimedEvent event, StringBuilder stringBuilder, int depth);

    public void betweenChildren(TimedEvent event, StringBuilder stringBuilder, int depth);

    public void afterChildren(TimedEvent event, StringBuilder stringBuilder, int depth);

    public void footer(TimedEvent event, StringBuilder stringBuilder, int depth);
  }

  private static class TextSummaryFormatter implements Formatter {

    @Override
    public void header(TimedEvent event, StringBuilder stringBuilder, int depth) {
      for (int i = 0; i < depth; ++i) {
        stringBuilder.append("  ");
      }
    }

    @Override
    public void beforeName(TimedEvent event, StringBuilder stringBuilder, int depth) {
    }

    @Override
    public String escapeName(String name) {
      return name;
    }

    @Override
    public void afterName(TimedEvent event, StringBuilder stringBuilder, int depth) {
      stringBuilder.append(": ");
    }

    @Override
    public void duration(TimedEvent event, StringBuilder stringBuilder, long parentDuration, int depth) {
      if (hasStarted(event)) {
        long eventDuration = TimedEventHelper.getEventDuration(event);
        stringBuilder.append((eventDuration / 100) / 10.0);
        stringBuilder.append(" s (");
        if (parentDuration > 0) {
          double eventDurationPercentage = TimedEventHelper.getEventDurationPercentage(parentDuration, eventDuration);
          stringBuilder.append(((long) (10 * eventDurationPercentage)) / (double) 10);
          stringBuilder.append("%, ");
        }
      }
      stringBuilder.append("start: ");
      stringBuilder.append(event.getEventStartTime());
      stringBuilder.append(", end: ");
      stringBuilder.append(event.getEventEndTime());
      stringBuilder.append(")");
    }

    @Override
    public void beforeChildren(TimedEvent event, StringBuilder stringBuilder, int depth) {
      stringBuilder.append("\n");
    }

    @Override
    public void betweenChildren(TimedEvent event, StringBuilder stringBuilder, int depth) {
    }

    @Override
    public void afterChildren(TimedEvent event, StringBuilder stringBuilder, int depth) {
    }

    @Override
    public void footer(TimedEvent event, StringBuilder stringBuilder, int depth) {
      if (event.getEventChildren().size() == 0) {
        stringBuilder.append("\n");
      }
    }
  }

  private static double getEventDurationPercentage(long parentDuration, long eventDuration) {
    return ((double) eventDuration / (double) parentDuration) * 100.0;
  }

  private static class JSONFormatter implements Formatter {

    @Override
    public void header(TimedEvent event, StringBuilder stringBuilder, int depth) {
      stringBuilder.append("{");
    }

    @Override
    public void beforeName(TimedEvent event, StringBuilder stringBuilder, int depth) {
      stringBuilder.append("name: \"");
    }

    @Override
    public String escapeName(String name) {
      return name.replaceAll("\"", "").replaceAll("\\\\", "");
    }

    @Override
    public void afterName(TimedEvent event, StringBuilder stringBuilder, int depth) {
      stringBuilder.append("\", ");
    }

    @Override
    public void duration(TimedEvent event, StringBuilder stringBuilder, long parentDuration, int depth) {
      if (hasStarted(event)) {
        long eventDuration = TimedEventHelper.getEventDuration(event);
        stringBuilder.append("duration: ");
        stringBuilder.append((eventDuration / 100) / 10.0);
        if (parentDuration > 0) {
          double eventDurationPercentage = TimedEventHelper.getEventDurationPercentage(parentDuration, eventDuration);
          stringBuilder.append(", duration_percentage: ");
          stringBuilder.append(eventDurationPercentage);
        }
      }
      stringBuilder.append(", start_time: ");
      stringBuilder.append(event.getEventStartTime());
      stringBuilder.append(", end_time: ");
      stringBuilder.append(event.getEventEndTime());
    }

    @Override
    public void beforeChildren(TimedEvent event, StringBuilder stringBuilder, int depth) {
      stringBuilder.append(", children: [");
    }

    @Override
    public void betweenChildren(TimedEvent event, StringBuilder stringBuilder, int depth) {
      stringBuilder.append(", ");
    }

    @Override
    public void afterChildren(TimedEvent event, StringBuilder stringBuilder, int depth) {
      stringBuilder.append("]");
    }

    @Override
    public void footer(TimedEvent event, StringBuilder stringBuilder, int depth) {
      stringBuilder.append("}");
    }
  }

  static public String toJSON(TimedEvent event) {
    return format(event, new JSONFormatter());
  }

  static public String toTextSummary(TimedEvent event) {
    return format(event, new TextSummaryFormatter());
  }

  static private String format(TimedEvent event, Formatter formatter) {
    StringBuilder stringBuilder = new StringBuilder();
    format(event, stringBuilder, formatter, -1, 0);
    return stringBuilder.toString();
  }

  static private void format(TimedEvent event,
                             StringBuilder stringBuilder,
                             Formatter formatter,
                             long parentDuration,
                             int depth) {
    formatter.header(event, stringBuilder, depth);
    formatter.beforeName(event, stringBuilder, depth);
    stringBuilder.append(formatter.escapeName(event.getEventName()));
    formatter.afterName(event, stringBuilder, depth);
    formatter.duration(event, stringBuilder, parentDuration, depth);
    // Copy children list and sort it by start time
    List<TimedEvent> children = new ArrayList<TimedEvent>(event.getEventChildren());
    // Display children if needed
    if (children.size() > 0) {
      Collections.sort(children, new EventStartTimeComparator());
      long eventDuration = -1;
      if (hasStarted(event)) {
        eventDuration = TimedEventHelper.getEventDuration(event);
      }
      formatter.beforeChildren(event, stringBuilder, depth);
      int i = 0;
      for (TimedEvent child : children) {
        format(child, stringBuilder, formatter, eventDuration, depth + 1);
        ++i;
        if (i != children.size()) {
          formatter.betweenChildren(event, stringBuilder, depth);
        }
      }
      formatter.afterChildren(event, stringBuilder, depth);
    }
    formatter.footer(event, stringBuilder, depth);
  }

  private static class EventStartTimeComparator implements Comparator<TimedEvent> {

    @Override
    public int compare(TimedEvent a, TimedEvent b) {
      long startTimeA = a.getEventStartTime();
      long startTimeB = b.getEventStartTime();
      if (startTimeA < startTimeB) {
        return -1;
      } else if (startTimeA > startTimeB) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
