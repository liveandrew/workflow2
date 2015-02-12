package com.rapleaf.cascading_ext.workflow2.counter;

import java.util.List;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;

public class CounterFilters {

  private static final List<Enum<?>> DEFAULT_CAPTURED_STATS = Lists.newArrayList(
      JobCounter.SLOTS_MILLIS_MAPS,
      JobCounter.SLOTS_MILLIS_REDUCES,

      TaskCounter.MAP_INPUT_RECORDS,
      TaskCounter.MAP_OUTPUT_RECORDS,
      TaskCounter.REDUCE_OUTPUT_RECORDS,

      FileSystemCounter.BYTES_READ,
      FileSystemCounter.BYTES_WRITTEN
  );

  private static final Multimap<String, String> DEFAULT_AS_MAP = HashMultimap.create();

  static {
    for (Enum<?> stat : DEFAULT_CAPTURED_STATS) {
      DEFAULT_AS_MAP.put(stat.getClass().getName(), stat.name());
    }
  }

  public static CounterFilter defaultCounters(){
    return new DefaultFilter();
  }

  public static CounterFilter userGroups(final Set<String> userGroups){
    return new UserFilter(userGroups);
  }

  private static boolean isDefault(String group, String name){
    return DEFAULT_AS_MAP.get(group).contains(name);
  }

  private static class DefaultFilter implements CounterFilter {
    @Override
    public boolean isRecord(String group, String name) {
      return isDefault(group, name);
    }
  }

  private static class UserFilter implements CounterFilter {
    private final Set<String> userGroups;

    public UserFilter(Set<String> userGroups) {
      this.userGroups = userGroups;
    }

    @Override
    public boolean isRecord(String group, String name) {
      return isDefault(group, name) || userGroups.contains(group);
    }
  }

}
