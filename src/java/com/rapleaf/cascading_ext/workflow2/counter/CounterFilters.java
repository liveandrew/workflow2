package com.rapleaf.cascading_ext.workflow2.counter;

import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;

import cascading.flow.SliceCounters;

public class CounterFilters {

  private static final Multimap<String, String> DEFAULT_AS_MAP = HashMultimap.create();

  static {

    DEFAULT_AS_MAP.put(JobCounter.class.getName(), JobCounter.SLOTS_MILLIS_MAPS.name());
    DEFAULT_AS_MAP.put(JobCounter.class.getName(), JobCounter.SLOTS_MILLIS_REDUCES.name());

    DEFAULT_AS_MAP.put(TaskCounter.class.getName(), TaskCounter.MAP_INPUT_RECORDS.name());
    DEFAULT_AS_MAP.put(TaskCounter.class.getName(), TaskCounter.MAP_OUTPUT_RECORDS.name());
    DEFAULT_AS_MAP.put(TaskCounter.class.getName(), TaskCounter.REDUCE_OUTPUT_RECORDS.name());

    DEFAULT_AS_MAP.put(FileSystemCounter.class.getName(), "HDFS_BYTES_READ");
    DEFAULT_AS_MAP.put(FileSystemCounter.class.getName(), "HDFS_BYTES_WRITTEN");
    DEFAULT_AS_MAP.put(FileSystemCounter.class.getName(), "HDFS_READ_OPS");
    DEFAULT_AS_MAP.put(FileSystemCounter.class.getName(), "HDFS_WRITE_OPS");
    DEFAULT_AS_MAP.put(FileSystemCounter.class.getName(), "HDFS_LARGE_READ_OPS");

    DEFAULT_AS_MAP.put(JobCounter.class.getName(), JobCounter.TOTAL_LAUNCHED_MAPS.name());
    DEFAULT_AS_MAP.put(JobCounter.class.getName(), JobCounter.DATA_LOCAL_MAPS.name());
    DEFAULT_AS_MAP.put(JobCounter.class.getName(), JobCounter.RACK_LOCAL_MAPS.name());

    DEFAULT_AS_MAP.put(JobCounter.class.getName(), JobCounter.TOTAL_LAUNCHED_REDUCES.name());


    DEFAULT_AS_MAP.put(JobCounter.class.getName(), JobCounter.VCORES_MILLIS_MAPS.name());
    DEFAULT_AS_MAP.put(JobCounter.class.getName(), JobCounter.VCORES_MILLIS_REDUCES.name());

    DEFAULT_AS_MAP.put(JobCounter.class.getName(), JobCounter.MB_MILLIS_MAPS.name());
    DEFAULT_AS_MAP.put(JobCounter.class.getName(), JobCounter.MB_MILLIS_REDUCES.name());

    DEFAULT_AS_MAP.put(SliceCounters.class.getName(), SliceCounters.Process_Begin_Time.name());
    DEFAULT_AS_MAP.put(SliceCounters.class.getName(), SliceCounters.Process_End_Time.name());

    DEFAULT_AS_MAP.put(TaskCounter.class.getName(), TaskCounter.PHYSICAL_MEMORY_BYTES.name());
    DEFAULT_AS_MAP.put(TaskCounter.class.getName(), TaskCounter.GC_TIME_MILLIS.name());

  }

  public static CounterFilter all(){
    return new AllFilter();
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

  private static class AllFilter implements CounterFilter {
    @Override
    public boolean isRecord(String group, String name) {
      return true;
    }
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
