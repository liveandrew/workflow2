package com.liveramp.workflow_ui.servlet;

public class ClusterConstants {

  public static final int DEFAULT_PORT = 8080;
  public static final String MR2_GROUP = "org.apache.hadoop.mapreduce.JobCounter";
  public static final String MB_MAP = "MB_MILLIS_MAPS";
  public static final String MB_RED = "MB_MILLIS_REDUCES";
  public static final String VCORE_MAP = "VCORES_MILLIS_MAPS";
  public static final String VCORE_RED = "VCORES_MILLIS_REDUCES";

  //  these numbers are calculated from this doc: https://docs.google.com/spreadsheets/d/1CcyqdNnzrZM2NK6lc4tiorPgQxCphZE6LBrXDi1pk8o/edit#gid=0
  //  since it's $/ms and mb, ideally this doesn't change dramatically as we expand, so it's ok to only update every year or two
  public static final double CPU_MS_COST = 3.89 * Math.pow(10, -9);
  public static final double MB_MS_COST = 9.73 * Math.pow(10, -13);

}
