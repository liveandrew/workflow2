package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public interface RunnableJob {

  public JobConf configure() throws IOException;

  public void addProperties(Map<Object, Object> userProperties);

  public void complete(RunningJob job) throws IOException;
}
