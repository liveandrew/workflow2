package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public interface RunnableJob {

  public JobConf configure() throws IOException;

  public void complete(RunningJob job) throws IOException;
}
