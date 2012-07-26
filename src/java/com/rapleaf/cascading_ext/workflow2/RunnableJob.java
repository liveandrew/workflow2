package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import org.apache.hadoop.mapred.JobConf;

public interface RunnableJob {

  public JobConf configure() throws IOException;

  public void complete() throws IOException;
}
