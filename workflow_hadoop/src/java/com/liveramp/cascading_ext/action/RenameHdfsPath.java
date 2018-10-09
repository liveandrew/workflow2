package com.liveramp.cascading_ext.action;

import org.apache.hadoop.fs.Path;

import com.rapleaf.cascading_ext.workflow2.Action;

public class RenameHdfsPath extends Action {
  private final String source;
  private final String destination;

  public RenameHdfsPath(
      String checkpointToken,
      String source,
      String destination) {
    super(checkpointToken);
    this.source = source;
    this.destination = destination;
  }

  @Override
  protected void execute() throws Exception {
    boolean successful = getFS().rename(new Path(source), new Path(destination));

    if (!successful) {
      throw new RuntimeException("Failed to rename " + source + " to " + destination);
    }
  }
}
