package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

import com.rapleaf.db_schemas.rldb.workflow.WorkflowStatePersistence;

public interface TrackerURLBuilder {
  public String buildURL(WorkflowStatePersistence persistence) throws IOException;

  public class None implements TrackerURLBuilder{

    @Override
    public String buildURL(WorkflowStatePersistence persistence) throws IOException {
      return "Not Implemented";
    }
  }

}
