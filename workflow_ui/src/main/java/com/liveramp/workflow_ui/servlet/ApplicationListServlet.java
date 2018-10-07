package com.liveramp.workflow_ui.servlet;

import java.util.Map;

import com.google.common.collect.Maps;
import org.json.JSONArray;
import org.json.JSONObject;

import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.databases.workflow_db.models.Application;
import com.liveramp.db_utils.BaseJackUtil;
import com.liveramp.workflow_db_state.WorkflowQueries;

public class ApplicationListServlet implements JSONServlet.Processor{
  @Override
  public JSONObject getData(IDatabases rldb, Map<String, String> parameters) throws Exception {

    JSONArray array = new JSONArray();
    for (Application application : WorkflowQueries.getAllApplications(rldb)) {
      array.put(BaseJackUtil.toJSON(application.getAttributes(), Maps.<Enum, Class<? extends Enum>>newHashMap(), ""));
    }

    return new JSONObject().put("applications", array);
  }
}
