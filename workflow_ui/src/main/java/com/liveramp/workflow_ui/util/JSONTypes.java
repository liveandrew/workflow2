package com.liveramp.workflow_ui.util;

import org.json.JSONException;
import org.json.JSONObject;

import com.liveramp.databases.workflow_db.models.Dashboard;

public class JSONTypes {

  public static JSONObject toJson(Dashboard dashboard) throws JSONException {
    return new JSONObject()
        .put("id", dashboard.getId())
        .put("name", dashboard.getName());
  }

}
