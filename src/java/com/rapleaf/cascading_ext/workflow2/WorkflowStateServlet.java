package com.rapleaf.cascading_ext.workflow2;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import org.json.JSONObject;

import com.rapleaf.db_schemas.rldb.workflow.WorkflowStatePersistence;

public class WorkflowStateServlet extends HttpServlet {

  private final WorkflowStatePersistence persistence;

  public WorkflowStateServlet(WorkflowStatePersistence persistence) {
    this.persistence = persistence;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      JSONObject state = WorkflowDiagram.getJSONState(persistence);
      resp.addHeader("Access-Control-Allow-Origin", "*");
      resp.getWriter().append(state.toString());
    }catch(Exception e){
      throw new RuntimeException(e);
    }

  }
}
