package com.rapleaf.cascading_ext.workflow2;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class WorkflowCommandServlet extends HttpServlet {

  private final WorkflowRunner runner;
  public WorkflowCommandServlet(WorkflowRunner runner) {
    this.runner = runner;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String command = req.getParameter("command");

    if(command.equals("shutdown")){
      String reason = req.getParameter("reason");
      runner.requestShutdown(reason);
    }

  }
}
