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

  private static final Object lock = new Object();

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String command = req.getParameter("command");

    synchronized (lock) {
      if (command.equals("shutdown")) {
        runner.requestShutdown(req.getParameter("reason"));
      } else if (command.equals("set_pool")) {
        runner.setPool(req.getParameter("pool"));
      } else if (command.equals("set_priority")) {
        runner.setPriority(req.getParameter("priority"));
      }
    }
    
  }
}
