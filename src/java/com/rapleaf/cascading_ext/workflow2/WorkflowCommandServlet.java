package com.rapleaf.cascading_ext.workflow2;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import com.rapleaf.cascading_ext.workflow2.state.WorkflowStatePersistence;

public class WorkflowCommandServlet extends HttpServlet {

  private final WorkflowStatePersistence persistence;

  public WorkflowCommandServlet(WorkflowStatePersistence persistence) {
    this.persistence = persistence;
  }

  private static final Object lock = new Object();

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String command = req.getParameter("command");

    synchronized (lock) {
      if (command.equals("shutdown")) {
        persistence.markShutdownRequested(req.getParameter("reason"));
      } else if (command.equals("set_pool")) {
        persistence.markPool(req.getParameter("pool"));
      } else if (command.equals("set_priority")) {
        persistence.markPriority(req.getParameter("priority"));
      }
    }
    
  }


}
