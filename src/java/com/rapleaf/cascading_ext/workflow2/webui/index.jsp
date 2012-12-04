<%@ page import="com.rapleaf.cascading_ext.workflow2.WorkflowRunner" %>
<%@ page import="com.rapleaf.cascading_ext.workflow2.WorkflowDiagram" %>
<%@ page language="java" contentType="text/html; charset=UTF-8"
         pageEncoding="UTF-8"%>
<%
  WorkflowRunner wfr = (WorkflowRunner)getServletContext().getAttribute("workflowRunner");
  WorkflowDiagram wfd;
  if (session.isNew()) {
    wfd = new WorkflowDiagram(wfr);
    session.setAttribute("workflowDiagram", wfd);
  } else {
    wfd = (WorkflowDiagram)session.getAttribute("workflowDiagram");
  }
%>
<%= wfr.getDocsHtml(wfd, true) %>
