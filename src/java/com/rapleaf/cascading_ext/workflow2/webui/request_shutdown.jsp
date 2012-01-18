<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
    
<%
  WorkflowRunner wfr = (WorkflowRunner)getServletContext().getAttribute("workflowRunner");
  wfr.requestShutdown(request.getParameter("reason"));
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="com.rapleaf.cascading_ext.workflow2.WorkflowRunner"%><html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Insert title here</title>
</head>
<body onload="document.location.href='/index.jsp';">

</body>
</html>
