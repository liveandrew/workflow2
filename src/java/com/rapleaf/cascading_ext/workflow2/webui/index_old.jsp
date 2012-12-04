<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>

<%@page import="com.rapleaf.cascading_ext.workflow2.*"%>
<%@page import="com.rapleaf.cascading_ext.workflow2.WorkflowDiagram.Vertex"%>
<%@page import="org.jgrapht.graph.*"%>
<%@page import="java.util.*"%>
<%@page import="com.rapleaf.support.DAGLayoutGenerator"%>
<%@page import="com.rapleaf.support.DAGLayoutGenerator.DAGLayout"%>
<%@page import="com.rapleaf.support.TimeHelper"%>
<%@page import="org.jgrapht.traverse.TopologicalOrderIterator"%>
<%@page import="org.jgrapht.DirectedGraph"%>
<%@page import="java.text.DateFormat"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="com.rapleaf.support.Rap"%>
<html>

<%!
public String renderProgressBar(int pctComplete) {
  if (pctComplete < 0) {
    return "<br />";
  }
  return "(" + pctComplete + "%)" +
    "<table class=\"progress_bar\">"
    + "<tr>"
    + "<td style=\"background-color: #8888ff; width: " + pctComplete + "%\"></td>"
    + "<td style=\"background-color: #aaffaa; width: " + (100 - pctComplete) + "%\"></td>"
    + "</tr>" 
    +  "</table>";
}
%>

<%
  boolean showDatastores = false;
  if (request.getParameter("datastores") != null && request.getParameter("datastores").equals("on")) {
    showDatastores = true;
  }

  WorkflowRunner wfr = (WorkflowRunner)getServletContext().getAttribute("workflowRunner");
  WorkflowDiagram wfd;
  if (session.isNew()) {
    wfd = new WorkflowDiagram(wfr);
    session.setAttribute("workflowDiagram", wfd);
  } else {
    wfd = (WorkflowDiagram)session.getAttribute("workflowDiagram");
  }
  
  if (request.getParameter("expand_all") != null && request.getParameter("expand_all").equals("1")) {
    wfd.expandAllMultistepVertices();
  } else if (request.getParameter("collapse_all") != null && request.getParameter("collapse_all").equals("1")) {
    wfd.collapseAllMultistepVertices();
  } else if (request.getParameter("expand") != null) {
    wfd.expandMultistepVertex(request.getParameter("expand"));
  } else if (request.getParameter("collapse") != null) {
    wfd.collapseParentOfVertex(request.getParameter("collapse"));
  } else if (request.getParameter("isolate") != null) {
    wfd.isolateVertex(request.getParameter("isolate"));
  } else if (request.getParameter("remove_isolation") != null) {
    wfd.reduceIsolation(request.getParameter("remove_isolation"));
  }
  
  DirectedGraph<Vertex, DefaultEdge> precedenceGraph;
  if (showDatastores) {
    precedenceGraph = wfd.getDiagramGraphWithDataStores();
  } else {
    precedenceGraph = wfd.getDiagramGraph();
  }
  Set<Vertex> vertices = precedenceGraph.vertexSet();

  Map<Vertex, Integer> vertexToId = new HashMap<Vertex, Integer>(vertices.size());
  int i = 0;
  for (Vertex vertex : vertices) {
    int id = i++;
    vertexToId.put(vertex, id);
  }

  DAGLayoutGenerator.DAGLayout<Vertex> layout = DAGLayoutGenerator.generateLayout(precedenceGraph, DAGLayoutGenerator.LayoutDirection.LEFT_TO_RIGHT);

  DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
%>

<head>
  <meta charset="utf-8" />
  <title>Workflow <%= wfr.getWorkflowName() %></title>

  <style type="text/css">
    body {
      font-size: 10pt
    }
  
    div.shutdown-notice {
      font-weight: bold;
      color: red;
    }

    .shutdown-notice-message {
      font-weight: bold;
      color: black;
    }

    td.status-column {
      text-align: center;
    }

    div.node-inner-div {
      display: table-cell;
      text-align: center;
      vertical-align: middle;
    }

    div.all-nodes {
      border: 1px solid black;
      font-size: 9pt;
      position: absolute;
    }

    div.<%= StepStatus.WAITING.name().toLowerCase() %> {
      background-color: #dddddd;
    }

    div.<%= StepStatus.RUNNING.name().toLowerCase() %> {
      background-color: #ddffdd;
    }

    div.<%= StepStatus.SKIPPED.name().toLowerCase() %> {
      background-color: #ffffdd;
    }

    div.<%= StepStatus.COMPLETED.name().toLowerCase() %> {
      background-color: #ddddff;
    }

    div.<%= StepStatus.FAILED.name().toLowerCase() %> {
      background-color: #ffdddd;
    }
    
    div.datastore {
      border-style: dashed;
      /*background-color: #ffcc88;*/
    }
    
    div.legend-entry {
      width: 50px;
      height: 30px;
      font-size: 9pt;
      text-align: center;
      vertical-align: middle;
      display: table-cell;
      padding: 3px;
    }
    
    label.collapse {
      position: absolute;
      top: 0;
      left: 1px;
      font-weight: bold;
      cursor: pointer;
    }
    label.expand {
      position: absolute;
      top: 0;
      right: 0;
      font-weight: bold;
      cursor: pointer;
    }
    
    table.progress_bar {
      border: 1px solid #333333;
      height: 12px;
      border-collapse: collapse;
      margin: auto;
      width: 100px;
    }
    table.progress_bar td {
      padding: 0;
      margin: 0;
    }
    
    #detail {
      border: 1px solid #333333;
      border-collapse: collapse;
    }
    #detail td {
      border: 1px solid #888888;
      border-left-width: 0;
    }
    #detail td.ec {
      border-right-width: 0;
      font-weight: bold;
    }
    #detail td.ec label {
      cursor: pointer;
    }

  </style>

  <script src="js/raphael-min.js" type="text/javascript" charset="utf-8"></script>
  <script src="diagrams.js" type="text/javascript" charset="utf-8"></script>
  <script type="text/javascript"><!--
    // this is where we'll put the node defns
    var diagramNodes = [
    <%
      boolean outerFirst = true;
      for (Vertex vertex : vertices) {
        if (!outerFirst) {
          %>, <%
        }
        outerFirst = false;
        %>
      {
        id: <%= vertexToId.get(vertex) %>,
        css_class: "<%= vertex.getStatus().toLowerCase() %>",
        unit_x: <%= layout.getXCoordForVertex(vertex) %>,
        unit_y: <%= layout.getYCoordForVertex(vertex) %>,
        short_name: "<%= vertex.getName() %>",
        full_name: "<%= vertex.getId() %>",
        expandable: <%= wfd.isExpandable(vertex.getId()) %>,
        collapsable: <%= wfd.hasParent(vertex.getId()) %>,
        outgoing_edges: [
        <%
          boolean innerFirst = true;
          for (DefaultEdge depEdge : precedenceGraph.incomingEdgesOf(vertex)) {
            if (!innerFirst) {
              out.print(", ");
            }
            innerFirst = false;
            Vertex depVertex = precedenceGraph.getEdgeSource(depEdge);
            out.print(vertexToId.get(depVertex));
          }
        %>],
        incoming_edges: [
        <%
          innerFirst = true;
          for (DefaultEdge depEdge : precedenceGraph.outgoingEdgesOf(vertex)) {
            if (!innerFirst) {
              out.print(", ");
            }
            innerFirst = false;
            Vertex depVertex = precedenceGraph.getEdgeTarget(depEdge);
            out.print(vertexToId.get(depVertex));
          }
        %>]
      }
        <%
        i++;
      }
    %>
    ];

    window.onload = function () {
      renderDiagram("canvas", diagramNodes);
    };
  </script>
</head>
<body>

<h2><%= wfr.getWorkflowName() %> </h2>

<form method="GET" name="diagram_options" id="diagram_options">
<%
List<String> isolated = wfd.getIsolated();
if (!isolated.isEmpty()){
  String previous = isolated.remove(0);
  for (String current : isolated) { %>
    <input type="submit" value="<%= previous %>" style="display:none;"
           name="remove_isolation" id ="remove_isolation_<%=current%>" onclick="this.value=<%= current %>" />
    <label for="remove_isolation_<%=current%>" style="color:blue;" ><%= previous %></label>
    <span style="padding: 0 8px 0 8px;">&gt;</span>
  <%
    previous = current;
  }
  %>
  <%= previous %>
<% 
} 
%>
<br/>
<div id="canvas" style="border:1px solid black; position:relative; overflow:auto; width:100%"></div>

<div id="legend" style="float:right">
<%
for (StepStatus status : StepStatus.values()) {
  String pretty_name = status.name().toLowerCase();
  %>
  <div class="legend-entry <%= status.name().toLowerCase() %>"><%= pretty_name %></div>
  <%
}
%>
<div class="legend-entry datastore">datastore</div>
</div>

<input type="submit" value="Expand All" name="expand_all" onclick="this.value=1" />
<input type="submit" value="Collapse All" name="collapse_all" onclick="this.value=1" />
<input type="checkbox" name="datastores" id="datastores" onclick="document.diagram_options.submit()"
<% if (showDatastores) out.print("checked=\"checked\""); %> />
<label for="datastores">Datastores?</label>
</form>
<h4>Controls</h4>

<%
  if (wfr.isShutdownPending()) {
%>
<div class='shutdown-notice'>
  <p>
    Workflow shutdown has been requested with the following message:
    <p class="shutdown-notice-message">
      "<%= wfr.getReasonForShutdownRequest() %>"
    </p>
  </p>
  <p>
    Currently running components will be allowed to complete before exiting.
  </p>
</div>
<%} else {%>
<form action="request_shutdown.jsp" method=post>
  <p>
    <label for="reason">Reason for shutdown:</label>
  </p>
  <p>
    <textarea name="reason" rows="10" cols="70"></textarea>
  </p>
  <input type="submit" value="Request Workflow Shutdown"/>
</form>
<%
  }
%>

<h4>Workflow Detail</h4>
<table id="detail">
  <tr>
    <th>&nbsp;</th>
    <th>&nbsp;</th>
    <th>Token</th>
    <th>Name</th>
    <th>Status</th>
    <th>Job Tracker</th>
    <th>Messages</th>
  </tr>
  <%
  TopologicalOrderIterator<Vertex, DefaultEdge> topoIter = new TopologicalOrderIterator(precedenceGraph);
  while (topoIter.hasNext()) {
    Vertex vertex = topoIter.next();
    if (vertex.getStatus().equals("datastore")) {
      continue;
    }
  %>
    <tr>
      <td class="ec">
        <% if (wfd.hasParent(vertex.getId())) { %>
          <label for="collapse_<%= vertex.getId() %>">&ndash;</label>
        <% } %>
      </td>
      <td class="ec">
        <% if (wfd.isExpandable(vertex.getId())) { %>
          <label for="expand_<%= vertex.getId() %>">+</label>
        <% } %>
      </td>
      <td>
        <label for="isolate_<%= vertex.getId() %>"><%= vertex.getId() %></label>        
      </td>
      <td>
        <%= vertex.getActionName() %>
      </td>
      <td class="status-column">
      <%= vertex.getStatus() %>
      <% if (vertex.getStatus().equals("running")) { %>
          <%= renderProgressBar(vertex.getPercentageComplete()) %>
          Started at <%= dateFormat.format(new Date(vertex.getStartTimestamp())) %>
        <%
      } else if (vertex.getStatus().equals("completed")) {
        %>
          <%= renderProgressBar(100) %>
          Started at <%= dateFormat.format(new Date(vertex.getStartTimestamp())) %>
          <br>Ended at <%= dateFormat.format(new Date(vertex.getEndTimestamp())) %>
          <br>Took <%= TimeHelper.shortHumanReadableElapsedTime(vertex.getStartTimestamp(), vertex.getEndTimestamp()) %>
        <%
      }
      %>
        </td>
      <td><%= vertex.getJobTrackerLinks() %></td>
      <td><%= vertex.getMessage() %></td>
    </tr>
    <%
  }%>
</table>

</body>
</html>
