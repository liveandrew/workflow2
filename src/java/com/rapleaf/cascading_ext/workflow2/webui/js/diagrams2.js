// layout constants
var columnWidth = 85;
var rowHeight = 60;

var gutterWidth = 30;
var maxEdgesInGutterSegment = 0;
var arrowGutterWidth = 4;

// the canvas we're drawing edges on
var paper;

var nodes;

var wfd;
var canvasId;

function renderDiagram (canvasElementId, workflowDiag) {
  wfd = workflowDiag;
  canvasId = canvasElementId;
  document.getElementById(canvasElementId).innerHTML = "";

  var graph = wfd.getDiagramGraph();
  nodes = getDiagramNodes(graph);

  var nodesByLayoutPosition = {};
  // some node preprocessing
  for (var nodeId in nodes) {
    var node = nodes[nodeId];
    nodesByLayoutPosition[[unitToLayoutX(node.unit_x), unitToLayoutY(node.unit_y)]] = node;
  }

  // route all the edges
  var edgeRoutes = routeEdges(nodesByLayoutPosition);

  // figure out the x and y offsets for edges
  var both = edgeSegmentsByPosition(edgeRoutes);
  var horizEdgeSegmentsByRow = both[0];
  var vertEdgeSegmentsByCol = both[1];

  var xOffsets = assignXOffsets(vertEdgeSegmentsByCol);
  var yOffsets = assignYOffsets2(horizEdgeSegmentsByRow);

  var width = determineWidth(nodes);
  var height = determineHeight(nodes);

  // figure out the dimensions of the display
  var canvasElement = document.getElementById(canvasElementId);

  // set up the canvas
  paper = Raphael(canvasElementId, width, height);

  // render all the nodes and their edges
  for (var nodeId = 0; nodeId < nodes.length; nodeId++) {
    drawNodeDiv(canvasElement, nodeId);

    var node = nodes[nodeId];

    for (var i = 0; i < node.outgoing_edges.length; i++) {
      drawRoute(nodeId, node.outgoing_edges[i], xOffsets, yOffsets, edgeRoutes);
    }
  }
}

function determineWidth(nodes) {
  var maxWidth = null;
  for (var i = 0; i < nodes.length; i++) {
    var node = nodes[i];
    if (maxWidth == null || maxWidth < node.unit_x) {
      maxWidth = node.unit_x;
    }
  }
  return layoutToRealX(unitToLayoutX(maxWidth) + 1);
}

function determineHeight(nodes) {
  var maxHeight = null;
  for (var i = 0; i < nodes.length; i++) {
    var node = nodes[i];
    if (maxHeight == null || maxHeight < node.unit_y) {
      maxHeight = node.unit_y;
    }
  }
  return layoutToRealY(unitToLayoutY(maxHeight) + 2);
}

// compute routes for all edges
function routeEdges(nodesByLayoutPosition) {
  var edgeRoutes = {};
  for (var nodeId in nodes) {
    var node = nodes[nodeId];
    for (var targetNodeIdx in node.outgoing_edges) {
      var targetNodeId = node.outgoing_edges[targetNodeIdx];
      // alert(nodes[nodeId].label + " -> " + nodes[targetNodeId].label);
      edgeRoutes[[nodeId, targetNodeId]] = routeEdge(nodes[nodeId], nodes[targetNodeId], nodesByLayoutPosition);
    }
  }
  return edgeRoutes;
}

function debug(s) {
  document.getElementById("debug").innerHTML += s + "<br/>";
}

function ins(x) {
  return JSON.stringify(x);
}

// compute a route from nodeFrom to nodeTo that doesn't cross over any other nodes.
function routeEdge(nodeFrom, nodeTo, nodesByLayoutPosition) {
  var targetLayoutX = unitToLayoutX(nodeTo.unit_x) + 1;
  var targetLayoutY = unitToLayoutY(nodeTo.unit_y);
  var currentLayoutX = unitToLayoutX(nodeFrom.unit_x) - 1;
  var currentLayoutY = unitToLayoutY(nodeFrom.unit_y);

  var path = [[currentLayoutX, currentLayoutY]];

  while (! (currentLayoutX == targetLayoutX && currentLayoutY == targetLayoutY)) {
    // prefer to move in the x dimension
    if (currentLayoutX > targetLayoutX && nodesByLayoutPosition[[currentLayoutX - 1, currentLayoutY]] == null) {
      currentLayoutX -= 1;
      path[path.length] = [currentLayoutX, currentLayoutY];
    } else {
      if (currentLayoutY > targetLayoutY) {
        // move in the negative y dimension
        currentLayoutY -= 1;
      } else {
        // move in the positive y dimension
        currentLayoutY += 1;
      }

      path[path.length] = [currentLayoutX, currentLayoutY];
    }
  }

  return path;
}

function unitToLayoutX(ux) {
  return 2 * ux;
}

function unitToLayoutY(uy) {
  return 2 * uy;
}

function getGutterWidth() {
  return Math.max(gutterWidth, (maxEdgesInGutterSegment + 2) * 5)
}

function layoutToRealX(lx) {
  return parseInt(Math.ceil(lx / 2) * (columnWidth + arrowGutterWidth) + Math.floor(lx / 2) * getGutterWidth()) + 5;
}

function layoutToRealY(ly) {
  return parseInt(Math.ceil(ly / 2) * rowHeight + Math.floor(ly / 2) * getGutterWidth()) + 5;
}

function getRealRowHeight(lx) {
  if (lx % 2 == 0) {
    return rowHeight;
  } else {
    return gutterWidth;
  }
}

// compare two edges x and y for the purpose of assigning Y offsets.
function compareEdgesY(x,y) {
  var aSource = nodes[x[0]];
  var aTarget = nodes[x[1]];
  var bSource = nodes[y[0]];
  var bTarget = nodes[y[1]];

  if (aSource.unit_y < bSource.unit_y) {
    return -1;
  } else if (aSource.unit_y > bSource.unit_y) {
    return 1;
  } else {
    if (aTarget.unit_y < bTarget.unit_y) {
      return -1;
    } else if (aTarget.unit_y > bTarget.unit_y) {
      return 1;
    } else {
      if (aTarget.unit_x > bTarget.unit_x) {
        return -1;
      } else if (aTarget.unit_x < bTarget.unit_x) {
        return 1;
      } else {
        if (aSource.unit_x > bSource.unit_x) {
          return -1;
        } else if (aSource.unit_x < bSource.unit_x) {
          return 1;
        } else {
          return 0;
        }
      }
    }
  }
}

//compare two edges x and y for the purpose of assigning X offsets.
function compareEdgesX(x,y) {
  var aSource = nodes[x[0]];
  var aTarget = nodes[x[1]];
  var bSource = nodes[y[0]];
  var bTarget = nodes[y[1]];

  if (aSource.unit_x < bSource.unit_x) {
    return -1;
  } else if (aSource.unit_x > bSource.unit_x) {
    return 1;
  } else {
    if (aTarget.unit_x < bTarget.unit_x) {
      return -1;
    } else if (aTarget.unit_x > bTarget.unit_x) {
      return 1;
    } else {
      if (aTarget.unit_y > bTarget.unit_y) {
        return -1;
      } else if (aTarget.unit_y < bTarget.unit_y) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}

function uniques(arr) {
  var uniq = []
  for (var i = 0; i < arr.length; i++) {
    var unique = true;
    for (var j = 0; j < uniq.length && unique; j++) {
      if ("" + uniq[j] == "" + arr[i]) {
        unique = false;
      }
    }
    if (unique) {
      uniq.push(arr[i]);
    }
  }
  return uniq;
}

function intersect(a1, a2) {
  var result = [];

  for (var i = 0; i < a1.length; i++) {
    for (var j = 0; j < a2.length; j++) {
      if ("" + a1[i] == "" + a2[j]) {
        result.push(a1[i]);
        break;
      }
    }
  }
  
  for (var i = 0; i < a2.length; i++) {
    for (var j = 0; j < a1.length; j++) {
      if ("" + a2[i] == "" + a1[j]) {
        result.push(a2[i]);
        break;
      }
    }
  }

  return uniques(result);
}

function union(a1, a2) {
  return uniques(a1.concat(a2));
}

function findMaxEdgesInGutterSegment(x) {
  if (x > maxEdgesInGutterSegment) {
    maxEdgesInGutterSegment = x;
  }
}

function assignYOffsets2(edgeSegmentsByRow) {
  var yOffsets = {};

  for (var rowNum = 0; rowNum < edgeSegmentsByRow.length; rowNum++) {
    var row = edgeSegmentsByRow[rowNum];

    if (row == null) {
      continue;
    }
    
    var startCol = null;
    var edgesFoundInThisRegion = [];
    for (var colNum = 0; colNum < row.length; colNum++) {
      var cell = row[colNum];

      if (startCol != null && cell == null) {
        // colNum - 1 is the end of the current region, and a new one is not starting
        for (var regionCol = startCol; regionCol < colNum; regionCol++) {
          edgesFoundInThisRegion.sort(compareEdgesY);
          findMaxEdgesInGutterSegment(edgesFoundInThisRegion.length);
          for (var edgeNum = 0; edgeNum < edgesFoundInThisRegion.length; edgeNum++) {
            var edge = edgesFoundInThisRegion[edgeNum];
            yOffsets[[edge[0], edge[1], regionCol, rowNum]] = parseInt(edgeNum + 1 - (Math.ceil(edgesFoundInThisRegion.length / 2))) * 5;
          }
        }
        startCol = null;
        edgesFoundInThisRegion = null;
      } else if (startCol == null && cell != null) {
        // no current region, starting a new one
        startCol = colNum;
        edgesFoundInThisRegion = cell;
      } else if (startCol != null && cell != null) {
        // might be the end of this and start of next, might be a continuation of current region
        var intersection = intersect(cell, row[colNum-1]);
        if (intersection.length == 0) {
          // colNum - 1 is the end of the current region; colNum is the start of the next
          for (var regionCol = startCol; regionCol < colNum; regionCol++) {
            edgesFoundInThisRegion.sort(compareEdgesY);
            findMaxEdgesInGutterSegment(edgesFoundInThisRegion.length);
            for (var edgeNum = 0; edgeNum < edgesFoundInThisRegion.length; edgeNum++) {
              var edge = edgesFoundInThisRegion[edgeNum];
              yOffsets[[edge[0], edge[1], regionCol, rowNum]] = parseInt(edgeNum + 1 - (Math.ceil(edgesFoundInThisRegion.length / 2))) * 5;
            }
          }
          startCol = colNum;
          edgesFoundInThisRegion = union(edgesFoundInThisRegion, cell);
        } else {
          // continuation of the current region
          edgesFoundInThisRegion = union(edgesFoundInThisRegion, cell);
        }
      }
    }
    if (startCol != null) {
      for (var regionCol = startCol; regionCol < row.length; regionCol++) {
        edgesFoundInThisRegion.sort(compareEdgesY);
        findMaxEdgesInGutterSegment(edgesFoundInThisRegion.length);
        for (var edgeNum = 0; edgeNum < edgesFoundInThisRegion.length; edgeNum++) {
          var edge = edgesFoundInThisRegion[edgeNum];
          yOffsets[[edge[0], edge[1], regionCol, rowNum]] = parseInt(edgeNum + 1 - (Math.ceil(edgesFoundInThisRegion.length / 2))) * 5;
        }
      }
    }
  }
  return yOffsets;
}


function assignXOffsets(edgeSegmentsByCol) {
  var xOffsets = {};
  for (var colNum = 0; colNum < edgeSegmentsByCol.length; colNum++) {
    var col = edgeSegmentsByCol[colNum];
    if (col == null) {
      continue;
    }

    var allEdgesSet = {};
    for (var rowNum = 0; rowNum < col.length; rowNum++) {
      for (edgeNum in col[rowNum]) {
        allEdgesSet[col[rowNum][edgeNum]] = 1;
      }
    }
    allEdges = [];

    for (var edge in allEdgesSet) {
      allEdges[allEdges.length] = eval("[" + edge + "]");
    }

    // assign y-offsets to edges
    allEdges.sort(compareEdgesX);

    findMaxEdgesInGutterSegment(allEdges.length);

    for (var edgeNum = 0; edgeNum < allEdges.length; edgeNum++) {
      var edge = allEdges[edgeNum];
      xOffsets[[edge[0], edge[1], colNum]] = parseInt(edgeNum + 1 - (Math.ceil(allEdges.length / 2))) * 5;
    }
  }
  return xOffsets;
}


function edgeSegmentsByPosition(edgeRoutes) {
  // contains horiz occurrences
  var byRow = [];
  // contains vert occurrences
  var byCol = [];

  for (var edge in edgeRoutes) {
    var route = edgeRoutes[edge];

    for (var i = 0; i < route.length; i++) {
      var point = route[i];
      if (i == 0 || i == route.length - 1 || route[i-1][1] == point[1] || (i < route.length - 1 && route[i+1][1] == point[1])) {
        // it's horizontal from the last point
        if (byRow[point[1]] == null) {
          byRow[point[1]] = [];
        }
        if (byRow[point[1]][point[0]] == null) {
          byRow[point[1]][point[0]] = [];
        }
        var targetSet = byRow[point[1]][point[0]];
        targetSet.push(eval("[" + edge + "]"));
      }

      if ((i < route.length - 1 && route[i+1][0] == point[0]) || (i > 0 && route[i-1][0] == point[0])) {
        // it's vertical
        if (byCol[point[0]] == null) {
          byCol[point[0]] = [];
        }
        if (byCol[point[0]][point[1]] == null) {
          byCol[point[0]][point[1]] = [];
        }
        var targetSet = byCol[point[0]][point[1]];
        targetSet.push(eval("[" + edge + "]"));
      }
    }
  }

  return [byRow, byCol];
}

function drawNodeDiv(canvasElement, nodeId) {
  var node = nodes[nodeId];
  var box = document.createElement('div');
  box.style.position = 'absolute';
  box.style.top = layoutToRealY(unitToLayoutY(node.unit_y)) + 'px';
  box.style.left = layoutToRealX(unitToLayoutX(node.unit_x)) + 'px';
  box.style.width = (columnWidth - 2) + 'px';
  box.style.height = (rowHeight - 2) + 'px';
  box.setAttribute("class", "all-nodes " + node.css_class);
  
  isolateButton = document.createElement('input');
  isolateButton.type = 'submit';
  isolateButton.name = 'isolate';
  isolateButton.id = 'isolate_' + node.full_name;
  isolateButton.value = node.full_name;
  isolateButton.style.display = 'none';
  box.appendChild(isolateButton);

  innerBox = document.createElement('div');
  innerBox.setAttribute("class", "node-inner-div");
  innerBox.title = node.full_name;
  innerBox.style.width = (columnWidth - 4) + 'px';
  innerBox.style.minWidth = (columnWidth - 4) + 'px';
  innerBox.style.height = (rowHeight - 4) + 'px';
  box.appendChild(innerBox);
  
  labelStr = node.short_name;
  // browsers wrap on hyphens but not underscores and we have no spaces to wrap,
  // so the only thing that could possibly wrap is a hyphen. therefore...
  if (labelStr.length > 10 && labelStr.length - labelStr.lastIndexOf('-') > 10)
  	labelStr = labelStr.substring(0, 10) + '...' 
  	//labelStr = '...' + labelStr.substring(labelStr.length - 10);
  
  isolateLabel = document.createElement('label');
  isolateLabel.htmlFor = 'isolate_' + node.full_name;
  isolateLabel.innerHTML = labelStr;
  isolateLabel.className = 'isolate';
  innerBox.appendChild(isolateLabel);
  
  if (node.collapsable) {
    var collapseLabel = document.createElement('label');
    collapseLabel.id = "collapse_" + node.full_name.replace(/\s/g, '-') + "_label";
    collapseLabel.innerHTML = '<i class="icon-minus-sign icon-white" >';
    collapseLabel.className = 'collapse-node';
    collapseLabel.onclick = function() {
      wfd.collapseParentOfStep(node.full_name)
      updateView();
    };
    
    box.appendChild(collapseLabel);
  }
  if (node.expandable) {
    var expandLabel = document.createElement('label');
    expandLabel.id = "expand_" + node.full_name.replace(/\s/g, '-') + "_label";
    expandLabel.innerHTML = '<i class="icon-plus-sign icon-white" >';
    expandLabel.className = 'expand-node';
    expandLabel.onclick = function() {
      wfd.expandMultistep(node.full_name)
      updateView();
    };
    
    box.appendChild(expandLabel);
  }
  
  canvasElement.appendChild(box);
}

function simplifyRoute(route) {
  if (route.length < 3) {
    return route;
  }

  var curDirection = null;

  var newRoute = [route[0]];
  var firstPointOnLine = route[0];
  var farthestDiscoveredPointOnLine = firstPointOnLine;

  for (var i = 1; i < route.length; i++) {
    var thisPoint = route[i];
    if (curDirection == null) {
      if (thisPoint[0] == farthestDiscoveredPointOnLine[0]) {
        curDirection = 0;
      } else {
        curDirection = 1;
      }
    }
  
    if (thisPoint[curDirection] == farthestDiscoveredPointOnLine[curDirection]) {
      farthestDiscoveredPointOnLine = thisPoint;
    } else {
      newRoute[newRoute.length] = farthestDiscoveredPointOnLine;
      firstPointOnLine = farthestDiscoveredPointOnLine = thisPoint;
      curDirection = curDirection == 0 ? 1 : 0;
    }
  }

  newRoute[newRoute.length] = farthestDiscoveredPointOnLine;

  return newRoute;
}

function drawRoute(nodeFromId, nodeToId, xOffsets, yOffsets, edgeRoutes) {
  var route = edgeRoutes[[nodeFromId, nodeToId]];
  var simplifiedRoute = simplifyRoute(route);

  var nodeFrom = nodes[nodeFromId];
  var nodeTo = nodes[nodeToId];

  var halfGutterWidth = parseInt(gutterWidth / 2);

  var halfRowHeight = parseInt(rowHeight / 2);

  var lastY = layoutToRealY(unitToLayoutY(nodeFrom.unit_y)) 
    + halfRowHeight
    + yOffsets[[nodeFromId, nodeToId, unitToLayoutX(nodeFrom.unit_x) - 1, unitToLayoutY(nodeFrom.unit_y)]];

  var pathStr = "M" + (layoutToRealX(unitToLayoutX(nodeFrom.unit_x)))
    + " "
    + lastY;
  
  var lastX = (layoutToRealX(unitToLayoutX(nodeFrom.unit_x)));
  
  paper.path("M" + lastX + " " + lastY 
    + " " + (lastX - 3) + " " + (lastY + 3) 
    + " " + (lastX - 3) + " " + (lastY - 3) 
    + "Z"
  ).attr({fill: "#000"});

  var direction = "H";

  for (var i = 0; i < simplifiedRoute.length; i++) {
    pathStr += direction;
    if (direction == "H") {
      pathStr += (layoutToRealX(simplifiedRoute[i][0]) 
        + halfGutterWidth 
        + to_i(xOffsets[[nodeFromId, nodeToId, simplifiedRoute[i][0]]]));
      if (i < simplifiedRoute.length - 1 && simplifiedRoute[i+1][0] == simplifiedRoute[i][0]) {
        direction = "V";
      } else {
        direction = "H";
      }
    } else {
      lastY = (layoutToRealY(simplifiedRoute[i][1]) 
        + getRealRowHeight(simplifiedRoute[i][1]) / 2)
        + yOffsets[[nodeFromId, nodeToId, simplifiedRoute[i][0], simplifiedRoute[i][1]]];
      pathStr += lastY;
      if (i < simplifiedRoute.length - 1 && simplifiedRoute[i+1][0] == simplifiedRoute[i][0]) {
        direction = "V";
      } else {
        direction = "H";
      }
    }
  }

  var targetX = layoutToRealX(unitToLayoutX(nodeTo.unit_x) + 1) - arrowGutterWidth;
  pathStr += "H" + targetX;
  paper.path(pathStr);
}

function to_i(x) {
  if (x == null) {
    return 0;
  }
  return x;
}

