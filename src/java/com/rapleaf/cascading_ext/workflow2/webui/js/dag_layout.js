var DAGLayout = function(columns, rows) {
  this.columns = columns;
  this.rows = rows;
};

DAGLayout.prototype = {
  getXCoordForNode: function(nodeId) {
    return this.columns[nodeId];
  },

  getYCoordForNode: function(nodeId) {
    return this.rows[nodeId];
  }
}

function generateLayout(graph) {
  var columns = {};
  var rows = {};
  var toVisit = [];

  function assignColumns() {
    for (nodeId in graph.nodes) {
      columns[nodeId] = 0;
      if (graph.incomingNodesOf(nodeId).length == 0) {
        toVisit.push(nodeId);
      }
    }
    while (toVisit.length > 0) {
      var nodeId = toVisit.shift();
      var depth = columns[nodeId];
      // look at all the incoming edges to redetermine the column for this node.
      var incomingNodes = graph.incomingNodesOf(nodeId);
      for (i in incomingNodes) {
        var source = incomingNodes[i];
        var sourceDepth = columns[source];
        if (sourceDepth >= depth) {
          depth = sourceDepth + 1;
        }
      }
      // reset the column for this node
      columns[nodeId] = depth;
      // add all the nodes connected by out-edges to the current vertex to the to-visit queue
      var outgoingNodes = graph.outgoingNodesOf(nodeId);
      for (i in outgoingNodes) {
        toVisit.push(outgoingNodes[i]);
      }
    }
  }

  function getNodesByColumn() {
    var map = {};
    for (i in columns) {
      if (map[columns[i]] == undefined) {
        map[columns[i]] = [];
      }
      map[columns[i]].push(i);
    }
    return map;
  }

  function assignRows() {
    var nodesByColumn = getNodesByColumn();

    // find the tallest column. We'll use this value to determine what the
    // vertical unit "center" of the diagram is.
    var tallestColumn = 0;
    for (i in nodesByColumn) {
      if (nodesByColumn[i].length > tallestColumn) {
        tallestColumn = nodesByColumn[i].length;
      }
    }

    // proceed through the nodes by column again, this time arraying them
    // around the verticl center of the diagram. They won't necessarily be
    // evenly distributed.
    for (i in nodesByColumn) {
      var pos = Math.round(-1 * (nodesByColumn[i].length / 2));
      for (var j = 0; j < nodesByColumn[i].length; j++, pos++) {
        rows[nodesByColumn[i][j]] = Math.round(tallestColumn / 2 + pos);
      }
    }

    // finally, go through col by col using the average heuristic to adjust
    // row positions
    for (var i = 0; i < 10; i++) {
      for (colNum in nodesByColumn) {
        if (colNum == 0) {
          continue;
        }
        var thisCol = nodesByColumn[colNum - 1];
        var nodeWeights = {};
        for (k in thisCol) {
          var nodeId = thisCol[k];
          var total = 0;
          var count = 0;
          for (m in graph.incomingNodesOf(nodeId)) {
            var nodeId2 = graph.incomingNodesOf(nodeId)[m];
            if (columns[nodeId2] == colNum -1) {
              total += rows[nodeId2];
              count++;
            }
          }
          nodeWeights[nodeId, count > 0 ? total / count : 0];
        }

        //sort by weight
        thisCol.sort(function(a,b) {
          return nodeWeights[a] - nodeWeights[b];
        });

        var pos = Math.round(-1 * (thisCol.length / 2));
        for (var j = 0; j < thisCol.length; j++, pos++) {
          rows[thisCol[j]] = Math.round(tallestColumn / 2 + pos);
        }
      }
    }
  }

  assignColumns();
  assignRows();
  var layout = new DAGLayout(columns, rows);
  return layout;
}

function merge(obj1, obj2) {
  var merged = {};
  for (var k in obj1) merged[k] = obj1[k];
  for (var k in obj2) merged[k] = obj2[k];
  return merged;
}


function getDiagramNodes(g) {
  var origGraph = g.normalizedNodeIdsGraph();
  var graph = origGraph.edgeReversedGraph();
  var layout = generateLayout(origGraph);
  var diagNodes = [];
  for (nodeId in graph.nodes) {
    var node = graph.nodes[nodeId];
    var diagNode = {
      id: nodeId,
      unit_x: layout.getXCoordForNode(nodeId),
      unit_y: layout.getYCoordForNode(nodeId),
      incoming_edges: graph.incomingNodesOf(nodeId),
      outgoing_edges: graph.outgoingNodesOf(nodeId)

    };
    diagNode = merge(diagNode, node.attrs);
    diagNodes.push(diagNode);
  }
  return diagNodes;
}
