var Graph = function() {
  this.nodes = {};
  this.edges = {};
  return this;
};

Graph.Node = function(id, attrs) {
  this.id = id;
  this.attrs = attrs || {};
  this.incomingNodes = [];
  this.outcomingNodes = [];
  return this;
};

Graph.Node.prototype = {
  addEdge: function(edge) {
    if (edge.source == this.id) {
      if (this.outcomingNodes.indexOf(edge.target) == -1) {
        this.outcomingNodes.push(edge.target);
      }
    } else if (edge.target == this.id) {
      if (this.incomingNodes.indexOf(edge.source) == -1) {
        this.incomingNodes.push(edge.source);
      }
    }
  },

  removeEdge: function(edge) {
    if (edge.source == this.id) {
      for (i in this.outcomingNodes) {
        var targetId = this.outcomingNodes[i];
        if (targetId == edge.target) {
          this.outcomingNodes.splice(i, 1);
          break;
        }
      }
    } else if (edge.target == this.id) {
      for (i in this.incomingNodes) {
        var sourceId = this.incomingNodes[i];
        if (sourceId == edge.source) {
          this.incomingNodes.splice(i, 1);
          break;
        }
      }
    }
  }
}

Graph.Edge = function(sourceId, targetId) {
  this.source = sourceId;
  this.target = targetId;
  return this;
}

Graph.Edge.prototype = {
  getKey: function() {
    return this.source + "-" + this.target;
  }
}

Graph.prototype = {
  addNode: function(node) {
    var ret = this.nodes[node.id];
    if (ret == undefined){
      this.nodes[node.id] = node;
      ret = node;
    }
    return ret;
  },

  addEdge: function(source, target) {
    var s = this.addNode(source);
    var t = this.addNode(target);
    var e = new Graph.Edge(s.id, t.id);
    var key = e.getKey();
    this.edges[key] = e;
    s.addEdge(e);
    t.addEdge(e);
  },

  removeEdge: function(sourceId, targetId) {
    var e = new Graph.Edge(sourceId, targetId);
    delete this.edges[e.getKey()];
    this.nodes[sourceId].removeEdge(e);
    this.nodes[targetId].removeEdge(e);
  },

  outgoingNodesOf: function(nodeId) {
    var node = this.nodes[nodeId];
    return node.outcomingNodes.slice();
  },

  incomingNodesOf: function(nodeId) {
    var node = this.nodes[nodeId];
    return node.incomingNodes.slice();
  },

  edgeReversedGraph: function() {
    var reversed = new Graph();
    for(var nodeId in this.nodes) {
      var node = this.nodes[nodeId];
      var newNode = new Graph.Node(node.id, node.attrs);
      newNode.incomingNodes = node.outcomingNodes.slice();
      newNode.outcomingNodes = node.incomingNodes.slice();
      reversed.addNode(newNode);
    }
    for(i in this.edges) {
      var edge = this.edges[i];
      var newEdge = new Graph.Edge(edge.target, edge.source);
      var key = newEdge.getKey();
      reversed.edges[key] = newEdge;
    }
    return reversed;
  },

  // Returns an equivalent graph whose nodes ids are 0, 1, ... origGraph.nodes.length - 1
  normalizedNodeIdsGraph: function() {
    var nodeMappings = {};
    var i = 0;
    for (var nodeId in this.nodes) {
      nodeMappings[nodeId] = i++;
    }
    var normalized = new Graph();
    for (var nodeId in this.nodes) {
      var nodeCopy = new Graph.Node(nodeMappings[nodeId], this.nodes[nodeId].attrs);
      normalized.addNode(nodeCopy);
    }
    for (var i in this.edges) {
      var e = this.edges[i];
      var sourceCopy = new Graph.Node(nodeMappings[e.source], this.nodes[e.source].attrs);
      var targetCopy = new Graph.Node(nodeMappings[e.target], this.nodes[e.target].attrs);
      normalized.addEdge(sourceCopy, targetCopy);
    }
    return normalized;
  }

}
