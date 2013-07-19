var Wfd = function(steps, datastores) {
  this.workflowDef = steps;
  this.datastores = datastores;
  this.idToStep = {};
  this.nameToDatastore = {};
  this.multistepIds = [];
  this.idToParentId = {};
  this.longNameToId = {};
  this.multistepsToExpand = [];
  this.includeDatastores = false;
  this.populateMappings();
};

Wfd.Step = function(id, name, java_class, status, startTimestamp, endTimestamp,pctComplete, dependencies, substeps, input_datastores, output_datastores) {
  this.id = id;
  this.name = name;
  this.java_class = java_class;
  this.status = status;
  this.startTimestamp = startTimestamp;
  this.endTimestamp = endTimestamp;
  this.pctComplete = pctComplete;
  this.dependencies = dependencies;
  this.substeps = substeps;
  this.inputDatastores = input_datastores;
  this.outputDatastores = output_datastores;

  return this;
};

Wfd.Datastore = function(name, type, path, relPath) {
  this.name = name;
  this.type = type;
  this.path = path;
  this.relPath = relPath;
};

function subtract(a1, a2) {
  var ret = [];
  for (i in a1) {
    if (a2.indexOf(a1[i]) == -1) {
      ret.push(a1[i]);
    }
  }
  return ret;
}

Wfd.prototype = {

  populateMappings: function() {
    for (i in this.workflowDef) {
      var step = this.workflowDef[i];
      this.idToStep[step.id] = step;
      if (step.substeps.length > 0) {
        this.multistepIds.push(step.id);
        for (j in step.substeps) {
          var substepId = step.substeps[j];
          this.idToParentId[substepId] = step.id;
        }
      }
    }
    for (i in this.workflowDef) {
      var step = this.workflowDef[i];
      var longName = this.getLongNameForStep(step);
      this.longNameToId[longName] = step.id;
    }
    for (i in this.datastores) {
      var ds = this.datastores[i];
      this.nameToDatastore[ds.name] = ds;
    }
  },

  getSortedSteps: function() {
    return this.workflowDef.sort(function(s1, s2){
      if (s1.name.toLowerCase() < s2.name.toLowerCase()) {
        return -1;
      } else if (s1.name.toLowerCase() > s2.name.toLowerCase()) {
        return 1;
      } else {
        return 0;
      }
    });
  },

  getSortedDatastores: function() {
    return this.datastores.sort(function(d1, d2) {
      if (d1.name.toLowerCase() < d2.name.toLowerCase()) {
        return -1;
      } else if (d1.name.toLowerCase() > d2.name.toLowerCase()) {
        return 1;
      } else {
        return 0;
      }
    });
  },

  getLongNameForStep: function(step) {
    var parents = [];
    var id = this.idToParentId[step.id];
    while (id != undefined) {
      parents.push(id);
      id = this.idToParentId[id];
    }
    var name = "";
    while (parents.length > 0) {
      name += this.idToStep[parents.pop()].name + "__";
    }
    name += step.name;
    return name;
  },

  isExpandable: function(stepId) {
    return this.multistepIds.indexOf(stepId) != -1;
  },

  isCollapsable: function(stepId) {
    var parentId = this.idToParentId[stepId];
    return parentId != undefined && this.shouldBeExpanded(parentId);
  },

  shouldBeExpanded: function(stepId) {
    return this.multistepsToExpand.indexOf(stepId) != -1;
  },

  shouldBeIncluded: function(stepId) {
    if ((this.idToParentId[stepId] == undefined || this.shouldBeExpanded(this.idToParentId[stepId])) && !this.shouldBeExpanded(stepId)) {
      return true;
    }
    return false;
  },

  dsShouldBeIncluded: function(ds, excludedStepId) {
    for (i in this.workflowDef) {
      var step = this.workflowDef[i];
      if ((excludedStepId == undefined || excludedStepId != step.id) && this.shouldBeIncluded(step.id)) {
        if (step.inputDatastores.indexOf(ds) != -1) {
          return true;
        }
      }
    }
    return false;
  },

  dsShouldBeIncludedMap: function() {
    var map = {};
    for (i in this.datastores) {
      var ds = this.datastores[i];
      map[ds.name] = this.dsShouldBeIncluded(ds.name);
    }
    return map;
  },

  getHeadsFromSteps: function(stepIds) {
    var ret = [];
    for (i in stepIds) {
      var stepId = stepIds[i];
      var step = this.idToStep[stepId];
      if (ret.indexOf(step.id) == -1 && step.dependencies.length == 0) {
        ret.push(step.id);
      }
    }
    return ret;
  },

  getTailsFromSteps: function(stepIds) {
    var nonTails = [];
    var seen = [];
    for (i in stepIds) {
      var stepId = stepIds[i];
      var step = this.idToStep[stepId];
      if (seen.indexOf(step.id) == -1) seen.push(step.id);
      nonTails = nonTails.concat(step.dependencies);
    }
    return subtract(seen, nonTails);
  },

  expandMultistep: function(stepLongName) {
    var stepId = this.longNameToId[stepLongName];
    if (this.multistepIds.indexOf(stepId) != -1 && this.multistepsToExpand.indexOf(stepId) == -1) {
      this.multistepsToExpand.push(stepId);
    }
  },

  expandAll: function() {
    this.multistepsToExpand = this.multistepIds.slice();
  },

  collapseParentOfStep: function(stepLongName) {
    var stepId = this.longNameToId[stepLongName];
    var parentId = this.idToParentId[stepId];
    this.collapseMultistep(parentId);
  },

  collapseMultistep: function(stepId) {
    if (stepId != undefined) {
      var i = this.multistepsToExpand.indexOf(stepId);
      if (i != -1) {
        this.multistepsToExpand.splice(i, 1);
        for (j in this.idToStep[stepId].substeps) {
          var subStepId = this.idToStep[stepId].substeps[j];
          this.collapseMultistep(subStepId);
        }
      }
    }
  },

  collapseAll: function() {
    this.multistepsToExpand = [];
  },

  // Sort the steps to expand such that parents always appear before their children.
  getSortedMultistepsToExpand: function(expandableStepIds) {
    var toProcess = expandableStepIds.slice();
    var sorted = [];
    while(toProcess.length > 0) {
      var stepId = toProcess.shift();
      var parentId = this.idToParentId[stepId];
      var step = this.idToStep[stepId];
      if (parentId == undefined || sorted.indexOf(parentId) != -1) {
        sorted.push(stepId);
      } else {
        toProcess.push(stepId);
      }
    }
    return sorted;
  },

  getStepMap: function(def) {
    var map = {};
    for (i in def) {
      var step = def[i];
      map[step.id] = step;
    }
    return map;
  },

  // Go though all nodes that should be expanded and update the dependencies
  // to make it possible to replace them with their children.
  getDefWithUpdatedDependencies: function(origDef) {

    var newDef = origDef.slice();
    var idToStep = this.getStepMap(newDef);
    var toExpand = this.getSortedMultistepsToExpand(this.multistepsToExpand);
    for (i in toExpand) {
      var eStep = this.idToStep[toExpand[i]];
      var heads = this.getHeadsFromSteps(eStep.substeps);
      for (j in heads) {
        var head = idToStep[heads[j]];
        head.dependencies = head.dependencies.concat(eStep.dependencies);
      }

      var tails = this.getTailsFromSteps(eStep.substeps);
      for (j in newDef) {
        var step = newDef[j];
        if (step.dependencies.indexOf(eStep.id) != -1) {
          step.dependencies = step.dependencies.concat(tails);
        }
      }
    }
    return newDef;
  },

  // Go through all expandable nodes and update them to contain the input/output
  // datastores from their children.
  getDefWithUpdatedDatastores: function(origDef) {
    var expandable = this.getSortedMultistepsToExpand(this.multistepIds).reverse;
    var newDef = origDef.slice();
    var idToStep = this.getStepMap(newDef);
    for (i in expandable) {
      var eStep = idToStep[expandable[i]];
      for (j in eStep.substeps) {
        var subStep = idToStep[eStep.substesp[j]];
        eStep.inputDatastores = eStep.inputDatastores.concat(subStep.inputDatastores);
        eStep.outputDatastores = eStep.outputDatastores.concat(subStep.outputDatastores);
      }
    }

    return newDef;
  },

  getInputDsRecursive: function(idToStep, datastores, step) {
    for (i in step.inputDatastores) {
      datastores.push(step.inputDatastores[i]);
    }
    for (i in step.dependencies) {
      var dependency = idToStep[step.dependencies[i]];
      this.getInputDsRecursive(idToStep, datastores, dependency);
    }
  },

  getDependenciesRecursive: function(stepId, dependencies) {
    var step = this.idToStep[stepId];
    for (i in step.dependencies) {
      var dependency = step.dependencies[i];
      if (dependencies.indexOf(dependency) == -1) dependencies.push(dependency);
      this.getDependenciesRecursive(dependency, dependencies);
    }
  },

  getDiagramGraphWithDatastores: function() {
    var def = this.getDefWithUpdatedDependencies(this.workflowDef);
    def = this.getDefWithUpdatedDatastores(def);
    var idToStep = this.getStepMap(def);
    var graph = new Graph();
    var dsId = 10000;
    var dsNodes = {};
    var dsToCreatorSteps = {};
    var dsShouldBeIncluded = this.dsShouldBeIncludedMap();
    for (i in def) {
      var step = def[i];
      if (!this.shouldBeIncluded(step.id)) continue;
      var stepNode = this.getNodeFromStep(step);

      for (j in step.outputDatastores) {
        var ds = step.outputDatastores[j];
        if (!dsShouldBeIncluded[ds]) continue;
        var inputDatastores = [];
        this.getInputDsRecursive(idToStep, inputDatastores, step);
        var suffix = inputDatastores.indexOf(ds) == -1 ? undefined : step.name;

        if(suffix != undefined) {
          if (!this.dsShouldBeIncluded(ds, step.id)) continue;
          if (dsToCreatorSteps[ds] == undefined) dsToCreatorSteps[ds] = [];
          if (dsToCreatorSteps[ds].indexOf(ds) == -1) dsToCreatorSteps[ds].push(step.id);
        }

        var dsNode = this.getOrCreateDsNode(dsNodes, dsId++, ds, suffix);
        graph.addNode(dsNode);
        graph.addEdge(stepNode, dsNode);
      }

      graph.addNode(stepNode);
      for (j in step.dependencies) {
        var dependencyNode = this.getNodeFromStep(this.idToStep[step.dependencies[j]]);

        if (this.shouldBeIncluded(dependencyNode.id)) {
          graph.addEdge(dependencyNode, stepNode);
        }
      }
    }

    for (i in def) {
      var step = def[i];
      if (!this.shouldBeIncluded(step.id)) continue;
      var stepNode = this.getNodeFromStep(step);

      var dependencies = [];
      this.getDependenciesRecursive(step.id, dependencies);

      for (j in step.inputDatastores) {
        var ds = step.inputDatastores[j];
        var suffix = undefined;
        for (k in dependencies) {
          var dependency = dependencies[k];
          if (dsToCreatorSteps[ds] != undefined && dsToCreatorSteps[ds].indexOf(dependency) != -1) {
            suffix = this.idToStep[dependency].name;
            break;
          }
        }
        if (!dsShouldBeIncluded[ds]) continue;
        var dsNode = this.getOrCreateDsNode(dsNodes, dsId++, ds, suffix);
        graph.addNode(dsNode);
        graph.addEdge(dsNode, stepNode);
      }
    }

    this.removeRedundantEdges(graph);
    return graph;
  },

  getDiagramGraphWithoutDatastores: function() {
    var def = this.getDefWithUpdatedDependencies(this.workflowDef);
    var graph = new Graph();
    for (i in def) {
      var step = def[i];
      if (!this.shouldBeIncluded(step.id)) continue;
      var stepNode = this.getNodeFromStep(step);
      graph.addNode(stepNode);
      for (j in step.dependencies) {
        var dependencyNode = this.getNodeFromStep(this.idToStep[step.dependencies[j]]);

        if (this.shouldBeIncluded(dependencyNode.id)) {
          graph.addEdge(dependencyNode, stepNode);
        }
      }
    }

    return graph;
  },

  getDiagramGraph: function() {
    return this.includeDatastores ? this.getDiagramGraphWithDatastores() : this.getDiagramGraphWithoutDatastores();
  },

  getNodeFromStep: function(step) {
    var nodeAttrs = {
      short_name: step.name,
      full_name: this.getLongNameForStep(step),
      css_class: step.status,
      expandable: step.substeps.length > 0 ? true : false,
      collapsable: this.idToParentId[step.id] != undefined ? true : false
    };
    return new Graph.Node(step.id, nodeAttrs);
  },

  getOrCreateDsNode: function(dsNodes, nodeId, datastore, suffix) {
    var fullName = datastore + (suffix != undefined ? "__" + suffix : "");
    if (dsNodes[fullName] != undefined) {
      return dsNodes[fullName];
    } else {
      var nodeAttrs = {
        short_name: datastore,
        full_name: fullName,
        css_class: "datastore",
        expandable: false,
        collapsable: false
      };
      var newNode = new Graph.Node(nodeId, nodeAttrs);
      dsNodes[fullName] = newNode;
      return newNode;
    }
  },

  removeRedundantEdges: function(graph) {
    function getOutgoingNodesRecursive(nodeId, results, graph, visited) {
      if (visited.indexOf(nodeId) == -1) {
        visited.push(nodeId);
      } else {
        return;
      }

      var outgoing = graph.outgoingNodesOf(nodeId);
      for (i in outgoing) {
        var outNodeId = outgoing[i];
        if (results.indexOf(outNodeId) == -1) results.push(outNodeId);
        getOutgoingNodesRecursive(outNodeId, results, graph, visited);
      }
    }

    for (nodeId in graph.nodes) {
      var node = graph.nodes[nodeId];
      if (node.attrs.css_class != "datastore") {
        var secondPlusDeg = [];
        var firstDeg = graph.outgoingNodesOf(nodeId);

        for (i in firstDeg) {
          var outNodeId = firstDeg[i];
          getOutgoingNodesRecursive(outNodeId, secondPlusDeg, graph, []);
        }

        for (var i in firstDeg) {
          var outNodeId = firstDeg[i];
          if (secondPlusDeg.indexOf(outNodeId) != -1) {
            graph.removeEdge(nodeId, outNodeId);
          }
        }
      }
    }
  },

  getNumSteps: function() {
    return  this.getSortedSteps().length;
  },

  getNumStepsRemaining: function() {
    var numStepsRemaining = 0;
    for (var i = 0; i < this.getNumSteps(); ++i) {
      var step = wfd.getSortedSteps()[i];
      if (step.status != 'completed' && step.status != 'skipped') {
        ++numStepsRemaining;
      }
    }
    return numStepsRemaining;
  },

  // For debugging

  stepIdsToString: function(stepIds) {
    var str = "";
    for (i in stepIds) {
      var stepId = stepIds[i];
      str += wfd.idToStep[stepId].name + "(" + stepId + ")";
      if (i != stepIds.length - 1) str += ", ";
    }
    return str;
  },

  nodeIdsToString: function(graph, nodeIds) {
    var str = "";
    for (i in nodeIds) {
      var nodeId = nodeIds[i];
      str += graph.nodes[nodeId].attrs.short_name + "(" + nodeId + ")";
      if (i != nodeIds.length - 1) str += ", ";
    }
    return str;
  }
}

