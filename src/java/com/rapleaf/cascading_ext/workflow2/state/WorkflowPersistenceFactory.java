package com.rapleaf.cascading_ext.workflow2.state;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.importer.generated.AppType;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowStatePersistence;

public interface WorkflowPersistenceFactory {
  public WorkflowStatePersistence prepare(DirectedGraph<Step, DefaultEdge> flatSteps,
                                          String name,
                                          String scopeId,
                                          AppType appType,
                                          String host,
                                          String username,
                                          String pool,
                                          String priority,
                                          String launchDir,
                                          String launchJar);
}
