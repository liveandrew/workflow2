package com.rapleaf.cascading_ext.workflow2.state;

import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;
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
                                          String launchJar,
                                          Set<WorkflowRunnerNotification> configuredNotifications,
                                          AlertsHandler configuredHandler,
                                          String remote,
                                          String implementationBuild);
}
