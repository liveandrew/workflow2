package com.rapleaf.cascading_ext.workflow2.state;

import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.Step;

public interface WorkflowPersistenceFactory {
  public WorkflowStatePersistence prepare(DirectedGraph<Step, DefaultEdge> flatSteps,
                                          String name,
                                          String scopeId,
                                          String description,
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
