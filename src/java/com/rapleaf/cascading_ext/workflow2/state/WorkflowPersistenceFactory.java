package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.liveramp.workflow_core.RunJarUtil;
import com.liveramp.workflow_core.WorkflowConstants;
import com.liveramp.workflow_state.IStep;
import com.liveramp.workflow_state.InitializedPersistence;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;

public abstract class WorkflowPersistenceFactory<INITIALIZED extends InitializedPersistence, OPTS extends BaseWorkflowOptions<OPTS>> {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowPersistenceFactory.class);

  public synchronized InitializedWorkflow<INITIALIZED, OPTS> initialize(OPTS options) throws IOException {
    return initialize(getName(options), options);
  }

  public InitializedWorkflow<INITIALIZED, OPTS> initialize(String workflowName, OPTS options) throws IOException {
    verifyName(workflowName, options);

    RunJarUtil.ScmInfo scmInfo = RunJarUtil.getRemoteAndCommit();

    final INITIALIZED initialized = initializeInternal(
        workflowName,
        options.getScopeIdentifier(),
        options.getDescription(),
        options.getAppType(),
        options.getHostnameProvider().getHostname(),
        System.getProperty("user.name"),
        findDefaultValue(options, WorkflowConstants.JOB_POOL_PARAM, "default"),
        findDefaultValue(options, WorkflowConstants.JOB_PRIORITY_PARAM, "NORMAL"),
        System.getProperty("user.dir"),
        RunJarUtil.getLaunchJarName(),
        options.getEnabledNotifications(),
        options.getAlertsHandler(),
        scmInfo.getGitRemote(),
        scmInfo.getRevision()
    );

    MultiShutdownHook hook = new MultiShutdownHook(workflowName);
    hook.add(new MultiShutdownHook.Hook() {
      @Override
      public void onShutdown() throws Exception {
        initialized.markWorkflowStopped();
      }
    });

    ResourceDeclarer resourceDeclarer = options.getResourceManager();

    ResourceManager resourceManager = resourceDeclarer.create(
        initialized.getExecutionId(),
        initialized.getClass().getName()
    );

    Runtime.getRuntime().addShutdownHook(hook);

    return new InitializedWorkflow<>(workflowName, options, initialized, this, resourceManager, hook);
  }


  public static String findDefaultValue(BaseWorkflowOptions options, String property, String defaultValue) {

    Object value = options.getConfiguredProperty(property);


    if (value != null) {
      return value.toString();
    }

    //  only really expect in tests
    return defaultValue;
  }


  public abstract INITIALIZED initializeInternal(String name,
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
                                                 AlertsHandler providedHandler,
                                                 String remote,
                                                 String implementationBuild) throws IOException;

  private void verifyName(String name, BaseWorkflowOptions options) {
    AppType appType = options.getAppType();
    if (appType != null) {
      if (!appType.name().equals(name)) {
        throw new RuntimeException("Workflow name cannot conflict with AppType name!");
      }
    } else {
      for (AppType a : AppType.values()) {
        if (a.name().equals(name)) {
          throw new RuntimeException("Provided workflow name " + name + " is already an AppType");
        }
      }
    }
  }

  private static String getName(BaseWorkflowOptions options) {
    AppType appType = options.getAppType();
    if (appType == null) {
      throw new RuntimeException("AppType must be set in WorkflowOptions!");
    }
    return appType.name();
  }


  public abstract WorkflowStatePersistence prepare(INITIALIZED persistence, DirectedGraph<IStep, DefaultEdge> flatSteps);
}
