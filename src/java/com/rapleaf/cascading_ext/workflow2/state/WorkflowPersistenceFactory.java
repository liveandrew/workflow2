package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.resource.ResourceDeclarerContainer;
import com.liveramp.cascading_ext.resource.ResourceDeclarerFactory;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.liveramp.java_support.RunJarUtil;
import com.liveramp.workflow_core.StepStateManager;
import com.liveramp.workflow_core.WorkflowConstants;
import com.liveramp.workflow_core.info.WorkflowInfo;
import com.liveramp.workflow_core.info.WorkflowInfoConsumer;
import com.liveramp.workflow_core.info.WorkflowInfoImpl;
import com.liveramp.workflow_state.IStep;
import com.liveramp.workflow_state.InitializedPersistence;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;

public abstract class WorkflowPersistenceFactory<
    S extends IStep,
    INITIALIZED extends InitializedPersistence,
    OPTS extends BaseWorkflowOptions<OPTS>,
    WORKFLOW extends InitializedWorkflow<S, INITIALIZED, OPTS>> {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowPersistenceFactory.class);

  private final StepStateManager<S> manager;

  protected WorkflowPersistenceFactory(StepStateManager<S> manager) {
    this.manager = manager;
  }

  protected StepStateManager<S> getManager() {
    return manager;
  }

  public synchronized WORKFLOW initialize(OPTS options) throws IOException {
    return initialize(getName(options), options);
  }


  public WORKFLOW initialize(String workflowName, OPTS options) throws IOException {
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
        options.getResourceManagerFactory(),
        scmInfo.getGitRemote(),
        scmInfo.getRevision()
    );

    MultiShutdownHook hook = new MultiShutdownHook(workflowName);

    if(manager.isLive()) {
      hook.add(new MultiShutdownHook.Hook() {
        @Override
        public void onShutdown() throws Exception {
          LOG.info("Invoking workflow shutdown hook");
          initialized.markWorkflowStopped();
        }
      });
    }

    ResourceDeclarer resourceDeclarer = options.getResourceManager();

    ResourceManager resourceManager = resourceDeclarer.create(
        initialized.getExecutionId(),
        initialized.getClass().getName()
    );

    WorkflowInfo info = new WorkflowInfoImpl(initialized, options);
    for (WorkflowInfoConsumer workflowInfoConsumer : options.getWorkflowInfoConsumers()) {
      workflowInfoConsumer.accept(info);
    }

    Runtime.getRuntime().addShutdownHook(hook);

    return construct(workflowName, options, initialized, resourceManager, hook);
  }

  //  force implementers to override to avoid generics overload
  public abstract WORKFLOW construct(String workflowName,
                                        OPTS options,
                                        INITIALIZED initialized,
                                        ResourceManager manager,
                                        MultiShutdownHook hook);

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
                                                 Class<? extends ResourceDeclarerFactory> resourceFactory,
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

  public abstract WorkflowStatePersistence prepare(INITIALIZED persistence, DirectedGraph<S, DefaultEdge> flatSteps);

}
