package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.mapred.JobConf;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.util.HadoopJarUtil;
import com.liveramp.cascading_ext.util.HadoopProperties;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_state.InitializedPersistence;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;

import static com.rapleaf.cascading_ext.workflow2.WorkflowRunner.JOB_POOL_PARAM;
import static com.rapleaf.cascading_ext.workflow2.WorkflowRunner.JOB_PRIORITY_PARAM;

public abstract class WorkflowPersistenceFactory<INITIALIZED extends InitializedPersistence> {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowPersistenceFactory.class);

  public synchronized InitializedWorkflow<INITIALIZED> initialize(WorkflowOptions options) throws IOException {
    return initialize(getName(options), options);
  }

  public InitializedWorkflow<INITIALIZED> initialize(String workflowName, WorkflowOptions options) throws IOException {
    verifyName(workflowName, options);

    HadoopJarUtil.ScmInfo scmInfo = HadoopJarUtil.getRemoteAndCommit();

    HadoopProperties properties = options.getWorkflowJobProperties();

    final INITIALIZED initialized = initializeInternal(
        workflowName,
        options.getScopeIdentifier(),
        options.getDescription(),
        options.getAppType(),
        options.getHostnameProvider().getHostname(),
        System.getProperty("user.name"),
        findDefaultValue(properties, JOB_POOL_PARAM, "default"),
        findDefaultValue(properties, JOB_PRIORITY_PARAM, "NORMAL"),
        System.getProperty("user.dir"),
        HadoopJarUtil.getLaunchJarName(),
        options.getEnabledNotifications(),
        options.getAlertsHandler(),
        scmInfo.getGitRemote(),
        scmInfo.getRevision()
    );

    MultiShutdownHook hook = new MultiShutdownHook(workflowName);
    hook.add(new MultiShutdownHook.Hook() {
      @Override
      public void onShutdown() throws Exception {
        initialized.stop();
      }
    });

    Runtime.getRuntime().addShutdownHook(hook);

    return new InitializedWorkflow<>(workflowName, options, initialized, this, hook);
  }


  public static String findDefaultValue(HadoopProperties properties, String property, String defaultValue) {

    //  fall back to static jobconf props if not set elsewhere
    JobConf jobconf = CascadingHelper.get().getJobConf();

    if (properties.containsKey(property)) {
      return (String)properties.get(property);
    }

    String value = jobconf.get(property);

    if (value != null) {
      return value;
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

  private void verifyName(String name, WorkflowOptions options) {
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

  private static String getName(WorkflowOptions options) {
    AppType appType = options.getAppType();
    if (appType == null) {
      throw new RuntimeException("AppType must be set in WorkflowOptions!");
    }
    return appType.name();
  }


  public abstract WorkflowStatePersistence prepare(INITIALIZED persistence, DirectedGraph<Step, DefaultEdge> flatSteps);
}
