package com.rapleaf.cascading_ext.workflow2.options;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;

import com.liveramp.cascading_ext.megadesk.MockStoreReaderLockProvider;
import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.cascading_tools.properties.PropertiesUtil;
import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.workflow.backpressure.FlowSubmissionController;
import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.support.Rap;

public class WorkflowOptions extends BaseWorkflowOptions<WorkflowOptions> {

  private StoreReaderLockProvider lockProvider;
  private FlowSubmissionController flowSubmissionController;

  protected WorkflowOptions() {
    super(CascadingHelper.get().getDefaultHadoopProperties(), toProperties(CascadingHelper.get().getJobConf()));
    flowSubmissionController = new FlowSubmissionController.SubmitImmediately();
  }

  protected WorkflowOptions(OverridableProperties defaultProperties,
                            Map<Object, Object> systemProperties) {
    super(defaultProperties, systemProperties);
  }

  private static Map<Object, Object> toProperties(JobConf conf) {
    Map<Object, Object> props = Maps.newHashMap();
    for (Map.Entry<String, String> entry : conf) {
      props.put(entry.getKey(), entry.getValue());
    }
    return props;
  }

  public WorkflowOptions configureTeam(TeamList team, String subPool) {
    addWorkflowProperties(PropertiesUtil.teamPool(team, subPool));
    setAlertsHandler(team);
    return this;
  }

  public StoreReaderLockProvider getLockProvider() {
    return lockProvider;
  }

  public WorkflowOptions setLockProvider(StoreReaderLockProvider lockProvider) {
    this.lockProvider = lockProvider;
    return this;
  }


  //  static helpers

  public static WorkflowOptions production() {
    WorkflowOptions opts = new WorkflowOptions();
    configureProduction(opts);
    return opts;
  }

  public static WorkflowOptions test() {
    WorkflowOptions opts = new WorkflowOptions();
    configureTest(opts);
    return opts;
  }

  public static WorkflowOptions emrProduction() {

    Map<Object, Object> defaultProperties = CascadingHelper.get().getDefaultProperties();
    defaultProperties.remove(MRJobConfig.MR_AM_COMMAND_OPTS); //  TODO hacky

    WorkflowOptions opts = new WorkflowOptions(
        new NestedProperties(defaultProperties, false),
        toProperties(CascadingHelper.get().getJobConf())
    );

    configureProduction(opts);
    return opts;
  }

  protected static void configureProduction(WorkflowOptions options) {
    Rap.assertProduction();

    BaseWorkflowOptions.configureProduction(options);

    options
        .setLockProvider(new MockStoreReaderLockProvider())
        .setFlowSubmissionController(new FlowSubmissionController.SubmitImmediately());
  }


  protected static void configureTest(WorkflowOptions options) {
    Rap.assertTest();

    BaseWorkflowOptions.configureTest(options);

    options
        .setLockProvider(new MockStoreReaderLockProvider())
        .addWorkflowProperties(Collections.<Object, Object>singletonMap(MRJobConfig.QUEUE_NAME, "test"))
        .setFlowSubmissionController(new FlowSubmissionController.SubmitImmediately());

  }

  public WorkflowOptions setFlowSubmissionController(FlowSubmissionController flowSubmissionController) {
    this.flowSubmissionController = flowSubmissionController;
    return this;
  }

  public FlowSubmissionController getFlowSubmissionController() {
    return flowSubmissionController;
  }
}
