package com.rapleaf.cascading_ext.workflow2.options;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;

import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.megadesk.MockStoreReaderLockProvider;
import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.liveramp.workflow.backpressure.FlowSubmissionController;
import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.liveramp.workflow_core.CoreOptions;
import com.rapleaf.support.Rap;

public class HadoopWorkflowOptions extends BaseWorkflowOptions<HadoopWorkflowOptions> {

  private StoreReaderLockProvider lockProvider;
  private FlowSubmissionController flowSubmissionController;
  private CascadingUtil cascadingUtil;

  protected HadoopWorkflowOptions(OverridableProperties defaultProperties,
                                  Map<Object, Object> systemProperties,
                                  CascadingUtil cascadingUtil) {
    super(defaultProperties, systemProperties);

    this.flowSubmissionController = new FlowSubmissionController.SubmitImmediately();
    this.cascadingUtil = cascadingUtil;
  }

  public static Map<Object, Object> toProperties(JobConf conf) {
    Map<Object, Object> props = Maps.newHashMap();
    for (Map.Entry<String, String> entry : conf) {
      props.put(entry.getKey(), entry.getValue());
    }
    return props;
  }

  public CascadingUtil getCascadingUtil() {
    return cascadingUtil;
  }

  public StoreReaderLockProvider getLockProvider() {
    return lockProvider;
  }

  public HadoopWorkflowOptions setLockProvider(StoreReaderLockProvider lockProvider) {
    this.lockProvider = lockProvider;
    return this;
  }

  public HadoopWorkflowOptions setFlowSubmissionController(FlowSubmissionController flowSubmissionController) {
    this.flowSubmissionController = flowSubmissionController;
    return this;
  }

  public FlowSubmissionController getFlowSubmissionController() {
    return flowSubmissionController;
  }


  protected static void configureTest(HadoopWorkflowOptions options) {
    Rap.assertTest();

    CoreOptions.configureTest(options);

    options
        .setLockProvider(new MockStoreReaderLockProvider())
        .addWorkflowProperties(Collections.<Object, Object>singletonMap(MRJobConfig.QUEUE_NAME, "test"))
        .setFlowSubmissionController(new FlowSubmissionController.SubmitImmediately());

  }

  public static HadoopWorkflowOptions test() {
    HadoopWorkflowOptions opts = new HadoopWorkflowOptions(
        new NestedProperties(),
        Collections.emptyMap(),
        CascadingUtil.get());
    configureTest(opts);
    return opts;
  }


}
