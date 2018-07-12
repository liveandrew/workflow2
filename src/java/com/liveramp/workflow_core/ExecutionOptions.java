package com.liveramp.workflow_core;

import java.util.Map;

import com.google.common.collect.Maps;

import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.rapleaf.support.Rap;

public class ExecutionOptions extends BaseWorkflowOptions<ExecutionOptions> {

  protected ExecutionOptions(OverridableProperties defaultProperties) {
    super(defaultProperties);
  }

  protected ExecutionOptions(OverridableProperties defaultProperties, Map<Object, Object> systemProperties) {
    super(defaultProperties, systemProperties);
  }

  public static ExecutionOptions production() {
    ExecutionOptions opts = new ExecutionOptions(new NestedProperties(Maps.newHashMap(), false));
    configureProduction(opts);
    return opts;
  }

  public static ExecutionOptions test() {
    Rap.assertTest();

    ExecutionOptions opts = new ExecutionOptions(new NestedProperties(Maps.newHashMap(), false));
    configureTest(opts);
    return opts;
  }

}
