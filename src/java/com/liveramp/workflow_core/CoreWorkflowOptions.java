package com.liveramp.workflow_core;

import java.util.Map;

import com.google.common.collect.Maps;

import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.rapleaf.support.Rap;

public class CoreWorkflowOptions extends BaseWorkflowOptions<CoreWorkflowOptions> {

  protected CoreWorkflowOptions(OverridableProperties defaultProperties) {
    super(defaultProperties);
  }

  protected CoreWorkflowOptions(OverridableProperties defaultProperties, Map<Object, Object> systemProperties) {
    super(defaultProperties, systemProperties);
  }

  public static CoreWorkflowOptions test() {
    Rap.assertTest();

    CoreWorkflowOptions opts = new CoreWorkflowOptions(new NestedProperties(Maps.newHashMap(), false));
    configureTest(opts);
    return opts;

  }

  public static BaseWorkflowOptions production() {
    CoreWorkflowOptions opts = new CoreWorkflowOptions(new NestedProperties(Maps.newHashMap(), false));
    configureProduction(opts);
    return opts;
  }

}
