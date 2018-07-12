package com.liveramp.workflow_core;

import java.util.Map;

import com.google.common.collect.Maps;

import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.rapleaf.support.Rap;

public class ConcreteOptions extends BaseWorkflowOptions<ConcreteOptions> {

  protected ConcreteOptions(OverridableProperties defaultProperties) {
    super(defaultProperties);
  }

  protected ConcreteOptions(OverridableProperties defaultProperties, Map<Object, Object> systemProperties) {
    super(defaultProperties, systemProperties);
  }

  public static ConcreteOptions production() {
    ConcreteOptions opts = new ConcreteOptions(new NestedProperties(Maps.newHashMap(), false));
    configureProduction(opts);
    return opts;
  }

  public static ConcreteOptions test() {
    Rap.assertTest();

    ConcreteOptions opts = new ConcreteOptions(new NestedProperties(Maps.newHashMap(), false));
    configureTest(opts);
    return opts;
  }

}
