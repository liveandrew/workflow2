package com.rapleaf.cascading_ext.workflow2;

import com.liveramp.java_support.workflow.ActionId;

public class ResourceFactory {

  private final ActionId actionId;

  ResourceFactory(ActionId actionId) {
    this.actionId = actionId;
  }

  public <T> OldResource<T> makeResource(String name){
    return new OldResource<T>(name, actionId);
  }

}
