package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import com.liveramp.commons.util.serialization.JavaObjectSerializationHandler;
import com.liveramp.commons.util.serialization.SerializationHandler;
import com.liveramp.java_support.workflow.ActionId;
import com.rapleaf.support.Rap;

public class InMemoryContext extends ContextStorage {

  private final Map<String, byte[]> resourceMap = Maps.newHashMap();
  private final SerializationHandler handler;

  public InMemoryContext() {
    Rap.assertTest();
    this.handler = new JavaObjectSerializationHandler();
  }

  @Override
  public <T> void set(Resource<T> ref, T value) throws IOException {
    resourceMap.put(getPath(ref), handler.serialize(value));
  }

  private <T> String getPath(Resource<T> ref) {
    StringBuilder location = new StringBuilder();
    ActionId parent = ref.getParent();

    if (parent != null) {
      location.append(parent.resolve());
    }

    return location
        .append(ref.getRelativeId())
        .toString();
  }

  @Override
  public <T> T get(Resource<T> ref) throws IOException {
    if (resourceMap.containsKey(getPath(ref))) {
      return (T)handler.deserialize(resourceMap.get(getPath(ref)));
    } else {
      return null;
    }
  }

  public <T> Resource<T> getAndSet(String globalResource, T obj) throws IOException {
    Resource<T> res = new Resource<T>(globalResource, null);
    set(res, obj);
    return res;
  }

  //  make public to expose to tests
  public <T> Resource<T> create(String name, ActionId actionId) {
    return new Resource<T>(name, actionId);
  }

}
