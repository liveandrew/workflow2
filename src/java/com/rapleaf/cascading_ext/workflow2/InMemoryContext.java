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
  public <T> void set(OldResource<T> ref, T value) throws IOException {
    resourceMap.put(getPath(ref), handler.serialize(value));
  }

  private <T> String getPath(OldResource<T> ref) {
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
  public <T> T get(OldResource<T> ref) throws IOException {
    if (resourceMap.containsKey(getPath(ref))) {
      return (T)handler.deserialize(resourceMap.get(getPath(ref)));
    } else {
      return null;
    }
  }

  public <T> OldResource<T> getAndSet(String globalResource, T obj) throws IOException {
    OldResource<T> res = new OldResource<T>(globalResource, null);
    set(res, obj);
    return res;
  }

  //  make public to expose to tests
  public <T> OldResource<T> create(String name, ActionId actionId) {
    return new OldResource<T>(name, actionId);
  }

}
