package com.liveramp.cascading_ext.resource;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;

public interface ResourceDeclarer<ID, RESOURCE_ROOT> {

  ResourceManager<ID, RESOURCE_ROOT> create(long version, String versionType) throws IOException;

  <T> T manage(T context);

  <T> Resource<T> emptyResource(String name);

  <T> ReadResource<T> getReadPermission(Resource<T> resource);

  <T> WriteResource<T> getWritePermission(Resource<T> resource);


  public static class NotImplemented implements ResourceDeclarer<Void, Void> {


    @Override
    public ResourceManager<Void, Void> create(long version, String versionType) {
      return new ResourceManager.NotImplemented();
    }

    @Override
    public <T> T manage(T context) {
      throw new NotImplementedException();
    }

    @Override
    public <T> Resource<T> emptyResource(String name) {
      throw new NotImplementedException();
    }

    @Override
    public <T> ReadResource<T> getReadPermission(Resource<T> resource) {
      throw new NotImplementedException();
    }

    @Override
    public <T> WriteResource<T> getWritePermission(Resource<T> resource) {
      throw new NotImplementedException();
    }

  }

}
