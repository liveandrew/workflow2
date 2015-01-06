package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

public abstract class ContextStorage {

  public abstract <T> void set(Resource<T> ref, T value) throws IOException;

  public abstract<T> T get(Resource<T> ref) throws IOException, ClassNotFoundException;


  public static class None extends ContextStorage{

    @Override
    public <T> void set(Resource<T> ref, T value) {
      throw new RuntimeException("Must set a real implementation in ProductionWorkflowOptions to use resources!");
    }

    @Override
    public <T> T get(Resource<T> ref) {
      throw new RuntimeException("Must set a real implementation in ProductionWorkflowOptions to use resources!");
    }

  }

  /**
   * helper mainly for tests
   */
  public <T> Resource<T> set(String resourceName, T value) throws IOException {
    Resource<T> res = new Resource<T>(resourceName);
    set(res, value);
    return res;
  }

}
