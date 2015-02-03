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

}
