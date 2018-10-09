package com.liveramp.workflow_core;

import java.io.IOException;

public abstract class ContextStorage {

  public abstract <T> void set(OldResource<T> ref, T value) throws IOException;

  public abstract<T> T get(OldResource<T> ref) throws IOException;


  public static class None extends ContextStorage{

    @Override
    public <T> void set(OldResource<T> ref, T value) {
      throw new RuntimeException("Must set a real implementation in ProductionWorkflowOptions to use resources!");
    }

    @Override
    public <T> T get(OldResource<T> ref) {
      throw new RuntimeException("Must set a real implementation in ProductionWorkflowOptions to use resources!");
    }

  }

}
