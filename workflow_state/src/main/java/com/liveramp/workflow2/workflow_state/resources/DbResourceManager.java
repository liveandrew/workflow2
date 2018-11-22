package com.liveramp.workflow2.workflow_state.resources;

import java.io.IOException;

import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.resource.ResourceDeclarerContainer;
import com.liveramp.cascading_ext.resource.ResourceDeclarerFactory;
import com.liveramp.cascading_ext.resource.RootManager;
import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.liveramp.databases.workflow_db.IWorkflowDb;

public class DbResourceManager {

  public static class Factory implements ResourceDeclarerFactory {
    @Override
    public ResourceDeclarer create() throws IOException {
      return DbResourceManager.create(new WorkflowDbFactory.Default());
    }
  }

  public static ResourceDeclarer create(WorkflowDbFactory factory) throws IOException {
    return new ResourceDeclarerContainer<>(
        new ResourceDeclarerContainer.MethodNameTagger(),
        new RootManager<>(
            new DbStorageRootDeterminer(factory.create()),
            dbStorage(factory))
    );
  }

  public static ResourceDeclarer create() throws IOException {
    return create(new WorkflowDbFactory.Default());
  }

  @Deprecated
  public static ResourceDeclarer create(IWorkflowDb rlDb) throws IOException {
    return new ResourceDeclarerContainer<>(
        new ResourceDeclarerContainer.MethodNameTagger(),
        new RootManager<>(
            new DbStorageRootDeterminer(rlDb),
            dbStorage(rlDb))
    );
  }

  public static DbStorage.Factory dbStorage(WorkflowDbFactory factory) {
    return new DbStorage.Factory(factory);
  }


  @Deprecated
  public static DbStorage.Factory dbStorage(IWorkflowDb rlDb) {
    return new DbStorage.Factory(new WorkflowDbFactory.Static(rlDb));
  }

  public interface WorkflowDbFactory {
    IWorkflowDb create();

    class Default implements WorkflowDbFactory {
      @Override
      public IWorkflowDb create() {
        IWorkflowDb rldb = new DatabasesImpl().getWorkflowDb();
        rldb.disableCaching();
        return rldb;
      }
    }

    @Deprecated
    class Static implements WorkflowDbFactory {

      private final IWorkflowDb rldb;

      public Static(IWorkflowDb rldb) {
        this.rldb = rldb;
      }

      @Override
      public IWorkflowDb create() {
        return rldb;
      }
    }

  }
}
