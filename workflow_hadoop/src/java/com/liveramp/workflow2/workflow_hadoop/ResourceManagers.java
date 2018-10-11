package com.liveramp.workflow2.workflow_hadoop;

import java.io.IOException;

import com.liveramp.cascading_ext.resource.InMemoryResourceManager;
import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.resource.ResourceDeclarerContainer;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.resource.RootManager;
import com.liveramp.databases.workflow_db.IWorkflowDb;
import com.liveramp.importer.generated.AppType;
import com.liveramp.workflow2.workflow_state.resources.DbResourceManager;
import com.liveramp.workflow2.workflow_state.resources.TransactorResourceManager;
import com.rapleaf.jack.transaction.ITransactor;

/**
 * All ResourceManagers returned by methods of this class use DbPersistence for checkpoints
 */
public class ResourceManagers {

  public static ResourceDeclarer defaultResourceManager() throws IOException {
    return TransactorResourceManager.create(new TransactorResourceManager.TransactorFactory.Default());
  }

  public static ResourceDeclarer defaultResourceManager(ITransactor<IWorkflowDb> transactor) throws IOException {
    return TransactorResourceManager.create(new TransactorResourceManager.TransactorFactory.Static(transactor));
  }

  @Deprecated
  public static ResourceDeclarer defaultResourceManager(IWorkflowDb rlDb) throws IOException {
    return DbResourceManager.create(rlDb);
  }

  @Deprecated
  public static ResourceDeclarer defaultResourceManager(String name, IWorkflowDb rlDb) throws IOException {
    return dbResourceManager(name, null, rlDb);
  }

  @Deprecated
  public static ResourceDeclarer defaultResourceManager(AppType appType, IWorkflowDb rlDb) throws IOException {
    return defaultResourceManager(appType.name(), rlDb);
  }

  @Deprecated
  public static ResourceDeclarer defaultResourceManager(Class classPassedIntoWorkflowRunner, IWorkflowDb rlDb) throws IOException {
    return defaultResourceManager(classPassedIntoWorkflowRunner.getName(), rlDb);
  }

  @Deprecated
  public static ResourceDeclarer dbResourceManager(String name, String scopeId, IWorkflowDb rlDb) throws IOException {
    return dbResourceManager();
  }

  public static ResourceDeclarer notImplemented() {
    return new ResourceManager.NotImplemented();
  }

  @Deprecated
  public static ResourceDeclarer dbResourceManager(IWorkflowDb rlDb) throws IOException {
    return DbResourceManager.create(rlDb);
  }

  public static ResourceDeclarer dbResourceManager() throws IOException {
    return TransactorResourceManager.create(new TransactorResourceManager.TransactorFactory.Default());
  }

  @Deprecated
  public static ResourceDeclarer dbResourceManager(AppType appType, String scopeId, IWorkflowDb rlDb) throws IOException {
    return dbResourceManager(appType.name(), scopeId, rlDb);
  }

  @Deprecated
  public static ResourceDeclarer dbResourceManager(Class classPassedIntoWorkflowRunner, String scopeId, IWorkflowDb rlDb) throws IOException {
    return dbResourceManager(classPassedIntoWorkflowRunner.getName(), scopeId, rlDb);
  }

  public static ResourceDeclarer hdfsResourceManager(String workflowRoot) throws IOException {
    return new ResourceDeclarerContainer<>(
        new ResourceDeclarerContainer.MethodNameTagger(),
        new RootManager<>(
            new HdfsStorageRootDeterminer(workflowRoot),
            new HdfsStorage.Factory())
    );
  }

  @Deprecated
  public static ResourceDeclarer hdfsResourceManager(String workflowRoot, String name, String scopeId, IWorkflowDb rlDb) throws IOException {
    return hdfsResourceManager(workflowRoot);
  }

  @Deprecated
  public static ResourceDeclarer hdfsResourceManager(String workflowRoot, AppType appType, String scopeId, IWorkflowDb rlDb) throws IOException {
    return hdfsResourceManager(workflowRoot, appType.name(), scopeId, rlDb);
  }

  @Deprecated
  public static ResourceDeclarer hdfsResourceManager(String workflowRoot, AppType appType, IWorkflowDb rlDb) throws IOException {
    return hdfsResourceManager(workflowRoot, appType.name(), null, rlDb);
  }

  @Deprecated
  public static ResourceDeclarer hdfsResourceManager(String workflowRoot, Class classPassedIntoWorkflowRunner, IWorkflowDb rlDb) throws IOException {
    return hdfsResourceManager(workflowRoot, classPassedIntoWorkflowRunner.getName(), null, rlDb);
  }

  public static ResourceDeclarer inMemoryResourceManager() throws IOException {
    return InMemoryResourceManager.create();
  }

}
