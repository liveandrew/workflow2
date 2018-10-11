package com.liveramp.workflow2.workflow_state.resources;

import java.io.IOException;

import com.liveramp.databases.workflow_db.IWorkflowDb;
import com.liveramp.databases.workflow_db.models.ResourceRoot;
import com.liveramp.cascading_ext.resource.RootDeterminer;

public class DbStorageRootDeterminer implements RootDeterminer<ResourceRoot> {

  private final IWorkflowDb rlDb;

  public DbStorageRootDeterminer(IWorkflowDb rlDb) {
    this.rlDb = rlDb;
  }

  @Override
  public synchronized ResourceRoot getResourceRoot(long version, String versionType) throws IOException {
    return BaseDbStorageRootDeterminer.getResourceRoot(version, versionType, rlDb);
  }
}
