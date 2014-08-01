package com.liveramp.cascading_ext.action;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class DeleteDataStore extends Action {

  private final FileSystem fs;
  private final DataStore dataStore;

  public DeleteDataStore(String checkpointToken, FileSystem fs, DataStore dataStore) {
    super(checkpointToken);

    if (!dataStore.getPath().startsWith("/tmp/")) {
      throw new IllegalArgumentException("DeleteDataStore should only be used for temporary stores");
    }

    this.fs = fs;
    this.dataStore = dataStore;
  }

  public DeleteDataStore(String checkpointToken, DataStore dataStore) {
    this(checkpointToken, FileSystemHelper.getFS(), dataStore);
  }

  @SuppressWarnings("PMD.BlacklistedMethods")
  @Override
  protected void execute() throws Exception {
    fs.delete(new Path(dataStore.getPath()), true);
  }
}
