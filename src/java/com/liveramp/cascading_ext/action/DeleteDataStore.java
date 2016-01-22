package com.liveramp.cascading_ext.action;


import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class DeleteDataStore extends Action {

  private final FileSystem fs;
  private final List<? extends DataStore> dataStores;

  public DeleteDataStore(String checkpointToken, FileSystem fs, List<? extends DataStore> dataStores) {
    super(checkpointToken);

    this.fs = fs;
    this.dataStores = dataStores;

    for (DataStore dataStore : dataStores) {
      readsFrom(dataStore);
    }
  }

  public DeleteDataStore(String checkpointToken, FileSystem fs, DataStore dataStore) {
    super(checkpointToken);

    if (!dataStore.getPath().startsWith("/tmp/")) {
      throw new IllegalArgumentException("DeleteDataStore should only be used for temporary stores");
    }

    this.fs = fs;
    this.dataStores = Lists.newArrayList(dataStore);

    readsFrom(dataStore);
  }

  public DeleteDataStore(String checkpointToken, DataStore dataStore) {
    this(checkpointToken, FileSystemHelper.getFS(), dataStore);
  }

  @SuppressWarnings("PMD.BlacklistedMethods")
  @Override
  protected void execute() throws Exception {
    for (DataStore dataStore : dataStores) {
      fs.delete(new Path(dataStore.getPath()), true);
    }
  }
}
