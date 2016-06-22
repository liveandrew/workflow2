package com.liveramp.cascading_ext.action;


import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.team_metadata.paths.hdfs.TeamTmpDir;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class DeleteDataStore extends Action {

  private final FileSystem fs;
  private final List<? extends DataStore> dataStores;
  private static Logger LOG = LoggerFactory.getLogger(DeleteDataStore.class);

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

    boolean isTmp = false;

    for (TeamTmpDir dir : TeamTmpDir.values()) {
      isTmp |= dataStore.getPath().startsWith(dir.getTmpDir());
    }

    if (!isTmp) {
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
      LOG.info("Deleting " + dataStore.getPath());
      fs.delete(new Path(dataStore.getPath()), true);
    }
  }
}
