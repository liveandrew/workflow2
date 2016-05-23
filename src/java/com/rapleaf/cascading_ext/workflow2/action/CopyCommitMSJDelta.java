package com.rapleaf.cascading_ext.workflow2.action;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CopyCommitMSJDelta<RecordType, KeyType extends Comparable> extends Action {
  private final BucketDataStore<RecordType> versionToCommit;
  private final MSJDataStore<KeyType> store;

  public CopyCommitMSJDelta(String checkpointToken, String tmpDir, BucketDataStore<RecordType> versionToCommit, MSJDataStore<KeyType> store) {
    super(checkpointToken, tmpDir);

    this.versionToCommit = versionToCommit;
    this.store = store;

    readsFrom(versionToCommit);
    writesTo(store);
  }

  @Override
  protected void execute() throws Exception {
    String tmpStorePath = FileSystemHelper.getRandomTemporaryPath().toString();
    versionToCommit.getBucket().snapshot(tmpStorePath, getTmpRoot()+"/snapshot_tmp");
    store.commitDelta(tmpStorePath);
  }
}
