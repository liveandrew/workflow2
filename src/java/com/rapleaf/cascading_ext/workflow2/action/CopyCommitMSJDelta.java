package com.rapleaf.cascading_ext.workflow2.action;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.RapDistcp;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CopyCommitMSJDelta<RecordType, KeyType extends Comparable> extends Action {
  private final BucketDataStore<RecordType> versionToCommit;
  private final MSJDataStore<KeyType> store;

  public CopyCommitMSJDelta(String checkpointToken, BucketDataStore<RecordType> versionToCommit, MSJDataStore<KeyType> store) {
    super(checkpointToken);

    this.versionToCommit = versionToCommit;
    this.store = store;

    readsFrom(versionToCommit);
    writesTo(store);
  }

  @Override
  protected void execute() throws Exception {
    String tmpStorePath = FileSystemHelper.getRandomTemporaryPath().toString();
    RapDistcp.distcp(versionToCommit.getPath(), new String[] {}, tmpStorePath);
    store.commitDelta(tmpStorePath);
  }
}
