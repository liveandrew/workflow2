package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.RapDistcp;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.support.collections.Iteratorable;


public class CopyAndPersistNewVersion<T> extends Action {

  private final BucketDataStore<T> versionToPersist;
  private final VersionedBucketDataStore<T> store;
  private final BucketDataStore<T> tmpCopy;

  public CopyAndPersistNewVersion(String checkpointToken, String tmpRoot, BucketDataStore<T> versionToPersist, VersionedBucketDataStore<T> store) throws IOException {
    super(checkpointToken, tmpRoot);

    this.versionToPersist = versionToPersist;
    this.store = store;
    tmpCopy = builder().getBucketDataStore("tmp_copy", versionToPersist.getRecordsType());

    readsFrom(versionToPersist);
    createsTemporary(tmpCopy);
    writesTo(store);
  }

  @Override
  protected void execute() throws Exception {

    tmpCopy.getBucket().copyAppend(versionToPersist.getBucket(), true, new HashMap<>(), getTmpRoot());

    Bucket newVersion = store.getBucketVersionedStore().openNewVersion();
    newVersion.absorbIntoEmpty(tmpCopy.getBucket());

    store.getBucketVersionedStore().completeVersion(newVersion);
  }
}
