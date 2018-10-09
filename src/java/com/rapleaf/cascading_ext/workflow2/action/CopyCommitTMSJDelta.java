package com.rapleaf.cascading_ext.workflow2.action;

import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBase;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.TMSJDataStore;

public class CopyCommitTMSJDelta<T extends TBase<?, ?>> extends CopyCommitMSJDelta<T, BytesWritable> {
  public CopyCommitTMSJDelta(String checkpointToken, String tmpDir, BucketDataStore<T> versionToPersist, TMSJDataStore<T> store) {
    super(checkpointToken, tmpDir, versionToPersist, store);
  }
}
