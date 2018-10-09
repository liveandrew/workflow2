package com.rapleaf.cascading_ext.workflow2.action;

import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBase;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.TMSJDataStore;


public class CommitTMSJDelete<T extends TBase<?, ?>> extends CommitMSJDelete<T, BytesWritable> {
  public CommitTMSJDelete(String checkpointToken, BucketDataStore<T> versionToPersist, TMSJDataStore<T> store) {
    super(checkpointToken, versionToPersist, store);
  }
}
