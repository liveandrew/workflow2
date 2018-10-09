package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBase;

public class CommitTMSJBase<T extends TBase<?, ?>> extends CommitMSJBase<T, BytesWritable> {
  public CommitTMSJBase(String checkpointToken, BucketDataStore<T> baseToCommit, MSJDataStore<BytesWritable> store) {
    super(checkpointToken, baseToCommit, store);
  }
}
