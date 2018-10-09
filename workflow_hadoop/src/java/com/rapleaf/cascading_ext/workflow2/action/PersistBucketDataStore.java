package com.rapleaf.cascading_ext.workflow2.action;

import org.apache.thrift.TBase;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class PersistBucketDataStore<T extends TBase<?, ?>> extends Action {

  private final BucketDataStore<T> from;
  private final BucketDataStore<T> to;

  public PersistBucketDataStore(String checkpointToken, BucketDataStore<T> from, BucketDataStore<T> to) {
    super(checkpointToken);
    this.from = from;
    this.to = to;

    readsFrom(from);
    creates(to);
  }

  @Override
  protected void execute() throws Exception {
    setStatusMessageSafe(String.format("Persisting %s to %s", from.getPath(), to.getPath()));
    to.getBucket().absorbIntoEmpty(from.getBucket());
  }
}
