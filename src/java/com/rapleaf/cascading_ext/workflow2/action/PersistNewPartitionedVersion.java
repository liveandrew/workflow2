package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TBase;

import com.liveramp.audience.generated.AudienceAndVersion;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.BucketDataStoreImpl;
import com.rapleaf.cascading_ext.datastore.PartitionedDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;


public class PersistNewPartitionedVersion<T extends TBase> extends Action {

  private final PartitionedDataStore destinationStore;
  private final PartitionedDataStore sourceStore;
  private final Class klass;

  public PersistNewPartitionedVersion(String checkpointToken, String tmpRoot, Class klass, PartitionedDataStore<T> sourceStore, PartitionedDataStore<T> destinationStore) throws IOException {
    super(checkpointToken, tmpRoot);

    this.destinationStore = destinationStore;
    this.sourceStore = sourceStore;
    this.klass = klass;
  }

  @Override
  protected void execute() throws Exception {
    destinationStore.persistFrom(sourceStore);
  }
}
