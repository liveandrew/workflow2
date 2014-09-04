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
  private final Map<Long, Integer> audienceIdToVersion;
  private final String newVersionsRoot;
  private final Class klass;

  public PersistNewPartitionedVersion(String checkpointToken, String newVersionsRoot, Class klass, PartitionedDataStore<T> destinationStore, Map<Long, Integer> audienceIdToVersion) throws IOException {
    super(checkpointToken);

    this.destinationStore = destinationStore;
    this.audienceIdToVersion = audienceIdToVersion;
    this.newVersionsRoot = newVersionsRoot;
    this.klass = klass;
  }

  @Override
  protected void execute() throws Exception {
    Map<AudienceAndVersion, BucketDataStore<T>> versionsToPersist = new HashMap<AudienceAndVersion, BucketDataStore<T>>();

    for (Map.Entry<Long, Integer> entry : audienceIdToVersion.entrySet()) {
      Long audienceId = entry.getKey();
      Integer audienceVersion = entry.getValue();
      String path = newVersionsRoot + "/" + audienceId.toString();
      BucketDataStore<T> sourceBucket = new BucketDataStoreImpl<T>(path, klass);
      versionsToPersist.put(new AudienceAndVersion(audienceId, audienceVersion), sourceBucket);
    }

    destinationStore.persistNewAudienceVersions(versionsToPersist);
  }
}
