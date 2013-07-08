package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DateVersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.support.DayOfYear;

public class PersistSpecificDatedVersion<T> extends Action {

  private final BucketDataStore<T> newVersion;
  private final DateVersionedBucketDataStore<T> versionedStore;
  private final long version;

  public PersistSpecificDatedVersion(String checkpointToken,
                                long version,
                                BucketDataStore<T> newVersion,
                                DateVersionedBucketDataStore<T> versionedStore) {
    super(checkpointToken);

    this.newVersion = newVersion;
    this.versionedStore = versionedStore;
    this.version = version;

    readsFrom(newVersion);
    writesTo(versionedStore);
  }

  @Override
  protected void execute() throws Exception {
    String version = versionedStore.openNewVersion(new DayOfYear(this.version));
    Bucket versionBucket = Bucket.create(getFS(), version);
    versionBucket.absorb(newVersion.getBucket());
    if (newVersion.getBucket().isImmutable()) {
      versionBucket.markAsImmutable();
    }
    versionedStore.completeVersion(version);
  }
}
