package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DateVersionedBucketDataStore;
import com.rapleaf.cascading_ext.state.TypedState;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.support.DayOfYear;

public class PersistNewDatedVersion<E extends Enum<E>, T> extends Action {

  private final BucketDataStore<T> newVersion;
  private final DateVersionedBucketDataStore<T> versionedStore;
  private final TypedState<E, DayOfYear> state;
  private final E versionField;

  public PersistNewDatedVersion(String checkpointToken,
                                E versionField,
                                TypedState<E, DayOfYear> state,
                                BucketDataStore<T> newVersion,
                                DateVersionedBucketDataStore<T> versionedStore) {
    super(checkpointToken);

    this.newVersion = newVersion;
    this.versionedStore = versionedStore;
    this.state = state;
    this.versionField = versionField;

    consumes(newVersion);
    writesTo(versionedStore);
  }

  @Override
  protected void execute() throws Exception {
    String version = versionedStore.openNewVersion(state.get(versionField));
    Bucket versionBucket = Bucket.create(getFS(), version, newVersion.getRecordsType());
    versionBucket.absorb(newVersion.getBucket());
    if (newVersion.getBucket().isImmutable()) {
      versionBucket.markAsImmutable();
    }
    versionedStore.completeVersion(version);
  }
}
