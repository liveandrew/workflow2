package com.liveramp.cascading_ext.megadesk;

import java.io.IOException;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.DateVersionedBucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.formats.datastore.VersionedStore;

public abstract class StoreReaderLockProvider {


  public ResourceSemaphore createLock(DataStore store) {
    if (store instanceof VersionedBucketDataStore) {
      return createLock((VersionedBucketDataStore)store);
    } else if (store instanceof DateVersionedBucketDataStore) {
      return createLock((DateVersionedBucketDataStore)store);
    } else {
      return createLock(store.getPath());
    }
  }

  public abstract ResourceSemaphore createLock(String path);

   ResourceSemaphore createLock(VersionedBucketDataStore store) {
    try {
      return createLock(store.getLatestVersion().getPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

   ResourceSemaphore createLock(DateVersionedBucketDataStore store) {
    try {
      return createLock(store.getLatestVersion().getPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public ResourceSemaphore createLock(DateVersionedBucketDataStore store, long version) {

    try {
      return createLock(store.getStoreForVersion(version).getPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public ResourceSemaphore createLock(VersionedBucketDataStore store, long version) {
    try {
      return createLock(store.getStoreForVersion(version).getPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public ResourceSemaphore createLock(VersionedStore store, long version) {
    String versionPath = store.getVersionPath(version);
    return createLock(versionPath);
  }

  public ResourceSemaphore createLock(VersionedStore store) {
    String versionPath = null;
    try {
      versionPath = store.getVersionPath(store.getMostRecentVersion());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return createLock(versionPath);
  }
}
