package com.liveramp.cascading_ext.megadesk;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.formats.datastore.VersionedStore;

public abstract class StoreReaderLocker {

  private static final Logger LOG = LoggerFactory.getLogger(StoreReaderLockProvider.class);

  public static final Set<Class<? extends DataStore>> CONSUME_LOCK_STORES = Sets.<Class<? extends DataStore>>newHashSet(
      VersionedBucketDataStore.class
  );

  public interface Factory {
    public StoreReaderLockProvider create();
  }

  public class LockManager {

    private final List<DataStore> lockOnConsume = Lists.newArrayList();
    private final List<DataStore> lockOnStart = Lists.newArrayList();

    private final List<ResourceSemaphore> heldLocks = Lists.newArrayList();

    private LockManager(Collection<DataStore> datastores) {
      for (DataStore datastore : datastores) {
        //  I don't think there's any way to avoid iterating over both here, without some kinda tree structure to see
        //  if the class implements any of the interfaces in the set.  which would be slighly over-engineered for a set of 1.
        for (Class<? extends DataStore> store : CONSUME_LOCK_STORES) {
          if (store.isAssignableFrom(datastore.getClass())) {
            lockOnConsume.add(datastore);
          } else {
            lockOnStart.add(datastore);
          }
        }
      }
    }

    public LockManager lockProcessStart() {
      LOG.info("Locking datastores on workflow start");
      lock(lockOnStart);
      return this;
    }

    public LockManager lockConsumeStart() {
      LOG.info("Locking datastores on action start");
      lock(lockOnConsume);
      return this;
    }

    public LockManager lock() {
      lockProcessStart();
      lockConsumeStart();
      return this;
    }

    private void lock(List<DataStore> toLock) {
      for (DataStore dataStore : toLock) {
        ResourceSemaphore lock = createLock(dataStore);
        lock.lock();

        heldLocks.add(lock);
      }
      LOG.info("Have acquired locks " + heldLocks);
    }

    public void release() {
      for (ResourceSemaphore lock : heldLocks) {
        lock.release();
      }
    }

  }

  public LockManager createManager(DataStore dataStore) {
    return createManager(Lists.newArrayList(dataStore));
  }

  public LockManager createManager(Collection<DataStore> datastores) {
    return new LockManager(datastores);
  }

  public boolean hasReaders(VersionedStore store, long version) {
    return createLock(store.getVersionPath(version)).hasReaders();
  }

  @Deprecated
  //  not really deprecated but can be made private after removing external uses
  public ResourceSemaphore createLock(DataStore store) {
    try {
      if (store instanceof VersionedBucketDataStore) {
        VersionedBucketDataStore versioned = (VersionedBucketDataStore)store;
        String path = versioned.getLatestVersion().getPath();
        LOG.info("Locking version path " + path + " against deletion.");
        return createLock(path);
      } else {
        String path = store.getPath();
        LOG.info("Locking path " + path + " against deletion.");
        return createLock(path);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Deprecated
  //  not really deprecated, but can make this private I think
  public abstract ResourceSemaphore createLock(String path);

  public abstract void shutdown();



}
