package com.liveramp.cascading_ext.megadesk;

public class MockStoreReaderLockProvider implements StoreReaderLockProvider {

  @Override
  public StoreReaderLocker create() {
    return new MockStoreReaderLocker();
  }

  public static class MockStoreReaderLocker extends StoreReaderLocker {
    @Override
    public ResourceSemaphore createLock(String path) {
      return new MockResourceSemaphore();
    }

    @Override
    public void shutdown() {
      // no op
    }
  }

}
