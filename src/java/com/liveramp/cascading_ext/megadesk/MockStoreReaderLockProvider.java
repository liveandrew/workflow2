package com.liveramp.cascading_ext.megadesk;

public class MockStoreReaderLockProvider extends StoreReaderLockProvider {
  @Override
  public ResourceSemaphore createLock(String path) {
    return new MockResourceSemaphore();
  }
}
