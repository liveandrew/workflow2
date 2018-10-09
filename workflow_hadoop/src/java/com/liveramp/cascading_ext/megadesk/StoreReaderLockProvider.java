package com.liveramp.cascading_ext.megadesk;

public interface StoreReaderLockProvider {
  public abstract StoreReaderLocker create();
}