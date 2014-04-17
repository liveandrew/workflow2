package com.rapleaf.cascading_ext.workflow2;

import com.liveramp.cascading_ext.clockwork.StoreReaderLockProvider;

public abstract class ResourceManagedAction extends Action {

  private final StoreReaderLockProvider lockProvider;

  public ResourceManagedAction(String checkpointToken) {
    super(checkpointToken);
    lockProvider = null;
  }

  public ResourceManagedAction(String checkpointToken, String tmpRoot) {
    super(checkpointToken, tmpRoot);
    lockProvider = null;

  }



}
