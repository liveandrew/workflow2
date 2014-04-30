package com.liveramp.cascading_ext.megadesk;

import org.apache.commons.codec.binary.Hex;
import org.apache.curator.framework.CuratorFramework;

import com.rapleaf.cascading_ext.queues.LiverampQueues;
import com.rapleaf.support.HashHelper;

public class CuratorStoreReaderLockProvider extends StoreReaderLockProvider {

  public final CuratorFramework framework;

  public static CuratorStoreReaderLockProvider getProduction() {
    return new CuratorStoreReaderLockProvider(LiverampQueues.getProduction().getFramework());
  }

  public CuratorStoreReaderLockProvider(CuratorFramework framework) {
    this.framework = framework;
  }

  @Override
  public ResourceSemaphore createLock(String path) {
    System.out.println("Semaphore on path "+path);
    String hashedPath = Hex.encodeHexString(HashHelper.getMD5Hash(path));
    return new ResourceSemaphoreImpl(framework, hashedPath, path);
  }
}
