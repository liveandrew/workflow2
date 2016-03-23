package com.liveramp.cascading_ext.megadesk;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Hex;
import org.apache.curator.framework.CuratorFramework;

import com.rapleaf.cascading_ext.queues.ProductionCuratorProvider;
import com.rapleaf.support.HashHelper;

public class CuratorStoreReaderLockProvider extends StoreReaderLockProvider {

  public final List<CuratorFramework> frameworks;

  public static CuratorStoreReaderLockProvider getProduction() {
    return new CuratorStoreReaderLockProvider(Lists.newArrayList(
        ProductionCuratorProvider.INSTANCE.getFrameworkNew()
    ));
  }

  public CuratorStoreReaderLockProvider(CuratorFramework ... frameworks){
    this(Lists.newArrayList(frameworks));
  }

  public CuratorStoreReaderLockProvider(List<CuratorFramework> frameworks) {
    this.frameworks = frameworks;
  }

  @Override
  public ResourceSemaphore createLock(String path) {
    System.out.println("Semaphore on path "+path);
    String hashedPath = Hex.encodeHexString(HashHelper.getMD5Hash(path));
    return new ResourceSemaphoreImpl(frameworks, hashedPath, path);
  }
}
