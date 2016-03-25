package com.liveramp.cascading_ext.megadesk;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Hex;
import org.apache.curator.framework.CuratorFramework;

import com.rapleaf.support.HashHelper;

public class CuratorStoreLocker extends StoreReaderLocker {

  public final List<CuratorFramework> frameworks;

  public CuratorStoreLocker(CuratorFramework ... frameworks){
    this(Lists.newArrayList(frameworks));
  }

  public CuratorStoreLocker(List<CuratorFramework> frameworks) {
    this.frameworks = frameworks;
  }

  @Override
  public ResourceSemaphore createLock(String path) {
    System.out.println("Semaphore on path "+path);
    String hashedPath = Hex.encodeHexString(HashHelper.getMD5Hash(path));
    return new ResourceSemaphoreImpl(frameworks, hashedPath, path);
  }

  @Override
  public void shutdown(){
    for (CuratorFramework framework : frameworks) {
      framework.close();
    }
  }
}
