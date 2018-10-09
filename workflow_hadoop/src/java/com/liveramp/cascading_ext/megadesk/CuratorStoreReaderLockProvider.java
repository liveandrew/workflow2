package com.liveramp.cascading_ext.megadesk;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;

import com.rapleaf.cascading_ext.queues.ProductionCuratorProvider;

public class CuratorStoreReaderLockProvider  {

  public static StoreReaderLockProvider getProduction(){
    return new StoreReaderLockProvider() {
      @Override
      public StoreReaderLocker create() {
        return new CuratorStoreLocker(Lists.newArrayList(ProductionCuratorProvider.getNewInstance()));
      }
    };
  }

  public static StoreReaderLockProvider forCluster(final CuratorFramework framework){
    return new StoreReaderLockProvider() {
      @Override
      public StoreReaderLocker create() {
        return new CuratorStoreLocker(Lists.newArrayList(framework));
      }
    };
  }


}
