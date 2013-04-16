package com.rapleaf.cascading_ext.workflow2.action;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.HankDataStore;
import com.rapleaf.hank.config.CoordinatorConfigurator;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public abstract class BaseOrDeltaHankDomainBuilderAction extends HankDomainBuilderAction {

  public static final double DELTA_SIZE_RATIO_LIMIT = 0.7;
  private BucketDataStore inputStore;

  public BaseOrDeltaHankDomainBuilderAction(
      String checkpointToken,
      BucketDataStore inputBase,
      BucketDataStore inputDelta,
      CoordinatorConfigurator configurator,
      HankDataStore output) {
    this(checkpointToken, null, inputBase, inputDelta, configurator, output);
  }

  public BaseOrDeltaHankDomainBuilderAction(
      String checkpointToken,
      String tmpRoot,
      BucketDataStore inputBase,
      BucketDataStore inputDelta,
      CoordinatorConfigurator configurator,
      HankDataStore output) {
    super(checkpointToken, tmpRoot, null, configurator, output);

    double baseSize = getSize(inputBase);
    double deltaSize = getSize(inputDelta);

    if(deltaSize/baseSize > DELTA_SIZE_RATIO_LIMIT){
      this.inputStore = inputBase;
      this.versionType = HankVersionType.BASE;
    }else{
      this.inputStore = inputDelta;
      this.versionType = HankVersionType.DELTA;
    }


  }

  private double getSize(BucketDataStore store){
    try {
      return FileSystemHelper.getFS().getContentSummary(new Path(store.getPath())).getLength();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public BucketDataStore getInputStore() {
    return inputStore;
  }
}
