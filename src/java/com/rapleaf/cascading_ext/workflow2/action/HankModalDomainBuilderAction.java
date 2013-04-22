package com.rapleaf.cascading_ext.workflow2.action;

import cascading.cascade.Cascades;
import cascading.tap.Tap;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.HankDataStore;
import com.rapleaf.hank.config.CoordinatorConfigurator;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public abstract class HankModalDomainBuilderAction extends HankDomainBuilderAction{
  BucketDataStore base = null;
  BucketDataStore delta = null;
  public static final double HANK_DELTA_TO_BASE_SIZE_RATIO = 0.5;
  private static final Logger LOG = Logger.getLogger(HankModalDomainBuilderAction.class);



  public HankModalDomainBuilderAction(String checkpointToken, HankVersionType versionType, CoordinatorConfigurator configurator, HankDataStore output) {
    super(checkpointToken, versionType, configurator, output);
  }

  public HankModalDomainBuilderAction(String checkpointToken, CoordinatorConfigurator configurator, HankDataStore output) {
    super(checkpointToken, null, configurator, output);
  }

  public HankModalDomainBuilderAction(String checkpointToken, String tmpRoot, HankVersionType versionType, CoordinatorConfigurator configurator, HankDataStore output) {
    super(checkpointToken, tmpRoot, versionType, configurator, output);
  }

  public HankModalDomainBuilderAction(String checkpointToken, String tmpRoot,  CoordinatorConfigurator configurator, HankDataStore output) {
    super(checkpointToken, tmpRoot, null, configurator, output);
  }

  public void setBase(BucketDataStore base) {
    this.base = base;
  }

  public void setDelta(BucketDataStore delta) {
    this.delta = delta;
  }

  public BucketDataStore getBase() {
    return base;
  }

  public BucketDataStore getDelta() {
    return delta;
  }

  @Override
  protected void prepare() {
    super.prepare();

    if(versionType == null){
      setVersionType(deltaLargerThanRatio(getBase(), getDelta()) ? HankVersionType.BASE : HankVersionType.DELTA);
      LOG.info("Choose version type "+getVersionType());
    }
  }

  @Override
  protected Map<String, Tap> getSources() {
    return Cascades.tapsMap(getPipeName(), getInputStore().getTap());
  }

  public abstract String getPipeName();

  private BucketDataStore getInputStore() {
    if(getBase() == null){
      return getDelta();
    }
    if(getDelta() == null){
      return getBase();
    }
    return deltaLargerThanRatio(getBase(), getDelta()) ? getBase() : getDelta();
  }

  private static double getStoreSize(BucketDataStore store) {
    try {
      return FileSystemHelper.getFS().getContentSummary(new Path(store.getPath())).getLength();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean deltaLargerThanRatio(BucketDataStore base, BucketDataStore delta) {
    return getStoreSize(delta) / getStoreSize(base) > HANK_DELTA_TO_BASE_SIZE_RATIO;
  }

  protected void assignSingleStore(BucketDataStore bucket, HankVersionType versionType) {
    if (versionType == HankVersionType.BASE) {
      setBase(bucket);
    } else {
      setDelta(bucket);
    }
  }
}
