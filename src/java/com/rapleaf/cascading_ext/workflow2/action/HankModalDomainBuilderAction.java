package com.rapleaf.cascading_ext.workflow2.action;

import cascading.cascade.Cascades;
import cascading.tap.Tap;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.hank.config.CoordinatorConfigurator;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.HankDataStore;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public abstract class HankModalDomainBuilderAction extends HankDomainBuilderAction {
  private BucketDataStore base = null;
  private BucketDataStore delta = null;
  private BucketDataStore input = null;
  private double domainDeltaRatio;
  private static final Logger LOG = LoggerFactory.getLogger(HankModalDomainBuilderAction.class);


  public HankModalDomainBuilderAction(
      String checkpointToken,
      HankVersionType versionType,
      CoordinatorConfigurator configurator,
      HankDataStore output,
      double ratio) {
    this(checkpointToken, null, versionType, configurator, output, ratio);
  }

  public HankModalDomainBuilderAction(
      String checkpointToken,
      HankVersionType versionType,
      boolean shouldPartitionAndSortInput,
      CoordinatorConfigurator configurator,
      HankDataStore output,
      double ratio) {
    super(checkpointToken,versionType, shouldPartitionAndSortInput, configurator, output);
    this.domainDeltaRatio = ratio;
  }

  public HankModalDomainBuilderAction(
      String checkpointToken,
      CoordinatorConfigurator configurator,
      HankDataStore output,
      double ratio) {
    this(checkpointToken, null, null, configurator, output, ratio);
  }

  public HankModalDomainBuilderAction(
      String checkpointToken,
      String tmpRoot,
      HankVersionType versionType,
      CoordinatorConfigurator configurator,
      HankDataStore output,
      double ratio) {
    super(checkpointToken, tmpRoot, versionType, configurator, output);
    this.domainDeltaRatio = ratio;
  }

  public HankModalDomainBuilderAction(
      String checkpointToken,
      String tmpRoot,
      CoordinatorConfigurator configurator,
      HankDataStore output,
      double ratio) {
    this(checkpointToken, tmpRoot, null, configurator, output, ratio);
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

    if (versionType == null) {
      if (deltaLargerThanRatio(getBase(), getDelta())) {
        setVersionType(HankVersionType.BASE);
        setInput(getBase());
      } else {
        setVersionType(HankVersionType.DELTA);
        setInput(getDelta());
      }
      LOG.info("Chose version type " + getVersionType());
    }
  }

  @Override
  protected Map<String, Tap> getSources() {
    return Cascades.tapsMap(getPipeName(), getInput().getTap());
  }

  public abstract String getPipeName();


  private static double getStoreSize(BucketDataStore store) {
    try {
      return FileSystemHelper.getFS().getContentSummary(new Path(store.getPath())).getLength();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean deltaLargerThanRatio(BucketDataStore base, BucketDataStore delta) {
    return getStoreSize(delta) / getStoreSize(base) > domainDeltaRatio;
  }

  protected void assignSingleStore(BucketDataStore bucket, HankVersionType versionType) {
    if (versionType == HankVersionType.BASE) {
      setBase(bucket);
    } else {
      setDelta(bucket);
    }

    setInput(bucket);
  }

  public void setInput(BucketDataStore input) {
    this.input = input;
  }

  public BucketDataStore getInput() {
    return input;
  }
}
