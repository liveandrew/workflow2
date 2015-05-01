package com.rapleaf.cascading_ext.workflow2.action;


import java.util.List;

import org.apache.thrift.TBase;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.test.ThriftBucketHelper;

public class WriteToBucketAction<T extends TBase> extends Action {
  private final List<T> inputList;
  private final BucketDataStore<T> outputStore;

  public WriteToBucketAction(String checkPointToken, String tmp, List<T> inputList, BucketDataStore<T> outputStore) {
    super(checkPointToken, tmp);
    this.inputList = inputList;
    this.outputStore = outputStore;
  }

  @Override
  protected void execute() throws Exception {
    ThriftBucketHelper.writeToBucket(outputStore.getBucket(), inputList);
  }
}
