package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.SplitBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CopyAppendWithTmpRootAction extends Action {

  private final BucketDataStore input;
  private final BucketDataStore target;

  public CopyAppendWithTmpRootAction(String checkpointToken, String tmpRoot, BucketDataStore input, BucketDataStore target) {
    super(checkpointToken, tmpRoot);

    this.input = input;
    this.target = target;

    if (target instanceof SplitBucketDataStore) {
      throw new RuntimeException("Trying to append into a split bucket! Use CopyAppendSplitBucketAction instead");
    }

    readsFrom(input);
    writesTo(target);
  }

  @Override
  protected void execute() throws Exception {
    this.setStatusMessage("Copy-appending " + input.getPath() + " to " + target.getPath());
    target.getBucket().copyAppend(input.getBucket(), true, getInheritedProperties(), getTmpRoot());
  }
}
