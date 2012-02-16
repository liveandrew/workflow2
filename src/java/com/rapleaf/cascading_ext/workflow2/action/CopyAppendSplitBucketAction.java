package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.SplitBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CopyAppendSplitBucketAction extends Action {
  
  private final SplitBucketDataStore input;
  private final SplitBucketDataStore target;
  
  public CopyAppendSplitBucketAction(SplitBucketDataStore input, SplitBucketDataStore target) {
    super();
    
    this.input = input;
    this.target = target;
    
    readsFrom(input);
    writesTo(target);
  }
  
  @Override
  protected void execute() throws Exception {
    this.setStatusMessage("Copy-appending " + input.getPath() + " to " + target.getPath());
    target.getAttributeBucket().copyAppend(input.getAttributeBucket());
  }
}
