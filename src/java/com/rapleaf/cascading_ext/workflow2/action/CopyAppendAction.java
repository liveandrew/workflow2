package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CopyAppendAction extends Action {
  
  private final BucketDataStore input;
  private final BucketDataStore target;
  
  public CopyAppendAction(BucketDataStore input, BucketDataStore target) {
    super();
    
    this.input = input;
    this.target = target;
    
    readsFrom(input);
    writesTo(target);
  }
  
  @Override
  protected void execute() throws Exception {
    this.setStatusMessage("Copy-appending " + input.getPath() + " to " + target.getPath());
    target.getBucket().copyAppend(input.getBucket());
  }
}
