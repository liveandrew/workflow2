package com.liveramp.workflow_state;

import com.google.common.collect.Multimap;

public interface IStep {

  public Multimap<DSAction, DataStoreInfo> getDataStores();

  public String getCheckpointToken();

  public String getActionClass();

}
