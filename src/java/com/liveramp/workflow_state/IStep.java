package com.liveramp.workflow_state;

import com.google.common.collect.Multimap;

import com.liveramp.workflow_core.runner.ExecutionNode;

public interface IStep {

  public Multimap<DSAction, DataStoreInfo> getDataStores();

  public String getCheckpointToken();

  public String getActionClass();

}
