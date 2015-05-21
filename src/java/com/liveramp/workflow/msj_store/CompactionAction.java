package com.liveramp.workflow.msj_store;

import java.io.IOException;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.compaction.CompactionUtil;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CompactionAction<T extends Comparable> extends Action {

  private final MSJDataStore dataStore;
  private final BucketDataStore tempOutput;

  public CompactionAction(String checkpointToken, String tmpRoot, MSJDataStore<T> store) throws IOException {
    super(checkpointToken, tmpRoot);
    this.dataStore = store;
    this.tempOutput = builder().getBytesDataStore("temp_output");
    createsTemporary(tempOutput);
  }

  @Override
  protected void execute() throws Exception {
    CompactionUtil.tryCompact(dataStore, completeCallback(), tempOutput);
  }

}
