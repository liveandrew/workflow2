package com.liveramp.workflow.msj_store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.map_side_join.MSJUtil;
import com.rapleaf.cascading_ext.map_side_join.MapSideJoin;
import com.rapleaf.cascading_ext.msj_tap.joiner.SingleStreamEmittingJoiner;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.MSJStore;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;
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



}
