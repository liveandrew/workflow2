package com.rapleaf.cascading_ext.workflow2.action;

import cascading.tuple.Fields;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.BucketDataStoreImpl;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.relevance.function.RelevanceFunction;
import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class ExtractAndCombineKeysAction extends MultiStepAction {

  public ExtractAndCombineKeysAction(String checkpointToken,
      String tmpDir,
      List<DataStore> sources,
      BucketDataStore output,
      String outField,
      RelevanceFunction func,
      Class type) throws IOException {
    super(checkpointToken, tmpDir);

    List<DataStore> tmpKeys = new ArrayList<DataStore>();
    List<Step> extractActions = new ArrayList<Step>();
    for (int i = 0; i < sources.size(); i++) {
      DataStore store = sources.get(i);

      BucketDataStore tmpStore = new BucketDataStoreImpl(getFS(), "tmp keys " + i, getTmpRoot(),
        "/tmp_keys_" + i, type);
      Step extract = new Step(new ExtractKeysAction("extract-keys-" + i, store, tmpStore, outField,
        func, type));

      tmpKeys.add(tmpStore);
      extractActions.add(extract);
    }

    setSubStepsFromTails(Collections.singleton(new Step(new CombineAndDistinct(
      "combine-and-distinct", tmpKeys, new Fields(outField), output), extractActions)));
  }
}
