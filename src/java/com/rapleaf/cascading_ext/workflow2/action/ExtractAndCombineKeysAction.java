package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cascading.tuple.Fields;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.BucketDataStoreImpl;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.relevance.Relevance.RelevanceFunction;
import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;

public class ExtractAndCombineKeysAction extends MultiStepAction {
  
  public ExtractAndCombineKeysAction(String tmpDir,
      List<DataStore> sources,
      BucketDataStore output,
      String outField,
      RelevanceFunction func,
      Class type) throws IOException {
    super(tmpDir);
    
    List<DataStore> tmpKeys = new ArrayList<DataStore>();
    List<Step> extractActions = new ArrayList<Step>();
    for (int i = 0; i < sources.size(); i++ ) {
      DataStore store = sources.get(i);
      
      BucketDataStore tmpStore = new BucketDataStoreImpl(getFS(), "tmp keys " + i, getTmpRoot(),
          "/tmp_keys_" + i, type);
      Step extract = new Step("extract-keys-" + i, new ExtractKeysAction(store, tmpStore, outField,
          func, type));
      
      tmpKeys.add(tmpStore);
      extractActions.add(extract);
    }
    
    setSubStepsFromTails(Collections.singleton(new Step("combine-and-distinct", new CombineAndDistinct(tmpKeys,
        new Fields(outField), output), extractActions)));
  }
}
