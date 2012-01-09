package com.rapleaf.support.workflow2.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import cascading.tuple.Fields;

import com.rapleaf.cascading_ext.relevance.Relevance;
import com.rapleaf.cascading_ext.relevance.Relevance.RelevanceFunction;
import com.rapleaf.support.datastore.BucketDataStore;
import com.rapleaf.support.datastore.BucketDataStoreImpl;
import com.rapleaf.support.datastore.DataStore;
import com.rapleaf.support.workflow2.MultiStepAction;
import com.rapleaf.support.workflow2.Step;

public class ExtractAndCombineKeysAction extends MultiStepAction {
  
  public ExtractAndCombineKeysAction(String checkpointToken, String tmpDir, List<DataStore> sources, BucketDataStore output,
      String outField, RelevanceFunction func, Class type) throws IOException {
    super(checkpointToken, tmpDir);

    List<DataStore> tmpKeys = new ArrayList<DataStore>();
    List<Step> extractActions = new ArrayList<Step>();
    for(int i = 0; i < sources.size(); i++){
      DataStore store = sources.get(i);      
      
      BucketDataStore tmpStore = new BucketDataStoreImpl(getFS(), "tmp keys "+i, getTmpRoot(), "/tmp_keys_"+i, type);
      Step extract = new Step(new ExtractKeysAction("extract-keys-"+i, store, tmpStore, outField, func, type));
      
      tmpKeys.add(tmpStore);
      extractActions.add(extract);
    }
    
    setSubStepsFromTails(Collections.singleton(new Step(new CombineAndDistinct("combine-and-distinct", tmpKeys, 
        new Fields(outField), output), extractActions)));
  }
}
