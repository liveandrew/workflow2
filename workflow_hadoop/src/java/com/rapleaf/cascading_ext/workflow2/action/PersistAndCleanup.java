package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;

import java.util.Collections;

public class PersistAndCleanup<T> extends MultiStepAction {

  public PersistAndCleanup(String checkpointToken,
                           String tmpRoot,
                           BucketDataStore<T> version,
                           VersionedBucketDataStore<T> store,
                           int versionsToKeep) {
    super(checkpointToken, tmpRoot);

    Step persist = new Step(new PersistNewVersion<T>("persist-new-version",
        version,
        store));

    Step cleanup = new Step(new CleanUpOlderVersions("cleanup-versions",
        getTmpRoot(),
        versionsToKeep,
        Collections.singleton(store)), persist);

    setSubStepsFromTail(cleanup);
  }
}
