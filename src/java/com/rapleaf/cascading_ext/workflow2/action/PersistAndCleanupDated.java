package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Collections;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DateVersionedBucketDataStore;
import com.rapleaf.cascading_ext.state.TypedState;
import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.support.DayOfYear;

public class PersistAndCleanupDated<E extends Enum<E>, T> extends MultiStepAction {
  public PersistAndCleanupDated(String checkpointToken,
                                String tmpDir,
                                E versionField,
                                TypedState<E, DayOfYear> state,
                                BucketDataStore<T> version,
                                DateVersionedBucketDataStore<T> store,
                                int versionsToKeep) {
    super(checkpointToken, tmpDir);

    Step persist = new Step(new PersistNewDatedVersion<E, T>("persist-new-version",
        versionField,
        state,
        version,
        store));

    Step cleanup = new Step(new CleanUpOlderVersions("cleanup-versions",
        getTmpRoot(),
        versionsToKeep,
        Collections.singleton(store)), persist);

    setSubStepsFromTail(cleanup);
  }
}
