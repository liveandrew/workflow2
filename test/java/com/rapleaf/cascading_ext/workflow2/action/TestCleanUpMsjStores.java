package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.datastore.TMSJDataStoreHelper;
import com.rapleaf.cascading_ext.map_side_join.extractors.TBinaryExtractor;
import com.rapleaf.cascading_ext.msj_tap.store.TMSJDataStore;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.PIN;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCleanUpMsjStores extends CascadingExtTestCase {

  private TMSJDataStore<DustinInternalEquiv> store;

  @Before
  public void setUp() throws Exception {
    store = new TMSJDataStore<DustinInternalEquiv>(getTestRoot() + "/store",
        DustinInternalEquiv.class,
        new TBinaryExtractor<PIN>(PIN.class, DustinInternalEquiv._Fields.PIN),
        0.5);
  }

  @Test
  public void testDeleteOldVersions() throws Exception {
    writeNewVersions(5);
    runAndVerify(2, new int[] {3, 4});
    runAndVerify(2, new int[] {3, 4});
    runAndVerify(5, new int[] {3, 4});
    writeNewVersions(5);
    runAndVerify(3, new int[] {7, 8, 9});
  }

  private void runAndVerify(int numToKeep, int[] expectedRemaining) throws IOException {
    executeWorkflow(new CleanUpMsjStores("checkpoint", numToKeep, false, store));
    FileStatus[] statuses = getFS().listStatus(new Path(store.getPath()));
    Set<String> remainingPaths = Sets.newHashSet();
    Set<Integer> remainingVersions = Sets.newHashSet();
    for (FileStatus status : statuses) {
      remainingPaths.add(status.getPath().getName());
      remainingVersions.add(Integer.parseInt(status.getPath().getName().split("_")[1]));
    }
    assertEquals("Total bases and mailboxes should match expected values", 2 * expectedRemaining.length, remainingPaths.size());
    for (int version : expectedRemaining) {
      assertTrue(remainingVersions.contains(version));
    }
  }

  private void writeNewVersions(int numVersions) throws IOException, org.apache.thrift.TException {
    for (int i = 0; i < numVersions; i++) {
      if (store.getStore().hasBase()) {
        store.getStore().acquireBaseCreationAttempt();
      }
      TMSJDataStoreHelper.writeNewBase(builder(), store, new ArrayList<DustinInternalEquiv>());
    }
  }
}