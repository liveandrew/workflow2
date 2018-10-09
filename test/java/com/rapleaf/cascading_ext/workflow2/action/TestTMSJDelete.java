package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.TMSJDataStoreHelper;
import com.rapleaf.cascading_ext.map_side_join.extractors.TStringBytesExtractor;
import com.rapleaf.cascading_ext.msj_tap.store.TMSJDataStore;
import com.rapleaf.cascading_ext.test.ByteArrayExtractorComparator;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.types.new_person_data.SPEL;

public class TestTMSJDelete extends WorkflowTestCase {
  private TMSJDataStore<SPEL> spelDataStore;
  private BucketDataStore<SPEL> deletionDelta;

  @Before
  public void prepareDataStore() throws IOException, TException {
    spelDataStore = new TMSJDataStore<SPEL>(getTestRoot() + "/spel_data_store",
        SPEL.class,
        new TStringBytesExtractor(SPEL._Fields.SPEL),
        .2,
        3
    );

    TMSJDataStoreHelper.writeNewBase(builder(), spelDataStore, Lists.newArrayList(
        new SPEL("1"),
        new SPEL("3"),
        new SPEL("4"),
        new SPEL("5")
    ));

    TMSJDataStoreHelper.writeNewDelta(builder(), spelDataStore, Lists.newArrayList(
        new SPEL("2"),
        new SPEL("4")
    ));

    deletionDelta = builder().getBucketDataStore("deletion_Data", SPEL.class);
    ThriftBucketHelper.writeToBucketAndSort(
        deletionDelta.getBucket(),
        new ByteArrayExtractorComparator<SPEL>(SPEL._Fields.SPEL),
        Lists.newArrayList(
            new SPEL("1"),
            new SPEL("4")
        )
    );
  }

  @Test
  public void testIt() throws IOException {
    Step delete = new Step(new CommitTMSJDelete<SPEL>(
        "commit-delete",
        deletionDelta,
        spelDataStore
    ));

    execute(delete);

    List<SPEL> expected = Lists.newArrayList(
        new SPEL("2"),
        new SPEL("3"),
        new SPEL("5")
    );

    Collection<SPEL> actual = HRap.getValues(spelDataStore);

    assertCollectionEquivalent(expected, actual);
  }
}
