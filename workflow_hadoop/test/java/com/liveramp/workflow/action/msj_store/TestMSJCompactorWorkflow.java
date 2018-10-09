package com.liveramp.workflow.action.msj_store;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import cascading.tuple.TupleEntryIterator;

import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.extractors.TByteArrayExtractor;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.MSJStore;
import com.rapleaf.cascading_ext.msj_tap.store.TMSJDataStore;
import com.rapleaf.cascading_ext.test.TExtractorComparator;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.PIN;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestMSJCompactorWorkflow extends WorkflowTestCase {

  public static final PIN PIN1 = PIN.email("ben1@gmail.com");
  public static final PIN PIN2 = PIN.email("ben2@gmail.com");
  public static final PIN PIN3 = PIN.email("ben3@gmail.com");
  public static final PIN PIN4 = PIN.email("ben4@gmail.com");
  public static final PIN PIN5 = PIN.email("ben5@gmail.com");
  public static final PIN PIN6 = PIN.email("ben6@gmail.com");

  //  base
  public static final DustinInternalEquiv die1 = new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN1, 0);
  public static final DustinInternalEquiv die3 = new DustinInternalEquiv(ByteBuffer.wrap("2".getBytes()), PIN3, 0);

  //  delta 1
  public static final DustinInternalEquiv die5 = new DustinInternalEquiv(ByteBuffer.wrap("2".getBytes()), PIN5, 0);
  public static final DustinInternalEquiv die2 = new DustinInternalEquiv(ByteBuffer.wrap("3".getBytes()), PIN2, 0);

  //  delta 2
  public static final DustinInternalEquiv die4 = new DustinInternalEquiv(ByteBuffer.wrap("3".getBytes()), PIN4, 0);
  public static final DustinInternalEquiv die6 = new DustinInternalEquiv(ByteBuffer.wrap("4".getBytes()), PIN6, 0);

  public static final TByteArrayExtractor DIE_EID_EXTRACTOR =
      new TByteArrayExtractor(DustinInternalEquiv._Fields.EID);

  public static final TExtractorComparator<DustinInternalEquiv, BytesWritable> DIE_EID_COMPARATOR =
      new TExtractorComparator<DustinInternalEquiv, BytesWritable>(DIE_EID_EXTRACTOR);

  @Test
  public void testCompaction() throws Exception {

    TMSJDataStore store = new TMSJDataStore<DustinInternalEquiv>(
        getTestRoot() + "/tmp",
        DustinInternalEquiv.class,
        DIE_EID_EXTRACTOR,
        .1
    );

    BucketDataStore<DustinInternalEquiv> store1 = builder().getBucketDataStore("base", DustinInternalEquiv.class);
    BucketDataStore<DustinInternalEquiv> store2 = builder().getBucketDataStore("delta1", DustinInternalEquiv.class);
    BucketDataStore<DustinInternalEquiv> store3 = builder().getBucketDataStore("delta2", DustinInternalEquiv.class);

    ThriftBucketHelper.writeToBucketAndSort(store1.getBucket(), DIE_EID_COMPARATOR,
        die1,
        die3
    );

    ThriftBucketHelper.writeToBucketAndSort(store2.getBucket(), DIE_EID_COMPARATOR,
        die5,
        die2
    );

    ThriftBucketHelper.writeToBucketAndSort(store3.getBucket(), DIE_EID_COMPARATOR,
        die4,
        die6
    );

    store.commitBase(store1.getPath());
    store.commitDelta(store2.getPath());
    store.commitDelta(store3.getPath());

    MSJStore.ReadSet prevReadSet = store.getReadSet();
    String previousBasePath = prevReadSet.getBase();

    execute(new MSJCompactorWorkflow("compact", getTestRoot()+"/compactor", Lists.<MSJDataStore>newArrayList(store)));

    //  should be no deltas anymore
    MSJStore.ReadSet postReadSet = store.getReadSet();
    assertEquals(0, postReadSet.getDeltas().size());
    assertEquals(0.0, postReadSet.getRatio(), .0000001);

    //  double-verify we have a new base
    assertFalse(previousBasePath.equals(postReadSet.getBase()));

    List<DustinInternalEquiv> dies = Lists.newArrayList();
    TupleEntryIterator iter = store.getTap().openForRead(CascadingHelper.get().getFlowProcess());
    while (iter.hasNext()) {
      dies.add((DustinInternalEquiv)iter.next().getObject(0));
    }

    assertCollectionEquivalent(Lists.newArrayList(die1, die5, die4, die6), dies);
  }

}