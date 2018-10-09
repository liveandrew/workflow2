package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;

import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.extractors.BytesWritableFromByteArray;
import com.rapleaf.cascading_ext.msj_tap.store.TMSJDataStore;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.types.new_person_data.PIN;

public class TestCopyCommitTMSJDelta extends WorkflowTestCase {
  private TMSJDataStore<PIN> pins;
  private BucketDataStore<PIN> delta;

  private static final PIN email1 = PIN.email("email1@gmail.com");
  private static final PIN email2 = PIN.email("email2@gmail.com");
  private static final PIN email3 = PIN.email("email3@gmail.com");

  @Before
  public void prepare() throws IOException, TException {
    BucketDataStore<PIN> base = builder().getPINDataStore("base");
    delta = builder().getPINDataStore("delta");
    pins = builder().getTMSJDataStore("pins", PIN.class, new BytesWritableFromByteArray());

    ThriftBucketHelper.writeToBucketAndSort(base.getBucket(), email1);
    ThriftBucketHelper.writeToBucketAndSort(delta.getBucket(), email1, email2, email3);

    pins.commitBase(base.getPath());
  }

  @Test
  public void testIt() throws IOException {
    execute(new CopyCommitTMSJDelta<PIN>("copy-commit-tmsj-delta", getTestRoot()+"/tmp", delta, pins));

    Assert.assertArrayEquals(
        HRap.getValues(pins).toArray(),
        new PIN[]{email1, email2, email3}
    );

    assertCollectionEquivalent(
        HRap.getValuesFromBucket(delta),
        Lists.newArrayList(email1, email2, email3)
    );
  }
}