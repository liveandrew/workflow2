package com.rapleaf.cascading_ext.workflow2.action;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStores;
import com.rapleaf.cascading_ext.datastore.DatastoresHelper;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.formats.test.BucketHelper;
import com.rapleaf.formats.test.ThriftBucketHelper;

import static org.junit.Assert.*;

public class TestCopyDataStoreAction extends WorkflowTestCase {
  @Test
  public void testBasic() throws Exception {
    final BucketDataStore<byte[]> ds1 = DataStores.bucket(getTestRoot() + "/ds1", byte[].class);
    BucketHelper.writeToBucket(ds1.getBucket(), new byte[] { 1 });

    final BucketDataStore<byte[]> ds2 = DataStores.bucket(getTestRoot() + "/ds2", byte[].class);

    new CopyDataStoreAction<>("cp", getTestRoot() + "/tmp", ds1, ds2).execute();

    assertTrue("Destination should exist", getFS().exists(new Path(ds2.getPath())));
    assertEquals("Destination should have a single record", 1, BucketHelper.readBucket(ds2.getBucket()).size());
    assertEquals("Destination record should have the right length", 1, BucketHelper.readBucket(ds2.getBucket()).get(0).length);
    assertEquals("Destination record should have the right contents", 1, BucketHelper.readBucket(ds2.getBucket()).get(0)[0]);
  }

  @Test
  public void testDestinationAlreadyExists() throws Exception {
    final BucketDataStore<byte[]> ds1 = DataStores.bucket(getTestRoot() + "/ds1", byte[].class);
    BucketHelper.writeToBucket(ds1.getBucket(), new byte[]{1});

    final BucketDataStore<byte[]> ds2 = DataStores.bucket(getTestRoot() + "/ds2", byte[].class);
    BucketHelper.writeToBucket(ds2.getBucket(), new byte[]{2});

    execute(new CopyDataStoreAction<>("cp", getTestRoot() + "/tmp", ds1, ds2));

    assertTrue("Destination should exist", getFS().exists(new Path(ds2.getPath())));
    assertEquals("Destination should have a single record", 1, BucketHelper.readBucket(ds2.getBucket()).size());
    assertEquals("Destination record should have the right length", 1, BucketHelper.readBucket(ds2.getBucket()).get(0).length);
    assertEquals("Destination record should have the right contents", 1, BucketHelper.readBucket(ds2.getBucket()).get(0)[0]);
  }
}