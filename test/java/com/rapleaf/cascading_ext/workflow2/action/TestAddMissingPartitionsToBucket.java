package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.stream.RecordOutputStream;
import com.rapleaf.support.Strings;

import static org.junit.Assert.assertEquals;

public class TestAddMissingPartitionsToBucket extends CascadingExtTestCase {

  @Test
  public void testIt() throws Exception {
    BucketDataStore<BytesWritable> dataStore = new DataStoreBuilder(getTestRoot()).getBytesDataStore("store");
    Bucket bucket = Bucket.open(fs, dataStore.getBucket().getRoot().toString(), BytesWritable.class);
    write(bucket, "part-00012_0", "a", "b", "c");
    write(bucket, "part-00061_0", "d", "e", "f");
    new AddMissingPartitionsToBucket<BytesWritable>("add-missing-partitions", 70, dataStore, BytesWritable.class).execute();
    assertEquals(Sets.newHashSet("a", "b", "c", "d", "e", "f"), readRecords(bucket));
    assertEquals(70, bucket.getStoredFiles().length);
  }

  private Set<String> readRecords(Bucket bucket) {
    Set<String> data = new HashSet<String>();

    for (byte[] record : bucket) {
      data.add(Strings.fromBytes(record));
    }

    return data;
  }
  private void write(Bucket bucket, String relPath, String... records) throws IOException {
    RecordOutputStream os = bucket.openWrite(relPath);

    for (String record : records) {
      os.write(Strings.toBytes(record));
    }

    os.close();
  }
}
