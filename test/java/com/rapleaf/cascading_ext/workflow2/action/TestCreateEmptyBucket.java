package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;

public class TestCreateEmptyBucket extends CascadingExtTestCase {
  private final DataStoreBuilder builder = new DataStoreBuilder(getTestRoot());
  private Pattern partPattern = Pattern.compile("part\\-\\d+\\.bucketfile");
  
  public void testWithManyParts() throws Exception {
    createBucketWithNumPartitions(builder.getPINDataStore("with_many_parts"), 300);
  }

  public void testWithOnePart() throws Exception {
    createBucketWithNumPartitions(builder.getPINDataStore("with_many_parts"), 1);
  }

  public void testWithNoParts() throws Exception {
    createBucketWithNumPartitions(builder.getPINDataStore("with_many_parts"), 0);
  }

  public void createBucketWithNumPartitions(BucketDataStore ds, int numPartitions) throws Exception {
    CreateEmptyBucket action = new CreateEmptyBucket(new Path(ds.getPath()).getName(), ds, numPartitions, true);

    action.execute();

    verifyPartitions(ds, numPartitions);
    
    assertTrue(ds.getBucket().isImmutable());
  }

  private void verifyPartitions(BucketDataStore ds, int numPartitions) throws IOException {
    FileStatus[] statuses = getFS().listStatus(new Path(ds.getPath()));
    Set<String> distinctPartitions = new HashSet<String>();
    boolean hasMeta = false;

    for (FileStatus status : statuses) {
      String path = status.getPath().toString();

      if (path.endsWith("bucket.meta")) {
        hasMeta = true;
      } else {
        distinctPartitions.add(path);
        assertTrue(partPattern.matcher(path).find());
      }
    }

    assertTrue(hasMeta);

    assertEquals(numPartitions, distinctPartitions.size());
  }
}
