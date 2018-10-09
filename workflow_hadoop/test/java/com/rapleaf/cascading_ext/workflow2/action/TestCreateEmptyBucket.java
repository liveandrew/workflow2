package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.types.new_person_data.PIN;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestCreateEmptyBucket extends WorkflowTestCase {
  private final DataStoreBuilder builder = new DataStoreBuilder(getTestRoot());
  private Pattern partPattern = Pattern.compile("part\\-\\d+\\.bucketfile");

  @Test
  public void testWithManyParts() throws Exception {
    createBucketWithNumPartitions(builder.getPINDataStore("with_many_parts"), 300, PIN.class);
  }

  @Test
  public void testWithOnePart() throws Exception {
    createBucketWithNumPartitions(builder.getPINDataStore("one_part"), 1, PIN.class);
  }

  @Test
  public void testWithNoParts() throws Exception {
    createBucketWithNumPartitions(builder.getPINDataStore("zero_parts"), 0, PIN.class);
  }

  public void createBucketWithNumPartitions(BucketDataStore ds, int numPartitions, Class recordType) throws Exception {
    CreateEmptyBucket action = new CreateEmptyBucket(new Path(ds.getPath()).getName(), ds, numPartitions, true, recordType);

    execute(action);

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
