package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Level;
import org.junit.Before;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.BucketDataStoreImpl;
import com.rapleaf.cascading_ext.workflow2.test.BaseWorkflowTestCase;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.stream.RecordOutputStream;
import com.rapleaf.support.Strings;

public class WorkflowTestCase extends BaseWorkflowTestCase {
  public WorkflowTestCase() {
    super(Level.ALL, "workflow");
  }

  @Before
  public void deleteFixtures() throws Exception {
    new DatabasesImpl().getRlDb().deleteAll();
  }


  public static void fillWithData(Bucket b, String relPath, String... records) throws IOException {
    RecordOutputStream os = b.openWrite(relPath);
    for (String record : records) {
      os.write(Strings.toBytes(record));
    }
    os.close();
  }

  public BucketDataStore<BytesWritable> asStore(String dir) throws IOException {
    return new BucketDataStoreImpl<BytesWritable>(fs, "", dir, "", BytesWritable.class);
  }

}
