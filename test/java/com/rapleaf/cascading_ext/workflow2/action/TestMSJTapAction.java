package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.BucketDataStoreImpl;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.msj_tap.merger.MSJGroup;
import com.rapleaf.cascading_ext.msj_tap.operation.MSJFunction;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.stream.RecordOutputStream;
import com.rapleaf.formats.test.BucketHelper;
import com.rapleaf.support.Strings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestMSJTapAction extends WorkflowTestCase {

  public static class MockExtractor extends Extractor<Integer> {
    @Override
    public Integer extractKey(byte[] record) {
      return Integer.valueOf(Strings.fromBytes(record).split("@")[0]);
    }

    @Override
    public Extractor<Integer> makeCopy() {
      return new MockExtractor();
    }
  }

  @Test
  public void testMerge() throws Exception {
    Bucket lBucket;
    Bucket rBucket;
    Bucket tBucket;

    String outputDir = getTestRoot() + "/2/output";

    final String bucket1Path = getTestRoot() + "/2/left";
    final String bucket2Path = getTestRoot() + "/2/right";
    final String bucket3Path = getTestRoot() + "/2/third";

    lBucket = Bucket.create(fs, bucket1Path, BytesWritable.class);
    rBucket = Bucket.create(fs, bucket2Path, BytesWritable.class);
    tBucket = Bucket.create(fs, bucket3Path, BytesWritable.class);

    fillWithData(lBucket, "part-1", "1@d", "4@b", "4@c", "7@a", "7@f", "9@a");
    fillWithData(rBucket, "part-1", "0@m", "1@a", "1@c", "7@a", "7@b", "7@c", "9@a", "9@b", "10@x");
    fillWithData(tBucket, "part-1", "3@f", "3@g", "7@z", "11@n");

    execute(new MSJTapAction<>(
            "TestMapSideJoin",
            getTestRoot() + "/tmp",
            new ExtractorsList<Integer>()
                .add(asStore(bucket1Path), new MockExtractor())
                .add(asStore(bucket2Path), new MockExtractor())
                .add(asStore(bucket3Path), new MockExtractor()),
            new MockJoiner(),
            asStore(outputDir),
            PartitionStructure.UNENFORCED
        )
    );

    Bucket oBucket = Bucket.open(fs, outputDir);
    List<byte[]> results = BucketHelper.readBucket(oBucket);

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.add("0@m");
    expectedResults.add("1@a");
    expectedResults.add("1@c");
    expectedResults.add("1@d");
    expectedResults.add("3@f");
    expectedResults.add("3@g");
    expectedResults.add("4@b");
    expectedResults.add("4@c");
    expectedResults.add("7@a");
    expectedResults.add("7@b");
    expectedResults.add("7@c");
    expectedResults.add("7@f");
    expectedResults.add("7@z");
    expectedResults.add("9@a");
    expectedResults.add("9@b");
    expectedResults.add("10@x");
    expectedResults.add("11@n");

    for (int i = 0; i < results.size(); i++) {
      assertEquals(expectedResults.get(i), Strings.fromBytes(results.get(i)));
    }

    assertEquals(BytesWritable.class, oBucket.getRecordClass());

  }


  @Test(expected = RuntimeException.class)
  public void testReverseSorting() throws Exception {
    Bucket lBucket;
    Bucket rBucket;

    String outputDir = getTestRoot() + "/2/output";

    final String bucket1Path = getTestRoot() + "/2/left";
    final String bucket2Path = getTestRoot() + "/2/right";

    lBucket = Bucket.create(fs, bucket1Path, BytesWritable.class);
    rBucket = Bucket.create(fs, bucket2Path, BytesWritable.class);

    fillWithData(lBucket, "part-1", "9@a", "7@f", "7@a", "4@c", "4@b", "1@d");
    fillWithData(rBucket, "part-1", "10@x", "9@b", "9@a", "7@c", "7@b", "7@a", "1@c", "1@a", "0@m");

    execute(new MSJTapAction<>(
        "test-tap",
        getTestRoot() + "/tmp",
        new ExtractorsList<Integer>()
            .add(asStore(bucket1Path), new MockExtractor())
            .add(asStore(bucket2Path), new MockExtractor()),
        new MockJoiner(),
        asStore(outputDir),
        PartitionStructure.UNENFORCED
    ));

    fail();
  }

  public static class MockJoiner extends MSJFunction<Integer> {

    public MockJoiner() {
      super(new Fields("bytes"));
    }

    @Override
    public void operate(FunctionCall functionCall, MSJGroup<Integer> group) {
      SortedSet<String> values = new TreeSet<String>();
      Set<String> keys = new HashSet<String>();

      for (int i = 0; i < group.getNumIterators(); i++) {
        Iterator<byte[]> iterator = group.getArgumentsIterator(i);

        while (iterator.hasNext()) {
          String[] fields = Strings.fromBytes(iterator.next()).split("@");
          keys.add(fields[0]);
          values.add(fields[1]);
        }
      }

      String key;

      if (keys.size() == 0) {
        if (values.size() != 0) {
          throw new RuntimeException("can only have no keys if there are no values!");
        } else {
          return;
        }
      }

      if (keys.size() == 1) {
        key = keys.iterator().next();
      } else {
        throw new RuntimeException("Can only have one key per group!:" + keys);
      }

      for (String value : values) {
        functionCall.getOutputCollector().add(new Tuple(new BytesWritable(Strings.toBytes(key + "@" + value))));
      }
    }
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