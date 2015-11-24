package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import cascading.flow.FlowProcess;

import com.liveramp.audience.generated.AudienceMember;
import com.liveramp.cascading_ext.Bytes;
import com.liveramp.commons.collections.map.MapBuilder;
import com.liveramp.importer.generated.ImportRecordID;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.BytesDataStore;
import com.rapleaf.cascading_ext.datastore.DataStores;
import com.rapleaf.cascading_ext.datastore.PartitionedDataStore;
import com.rapleaf.cascading_ext.datastore.PartitionedThriftDataStoreHelper;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.map_side_join.TIterator;
import com.rapleaf.cascading_ext.map_side_join.extractors.TByteArrayExtractor;
import com.rapleaf.cascading_ext.map_side_join.extractors.ThriftExtractor;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.merger.MSJGroup;
import com.rapleaf.cascading_ext.msj_tap.operation.MOMSJFunction;
import com.rapleaf.cascading_ext.msj_tap.operation.functioncall.MOMSJFunctionCall;
import com.rapleaf.cascading_ext.tap.bucket2.partitioner.AudienceMemberPartitioner;
import com.rapleaf.cascading_ext.test.TExtractorComparator;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.data_helpers.AudienceMemberHelper;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.test.BucketHelper;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.support.Strings;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.IdentitySumm;
import com.rapleaf.types.new_person_data.LRCField;
import com.rapleaf.types.new_person_data.PIN;
import com.rapleaf.types.new_person_data.PINAndOwners;
import com.rapleaf.types.new_person_data.StringList;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TestMOMSJTapAction extends WorkflowTestCase {

  public static final TByteArrayExtractor DIE_EID_EXTRACTOR = new TByteArrayExtractor(DustinInternalEquiv._Fields.EID);
  public static final TByteArrayExtractor ID_SUMM_EID_EXTRACTOR = new TByteArrayExtractor(IdentitySumm._Fields.EID);
  public static final TExtractorComparator<DustinInternalEquiv, BytesWritable> DIE_EID_COMPARATOR =
      new TExtractorComparator<DustinInternalEquiv, BytesWritable>(DIE_EID_EXTRACTOR);
  public static final TExtractorComparator<IdentitySumm, BytesWritable> ID_SUMM_EID_COMPARATOR =
      new TExtractorComparator<IdentitySumm, BytesWritable>(ID_SUMM_EID_EXTRACTOR);

  public static final PIN PIN1 = PIN.email("ben@gmail.com");
  public static final PIN PIN2 = PIN.email("ben@liveramp.com");
  public static final PIN PIN3 = PIN.email("ben@yahoo.com");


  public static final DustinInternalEquiv die1 = new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN1, 0);
  public static final DustinInternalEquiv die2 = new DustinInternalEquiv(ByteBuffer.wrap("2".getBytes()), PIN2, 0);
  public static final DustinInternalEquiv die3 = new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN3, 0);

  public static final IdentitySumm SUMM = new IdentitySumm(ByteBuffer.wrap("1".getBytes()), Lists.<PINAndOwners>newArrayList());
  public static final IdentitySumm SUMM_AFTER = new IdentitySumm(ByteBuffer.wrap("1".getBytes()), Lists.<PINAndOwners>newArrayList(
      new PINAndOwners(PIN1),
      new PINAndOwners(PIN3)
  ));

  private static final ByteBuffer EID1 = ByteBuffer.wrap(Strings.toBytes("1"));
  private static final ByteBuffer EID2 = ByteBuffer.wrap(Strings.toBytes("2"));

  private static final PIN EMAIL1 = PIN.email("test1@gmail.com");
  private static final PIN EMAIL2 = PIN.email("test2@gmail.com");
  private static final PIN EMAIL3 = PIN.email("test3@gmail.com");
  private static final PIN EMAIL4 = PIN.email("test4@gmail.com");

  private static final DustinInternalEquiv DIE1 = new DustinInternalEquiv(EID1, EMAIL1, 0);
  private static final DustinInternalEquiv DIE2 = new DustinInternalEquiv(EID2, EMAIL2, 0);
  private static final DustinInternalEquiv DIE3 = new DustinInternalEquiv(EID1, EMAIL3, 0);
  private static final DustinInternalEquiv DIE4 = new DustinInternalEquiv(EID2, EMAIL4, 0);


  enum Outputs {
    ONE,
    TWO,
    META
  }

  @Test
  public void testIt() throws Exception {

    BucketDataStore<DustinInternalEquiv> pins1 = builder().getBucketDataStore("pin1", DustinInternalEquiv.class);
    BucketDataStore<DustinInternalEquiv> pins2 = builder().getBucketDataStore("pin2", DustinInternalEquiv.class);

    setupInput(pins1, pins2);

    BucketDataStore<PIN> output1 = builder().getBucketDataStore("output1", PIN.class);
    BucketDataStore<BytesWritable> output2 = builder().getBucketDataStore("output2", BytesWritable.class);
    BucketDataStore<StringList> metaOut = builder().getBucketDataStore("meta", StringList.class);

    MOMSJTapAction<Outputs, BytesWritable> action = new MOMSJTapAction<Outputs, BytesWritable>(
        "token",
        getTestRoot() + "/tmp",
        new ExtractorsList<BytesWritable>()
            .add(pins1, DIE_EID_EXTRACTOR)
            .add(pins2, DIE_EID_EXTRACTOR),
        new TestFunction(),
        new MapBuilder<Outputs, BucketDataStore>()
            .put(Outputs.ONE, output1)
            .put(Outputs.TWO, output2).get(),
        Collections.singletonMap(Outputs.META, metaOut)
    );

    execute(action);

    assertCollectionEquivalent(Lists.newArrayList(EMAIL1, EMAIL2, EMAIL3, EMAIL4), HRap.getValuesFromBucket(output1));
    assertCollectionEquivalent(
        Lists.newArrayList(
            Bytes.byteBufferToBytesWritable(EID1),
            Bytes.byteBufferToBytesWritable(EID2),
            Bytes.byteBufferToBytesWritable(EID1),
            Bytes.byteBufferToBytesWritable(EID2)),
        HRap.getValuesFromBucket(output2));

    assertCollectionEquivalent(Lists.newArrayList(
        new StringList(Lists.newArrayList("b")),
        new StringList(Lists.newArrayList("b")),
        new StringList(Lists.newArrayList("c"))
    ), HRap.getValuesFromBucket(metaOut));

    assertFalse(metaOut.getBucket().hasIndex());

  }

  @Test
  public void testFailInvalidPartition() throws Exception {

    BucketDataStore<DustinInternalEquiv> pins1 = builder().getBucketDataStore("pin1", DustinInternalEquiv.class);
    BucketDataStore<DustinInternalEquiv> pins2 = builder().getBucketDataStore("pin2", DustinInternalEquiv.class);

    setupInput(pins1, pins2);

    BucketDataStore<PIN> output1 = builder().getBucketDataStore("output1", PIN.class);
    BucketDataStore<BytesWritable> output2 = builder().getBucketDataStore("output2", BytesWritable.class);
    BucketDataStore<StringList> metaOut = builder().getBucketDataStore("meta", StringList.class);

    MOMSJTapAction<Outputs, BytesWritable> action = new MOMSJTapAction<Outputs, BytesWritable>(
        "token",
        getTestRoot() + "/tmp",
        new ExtractorsList<BytesWritable>()
            .add(pins1, DIE_EID_EXTRACTOR)
            .add(pins2, DIE_EID_EXTRACTOR),
        new TestFunction(),
        new MapBuilder<Outputs, BucketDataStore>()
            .put(Outputs.ONE, output1)
            .put(Outputs.TWO, output2)
            .put(Outputs.META, metaOut)
            .get()
    );

    try {
      execute(action);
      fail();
    } catch (Exception e) {
      //  good
    }

  }

  @Test
  public void testRetainSplits() throws Exception {

    PartitionedDataStore<AudienceMember> ams1 = builder().getPartitionedDataStore(
        "ams1",
        AudienceMember.class
    );

    AudienceMember am1 = makeAudienceMember(0l, 1);

    AudienceMemberPartitioner.BaseAMPartitioner<AudienceMember> partitioner = new AudienceMemberPartitioner.BaseAMPartitioner<AudienceMember>(AudienceMember.class);
    PartitionedThriftDataStoreHelper.writeSortedToNewVersion(
        ams1,
        partitioner,
        AudienceMember.class,
        am1
    );

    BucketDataStore<AudienceMember> out = builder().getBucketDataStore("output", AudienceMember.class);

    execute(new IdentityMOMSJ("identity", getTestRoot() + "/tmp", ams1, out));

    assertEquals(1, HRap.getValuesFromBucket(DataStores.bucket(out.getPath(), "/custom-split", AudienceMember.class)).size());

  }

  private enum OutputEnum {
    OUT1
  }

  private static class IdentityOutput extends MOMSJFunction<OutputEnum, BytesWritable> {

    @Override
    public void operate(MOMSJFunctionCall<OutputEnum> functionCall, MSJGroup<BytesWritable> group) {

      TIterator<AudienceMember> iter1 = group.getThriftIterator(0, new AudienceMember());

      while (iter1.hasNext()) {
        functionCall.emit(OutputEnum.OUT1, "custom-split", iter1.next());
      }

    }
  }

  private static class IdentityMOMSJ extends MOMSJTapAction<OutputEnum, BytesWritable> {

    public IdentityMOMSJ(String checkpointToken, String tmpRoot,
                         final PartitionedDataStore<AudienceMember> ams1,
                         BucketDataStore<AudienceMember> out) throws IOException {
      super(checkpointToken, tmpRoot,
          new ExtractorsList<BytesWritable>()
              .add(ams1, new SimpleConfFactory(ams1)),
          new IdentityOutput(),
          Collections.singletonMap(OutputEnum.OUT1, out)
      );
    }

    private static class SimpleConfFactory implements ConfFactory<BytesWritable> {
      private final PartitionedDataStore<AudienceMember> ams1;

      public SimpleConfFactory(PartitionedDataStore<AudienceMember> ams1) {
        this.ams1 = ams1;
      }

      @Override
      public InputConf<BytesWritable> getInputConf() {
        try {
          return ams1.getInputConf(new TByteArrayExtractor(AudienceMember._Fields.MEMBER_ID), Collections.singletonMap(0l, 0));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static byte[] makeAudienceKey(byte number) {
    return new byte[]{number, number, number, number};
  }

  public static AudienceMember makeAudienceMember(Long audienceId, Integer memberNumber) {
    byte[] bytes = makeAudienceKey(memberNumber.byteValue());
    return new AudienceMember(audienceId, 0, ByteBuffer.wrap(bytes), ByteBuffer.wrap(AudienceMemberHelper.computeMemberID(audienceId, bytes)),
        new HashMap<ImportRecordID, List<LRCField>>(), new HashMap<PIN, List<ImportRecordID>>());
  }


  @Test
  public void testStrings() throws Exception {

    BucketDataStore<DustinInternalEquiv> pins1 = builder().getBucketDataStore("pin1", DustinInternalEquiv.class);

    ThriftBucketHelper.writeToBucketAndSort(pins1.getBucket(),
        new StringComparator(),
        DIE1,
        DIE2
    );

    BucketDataStore<DustinInternalEquiv> output1 = builder().getBucketDataStore("output1", DustinInternalEquiv.class);

    MOMSJTapAction<Outputs, String> action = new MOMSJTapAction<>(
        "token",
        getTestRoot() + "/tmp",
        new ExtractorsList<String>()
            .add(pins1, new StringExtractor()),
        new TestStrings(),
        new MapBuilder<Outputs, BucketDataStore>()
            .put(Outputs.ONE, output1)
            .get()
    );

    execute(action);

  }

  private static class StringComparator implements Comparator<DustinInternalEquiv> {

    @Override
    public int compare(DustinInternalEquiv o1, DustinInternalEquiv o2) {
      return Strings.fromBytes(o1.get_eid()).compareTo(Strings.fromBytes(o1.get_eid()));
    }
  }

  private static class StringExtractor extends ThriftExtractor<DustinInternalEquiv, String> {

    @Override
    public String extractThriftKey(DustinInternalEquiv thriftObject) {
      return Strings.fromBytes(thriftObject.get_eid());
    }

    @Override
    public DustinInternalEquiv getThriftObj() {
      return new DustinInternalEquiv();
    }

    @Override
    public Extractor<String> makeCopy() {
      return new StringExtractor();
    }
  }


  private void setupInput(BucketDataStore<DustinInternalEquiv> pins1, BucketDataStore<DustinInternalEquiv> pins2) throws java.io.IOException, org.apache.thrift.TException {
    ThriftBucketHelper.writeToBucketAndSort(pins1.getBucket(),
        DIE_EID_COMPARATOR,
        DIE1,
        DIE2
    );

    ThriftBucketHelper.writeToBucketAndSort(pins2.getBucket(),
        DIE_EID_COMPARATOR,
        DIE3,
        DIE4
    );
  }

  private static class TestStrings extends MOMSJFunction<Outputs, String> {

    public TestStrings() {
      super();
    }

    @Override
    public void operate(MOMSJFunctionCall<Outputs> functionCall, MSJGroup<String> group) {

      TIterator<DustinInternalEquiv> iter1 = group.getThriftIterator(0, new DustinInternalEquiv());

      while (iter1.hasNext()) {
        functionCall.emit(Outputs.ONE, iter1.next());
      }

    }

  }


  private static class TestFunction extends MOMSJFunction<Outputs, BytesWritable> {

    public TestFunction() {
      super();
    }

    @Override
    public void operate(MOMSJFunctionCall<Outputs> functionCall, MSJGroup<BytesWritable> group) {

      TIterator<DustinInternalEquiv> iter1 = group.getThriftIterator(0, new DustinInternalEquiv());
      TIterator<DustinInternalEquiv> iter2 = group.getThriftIterator(1, new DustinInternalEquiv());

      emitValues(functionCall, iter1);
      emitValues(functionCall, iter2);

      functionCall.emit(Outputs.META, new StringList(Lists.newArrayList("b")));

    }

    @Override
    public void flush(FlowProcess flowProcess, MOMSJFunctionCall<Outputs> msjFunctionCall) {
      msjFunctionCall.emit(Outputs.META, new StringList(Lists.newArrayList("c")));
    }

  }


  private static void emitValues(MOMSJFunctionCall<Outputs> functionCall, TIterator<DustinInternalEquiv> iter1) {
    while (iter1.hasNext()) {
      DustinInternalEquiv val = iter1.next();
      functionCall.emit(Outputs.ONE, val.get_pin());
      functionCall.emit(Outputs.TWO, new BytesWritable(val.get_eid()));
    }
  }

  private enum MyEnum {
    MERGED, REPEATED, ODD, NEVER_USED
  }

  @Test
  public void testPostCreationOfSubBuckets() throws Exception {
    Bucket lBucket;
    Bucket rBucket;
    Bucket tBucket;

    lBucket = Bucket.create(fs, getTestRoot() + "/2/left", BytesWritable.class);
    rBucket = Bucket.create(fs, getTestRoot() + "/2/right", BytesWritable.class);
    tBucket = Bucket.create(fs, getTestRoot() + "/2/third", BytesWritable.class);

    fillWithData(lBucket, "part-1", "1@d", "4@b", "4@c", "7@a", "7@f", "9@a");
    fillWithData(rBucket, "part-1", "0@m", "1@a", "1@c", "7@a", "7@b", "7@c", "9@a", "9@b", "10@x");
    fillWithData(tBucket, "part-1", "3@f", "3@g", "7@z", "11@n");

    BucketDataStore inputDS1 = new BytesDataStore(fs, "store1", lBucket.getRoot().toString(), "");
    BucketDataStore inputDS2 = new BytesDataStore(fs, "store2", rBucket.getRoot().toString(), "");
    BucketDataStore inputDS3 = new BytesDataStore(fs, "store3", tBucket.getRoot().toString(), "");

    BucketDataStore outputDS1 = new BytesDataStore(fs, "store1", getTestRoot(), "/store1");
    BucketDataStore outputDS2 = new BytesDataStore(fs, "store2", getTestRoot(), "/store2");
    BucketDataStore outputDS3 = new BytesDataStore(fs, "store3", getTestRoot(), "/store3");
    BucketDataStore outputDS4 = new BytesDataStore(fs, "store4", getTestRoot(), "/store4");

    //
    // This should run the MSJ and create individual buckets
    // for all of the output sub-directories
    //

    Map<MyEnum, BucketDataStore> categoryToBucketDataStore = new MapBuilder<MyEnum, BucketDataStore>()
        .put(MyEnum.MERGED, outputDS1)
        .put(MyEnum.ODD, outputDS2)
        .put(MyEnum.REPEATED, outputDS3)
        .put(MyEnum.NEVER_USED, outputDS4)
        .get();

    execute(new MOMSJTapAction<>(
            "TestMOMSJ",
            getTestRoot() + "/tmp",
            new ExtractorsList<Integer>()
                .add(inputDS1, new MockExtractor())
                .add(inputDS2, new MockExtractor())
                .add(inputDS3, new MockExtractor()),
            new MockMOJoiner(),
            categoryToBucketDataStore,
            Maps.<MyEnum, BucketDataStore>newHashMap()
        )
    );

    verifyMerged(BucketHelper.readBucket(outputDS1.getBucket()));

    verifyOdd(BucketHelper.readBucket(outputDS2.getBucket()));

    verifyRepeated(BucketHelper.readBucket(outputDS3.getBucket()));

    verifyNeverUsed(BucketHelper.readBucket(outputDS4.getBucket()));

    verifyRecordTypes();
  }

  /*public void testMerge() throws Exception {
    Bucket lBucket;
    Bucket rBucket;
    Bucket tBucket;

    String outputDir = getTestRoot() + "/2/output";

    lBucket = Bucket.create(fs, getTestRoot() + "/2/left");
    rBucket = Bucket.create(fs, getTestRoot() + "/2/right");
    tBucket = Bucket.create(fs, getTestRoot() + "/2/third");

    fillWithData(lBucket, "part-1", "1@d", "4@b", "4@c", "7@a", "7@f", "9@a");
    fillWithData(rBucket, "part-1", "0@m", "1@a", "1@c", "7@a", "7@b", "7@c", "9@a", "9@b", "10@x");
    fillWithData(tBucket, "part-1", "3@f", "3@g", "7@z", "11@n");

    new MOMapSideJoin<Integer, MyEnum>(
        "TestMOMSJ",
        Arrays.<Extractor<Integer>>asList(new MockExtractor(), new MockExtractor(), new MockExtractor()),
        new MockMOJoiner(),
        Arrays.asList(lBucket, rBucket, tBucket),
        outputDir
    ).run();

    // assert results from 'merged' sub-directory
    verifyMerged(BucketHelper.readBucket(Bucket.open(fs, outputDir).getSubBucket(MOJoiner.getRelPath(MyEnum.MERGED))));

    // assert results from 'repeated' sub-directory
    verifyRepeated(BucketHelper.readBucket(MOJoiner.getSubBucket(Bucket.open(fs, outputDir), MyEnum.REPEATED)));

    // assert results from 'repeated' sub-directory
    verifyOdd(BucketHelper.readBucket(MOJoiner.getSubBucket(Bucket.open(fs, outputDir), MyEnum.ODD)));

    // assert that never used sub-directory sub-bucket exists but is empty
    verifyNeverUsed(BucketHelper.readBucket(MOJoiner.getSubBucket(Bucket.open(fs, outputDir), MyEnum.NEVER_USED)));
  }*/

  private void verifyMerged(List<byte[]> mergedResults) {
    List<String> expectedMergedRecords = new ArrayList<String>();
    expectedMergedRecords.add("0@m");
    expectedMergedRecords.add("1@a");
    expectedMergedRecords.add("1@c");
    expectedMergedRecords.add("1@d");
    expectedMergedRecords.add("3@f");
    expectedMergedRecords.add("3@g");
    expectedMergedRecords.add("4@b");
    expectedMergedRecords.add("4@c");
    expectedMergedRecords.add("7@a");
    expectedMergedRecords.add("7@b");
    expectedMergedRecords.add("7@c");
    expectedMergedRecords.add("7@f");
    expectedMergedRecords.add("7@z");
    expectedMergedRecords.add("9@a");
    expectedMergedRecords.add("9@b");
    expectedMergedRecords.add("10@x");
    expectedMergedRecords.add("11@n");

    for (int i = 0; i < expectedMergedRecords.size(); i++) {
      assertEquals(expectedMergedRecords.get(i), Strings.fromBytes(mergedResults.get(i)));
    }
  }

  private void verifyRepeated(List<byte[]> repeatedRecords) {
    List<String> expectedRepeatedRecords = new ArrayList<String>();
    expectedRepeatedRecords.add("7@a");
    expectedRepeatedRecords.add("9@a");

    for (int i = 0; i < expectedRepeatedRecords.size(); i++) {
      assertEquals(expectedRepeatedRecords.get(i), Strings.fromBytes(repeatedRecords.get(i)));
    }
  }

  private void verifyOdd(List<byte[]> recordsWithOddKeys) {
    List<String> expectedRecordsWithOddKeys = new ArrayList<String>();
    expectedRecordsWithOddKeys.add("1@d");
    expectedRecordsWithOddKeys.add("1@a");
    expectedRecordsWithOddKeys.add("1@c");
    expectedRecordsWithOddKeys.add("3@f");
    expectedRecordsWithOddKeys.add("3@g");
    expectedRecordsWithOddKeys.add("7@a");
    expectedRecordsWithOddKeys.add("7@f");
    expectedRecordsWithOddKeys.add("7@a");
    expectedRecordsWithOddKeys.add("7@b");
    expectedRecordsWithOddKeys.add("7@c");
    expectedRecordsWithOddKeys.add("7@z");
    expectedRecordsWithOddKeys.add("9@a");
    expectedRecordsWithOddKeys.add("9@a");
    expectedRecordsWithOddKeys.add("9@b");
    expectedRecordsWithOddKeys.add("11@n");

    for (int i = 0; i < expectedRecordsWithOddKeys.size(); i++) {
      assertEquals(expectedRecordsWithOddKeys.get(i), Strings.fromBytes(recordsWithOddKeys.get(i)));
    }
  }

  private void verifyNeverUsed(List<byte[]> neverUsed) {
    assertTrue(neverUsed.isEmpty());
  }

  private static class MockMOJoiner extends MOMSJFunction<MyEnum, Integer> {

    @Override
    public void operate(MOMSJFunctionCall<MyEnum> functionCall, MSJGroup<Integer> group) {
      SortedSet<String> values = new TreeSet<String>();
      Set<String> keys = new HashSet<String>();

      for (int i = 0; i < group.getNumIterators(); i++) {
        Iterator<byte[]> iterator = group.getArgumentsIterator(i);

        while (iterator.hasNext()) {
          String value = Strings.fromBytes(iterator.next());
          String[] fields = value.split("@");
          keys.add(fields[0]);

          if (values.contains(fields[1])) {
            functionCall.emit(MyEnum.REPEATED, new BytesWritable(Strings.toBytes(value)));
          } else {
            values.add(fields[1]);
          }

          if (Integer.valueOf(fields[0]) % 2 != 0) {
            functionCall.emit(MyEnum.ODD, new BytesWritable(Strings.toBytes(value)));
          }
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
        functionCall.emit(MyEnum.MERGED, new BytesWritable(Strings.toBytes(key + "@" + value)));
      }
    }

  }

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


  public void verifyRecordTypes() throws Exception {
    assertEquals(BytesWritable.class, Bucket.open(fs, getTestRoot() + "/store1").getRecordClass());
    assertEquals(BytesWritable.class, Bucket.open(fs, getTestRoot() + "/store2").getRecordClass());
    assertEquals(BytesWritable.class, Bucket.open(fs, getTestRoot() + "/store3").getRecordClass());
    assertEquals(BytesWritable.class, Bucket.open(fs, getTestRoot() + "/store4").getRecordClass());
  }

}