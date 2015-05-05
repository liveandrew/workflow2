package com.rapleaf.cascading_ext.workflow2.action;

import java.nio.ByteBuffer;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import com.liveramp.cascading_ext.Bytes;
import com.liveramp.commons.collections.map.MapBuilder;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.TIterator;
import com.rapleaf.cascading_ext.msj_tap.merger.MSJGroup;
import com.rapleaf.cascading_ext.msj_tap.operation.MOMSJFunction;
import com.rapleaf.cascading_ext.msj_tap.operation.functioncall.MOMSJFunctionCall;
import com.rapleaf.cascading_ext.msj_tap.tap.MSJFixtures;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.support.Strings;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.PIN;

public class TestMOMSJTapAction extends WorkflowTestCase {

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
    TWO
  }

  @Test
  public void testIt() throws Exception {

    BucketDataStore<DustinInternalEquiv> pins1 = builder().getBucketDataStore("pin1", DustinInternalEquiv.class);
    BucketDataStore<DustinInternalEquiv> pins2 = builder().getBucketDataStore("pin2", DustinInternalEquiv.class);

    ThriftBucketHelper.writeToBucketAndSort(pins1.getBucket(),
        MSJFixtures.DIE_EID_COMPARATOR,
        DIE1,
        DIE2
    );

    ThriftBucketHelper.writeToBucketAndSort(pins2.getBucket(),
        MSJFixtures.DIE_EID_COMPARATOR,
        DIE3,
        DIE4
    );

    BucketDataStore<PIN> output1 = builder().getBucketDataStore("output1", PIN.class);
    BucketDataStore<BytesWritable> output2 = builder().getBucketDataStore("output2", BytesWritable.class);

    MOMSJTapAction<Outputs> action = new MOMSJTapAction<Outputs>(
        "token",
        getTestRoot() + "/tmp",
        new ExtractorsList<BytesWritable>()
            .add(pins1, MSJFixtures.DIE_EID_EXTRACTOR)
            .add(pins2, MSJFixtures.DIE_EID_EXTRACTOR),
        new TestFunction(),
        new MapBuilder<Outputs, BucketDataStore>()
            .put(Outputs.ONE, output1)
            .put(Outputs.TWO, output2).get()

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

    }

  }

  private static void emitValues(MOMSJFunctionCall<Outputs> functionCall, TIterator<DustinInternalEquiv> iter1) {
    while (iter1.hasNext()) {
      DustinInternalEquiv val = iter1.next();
      functionCall.emit(Outputs.ONE, val.get_pin());
      functionCall.emit(Outputs.TWO, new BytesWritable(val.get_eid()));
    }
  }
}