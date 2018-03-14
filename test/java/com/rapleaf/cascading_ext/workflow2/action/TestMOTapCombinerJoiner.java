package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBase;
import org.junit.Test;

import cascading.tuple.TupleEntry;

import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.commons.collections.map.MapBuilder;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.map_side_join.MapSideJoinAggregator;
import com.rapleaf.cascading_ext.map_side_join.extractors.BytesWritableFromByteArray;
import com.rapleaf.cascading_ext.msj_tap.MOTapCombinerJoiner;
import com.rapleaf.cascading_ext.msj_tap.merger.MSJGroup;
import com.rapleaf.cascading_ext.msj_tap.operation.functioncall.MOMSJFunctionCall;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.formats.test.BucketHelper;
import com.rapleaf.types.new_person_data.StringList;

public class TestMOTapCombinerJoiner extends WorkflowTestCase {

  @Test
  public void testIt() throws IOException {

    DataStoreBuilder builder = new DataStoreBuilder(FileSystemHelper.getFS(), getTestRoot() + "/buckets");

    BucketDataStore<BytesWritable> store = builder.getBucketDataStore("store1", BytesWritable.class);
    BucketDataStore<StringList> output1 = builder.getBucketDataStore("output1", StringList.class);
    BucketDataStore<StringList> output2 = builder.getBucketDataStore("output2", StringList.class);

    byte[] key1 = {0, 0};
    byte[] key2 = {0, 1};
    byte[] key3 = {1, 0};

    BucketHelper.writeSortedAsBytesWritablesToBucket(
        store.getBucket(),
        key1,
        key1,
        key2,
        key3
    );

    MOMSJTapAction<OutputType, BytesWritable> msj = new MOMSJTapAction<>(
        "test-combiner-joiner",
        getTestRoot()+"/tmp",
        new ExtractorsList<BytesWritable>().add(store, new BytesWritableFromByteArray()),
        new MockMOCombinerJoiner(),
        Maps.<OutputType, BucketDataStore>newHashMap(),
        MapBuilder
            .<OutputType, BucketDataStore>of(OutputType.LIST1, output1)
            .put(OutputType.LIST2, output2)
            .get()

    );

    execute(msj);

    List<StringList> valuesFromBucket1 = HRap.getValuesFromBucket(output1);
    assertCollectionEquivalent(
        Lists.newArrayList(
            new StringList(Lists.newArrayList(Arrays.toString(key1), Arrays.toString(key2))),
            new StringList(Lists.newArrayList(Arrays.toString(key3)))),
        valuesFromBucket1
    );

    List<StringList> valuesFromBucket2 = HRap.getValuesFromBucket(output2);
    assertCollectionEquivalent(
        Lists.newArrayList(
            new StringList(Lists.newArrayList(Arrays.toString(key1))),
            new StringList(Lists.newArrayList(Arrays.toString(key2))),
            new StringList(Lists.newArrayList(Arrays.toString(key3))
            )),
        valuesFromBucket2
    );
  }

  private enum OutputType {
    LIST1, LIST2
  }

  static class MockMOCombinerJoiner extends MOTapCombinerJoiner<BytesWritable, OutputType> {
    public MockMOCombinerJoiner() {
      super(
          MapBuilder.<OutputType, KeyExtractor>of(OutputType.LIST1, new MockKeyExtractor()).get(),
          MapBuilder.<OutputType, MapSideJoinAggregator<TBase>>of(OutputType.LIST1, new MockAggregator()).get(),
          1
      );
    }

    @Override
    public void operateInternal(MOMSJFunctionCall<OutputType> functionCall, MSJGroup<BytesWritable> group) {
      BytesWritable key = group.getKey();
      try {
        emit(functionCall, OutputType.LIST1, new StringList(Lists.newArrayList(Arrays.toString(Bytes.getBytes(key)))));
        emit(functionCall, OutputType.LIST2, new StringList(Lists.newArrayList(Arrays.toString(Bytes.getBytes(key)))));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public static class MockAggregator extends MapSideJoinAggregator<TBase> {
      @Override
      public StringList initialize() {
        return new StringList(Lists.<String>newArrayList());
      }

      @Override
      public TBase partialAggregate(TBase aggregate, TupleEntry nextValue) {
        StringList list = (StringList)nextValue.getObject("value");
        for (String eid : list.get_strings()) {
          ((StringList)aggregate).add_to_strings(eid);
        }
        return aggregate;
      }
    }

    public static class MockKeyExtractor implements KeyExtractor {
      @Override
      public ByteWritable extractKey(TBase eidList) {
        return new ByteWritable((byte) ((StringList)eidList).get_strings().iterator().next().charAt(0));
      }
    }
  }
}
