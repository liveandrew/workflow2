package com.liveramp.cascading_ext.function;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TException;
import org.junit.Test;

import com.liveramp.cascading_ext.fields.SingleField;
import com.liveramp.cascading_ext.util.FieldHelper;
import com.liveramp.java_support.functional.ReducerFn;
import com.liveramp.util.generated.CounterContainer;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.extractors.TByteArrayExtractor;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.cascading_ext.workflow2.action.ExtractorsList;
import com.rapleaf.cascading_ext.workflow2.action.MSJTapAction;
import com.rapleaf.formats.test.ThriftBucketHelper;

public class TestMsjFoldl extends WorkflowTestCase {

  @Test
  public void testIt() throws IOException, TException {
    final BucketDataStore<CounterContainer> sortedLeft = builder().getBucketDataStore("sorted-left", CounterContainer.class);
    final BucketDataStore<CounterContainer> sortedRight = builder().getBucketDataStore("sorted-right", CounterContainer.class);
    final BucketDataStore<CounterContainer> out = builder().getBucketDataStore("out", CounterContainer.class);

    ThriftBucketHelper.writeToBucketAndSort(sortedLeft.getBucket(), new StringValueComparator(),
        new CounterContainer("a", 1),
        new CounterContainer("b", 2),
        new CounterContainer("c", 3),
        new CounterContainer("d", 4),
        new CounterContainer("e", 5));

    ThriftBucketHelper.writeToBucketAndSort(sortedRight.getBucket(), new StringValueComparator(),
        new CounterContainer("a", 1),
        new CounterContainer("a", 2),
        new CounterContainer("a", 3),
        new CounterContainer("d", 4),
        new CounterContainer("e", 5));

    execute(new JoinCounters(getTestRoot(), sortedLeft, sortedRight, new SumCounter(), FieldHelper.singleFieldOf(CounterContainer.class), out));

    List<CounterContainer> expected = Lists.newArrayList(
        new CounterContainer("a", 7),
        new CounterContainer("b", 2),
        new CounterContainer("c", 3),
        new CounterContainer("d", 8),
        new CounterContainer("e", 10)
    );

    org.junit.Assert.assertEquals(expected, ThriftBucketHelper.readBucket(out.getBucket(), CounterContainer.class));
  }

  private static class SumCounter implements ReducerFn<CounterContainer, CounterContainer> {
    @Override
    public CounterContainer reduce(CounterContainer left, CounterContainer right) {
      if (!left.is_set_name()) {
        left.set_name(right.get_name());
      }
      return left.set_value(left.get_value() + right.get_value());
    }
  }

  private static class StringValueComparator implements Comparator<CounterContainer> {
    @Override
    public int compare(CounterContainer o1, CounterContainer o2) {
      return o1.get_name().compareTo(o2.get_name());
    }
  }

  private static class JoinCounters extends MSJTapAction<BytesWritable> {

    public JoinCounters(String tmpRoot,
                        BucketDataStore<CounterContainer> left,
                        BucketDataStore<CounterContainer> right,
                        ReducerFn<CounterContainer, CounterContainer> reducerFn,
                        SingleField<String> outputField,
                        BucketDataStore<CounterContainer> out

                        ) {
      super("join-counters", tmpRoot,
          new ExtractorsList<BytesWritable>()
            .add(left, new TByteArrayExtractor(CounterContainer._Fields.NAME))
            .add(right, new TByteArrayExtractor(CounterContainer._Fields.NAME)),
          new MsjFoldl<>(reducerFn, outputField, CounterContainer.class, CounterContainer.class),
          out,
          PartitionStructure.UNENFORCED);
    }
  }

}