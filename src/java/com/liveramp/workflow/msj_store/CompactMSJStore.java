package com.liveramp.workflow.msj_store;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.liveramp.cascading_tools.properties.PropertiesBuilder;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.merger.MSJGroup;
import com.rapleaf.cascading_ext.msj_tap.operation.MSJFunction;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.tap.bucket2.ThriftBucketScheme;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.ActionCallback;
import com.rapleaf.cascading_ext.workflow2.action.ExtractorsList;
import com.rapleaf.cascading_ext.workflow2.action.MSJTapAction;

public class CompactMSJStore<T extends Comparable, K extends Comparable> extends MSJTapAction<K> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactMSJStore.class);

  //  TODO ugly
  private static <K extends Comparable> Fetch fetch(Class<K> type) throws InstantiationException, IllegalAccessException {
    if (TBase.class.isAssignableFrom(type)) {
      return new FetchThrift(type);
    } else if (BytesWritable.class.isAssignableFrom(type)) {
      return new FetchBytes();
    } else {
      throw new RuntimeException();
    }
  }

  public static class FetchBytes implements Fetch<BytesWritable> {
    @Override
    public Iterator fetch(final MSJGroup<BytesWritable> group) {
      final Iterator<byte[]> iter = group.getArgumentsIterator(0);
      return new Iterator<BytesWritable>() {
        @Override
        public boolean hasNext() {
          return iter.hasNext();
        }

        @Override
        public BytesWritable next() {
          return new BytesWritable(iter.next());
        }

        @Override
        public void remove() {
          iter.remove();
        }
      };
    }
  }

  private interface Fetch<K extends Comparable> extends Serializable {
    Iterator fetch(MSJGroup<K> group);
  }

  public static class FetchThrift<K extends TBase> implements Fetch<K> {

    private Class<K> type;
    private transient K proto;

    public FetchThrift(Class<K> type) throws IllegalAccessException, InstantiationException {
      this.type = type;
    }

    @Override
    public Iterator fetch(MSJGroup<K> group) {
      if (proto == null) {
        try {
          proto = type.newInstance();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      return group.getThriftIterator(0, proto);
    }
  }


  public CompactMSJStore(String checkpointToken, String tmpDir, Class<T> outputType, final MSJDataStore<K> store, BucketDataStore<T> tempStore) throws InstantiationException, IllegalAccessException {
    super(checkpointToken, tmpDir, new PropertiesBuilder()
            .mapHeapSize(950)
            .reduceHeapSize(950)
            .build(),
        new ExtractorsList().add(store, store.getExtractor()),
        new AllJoin<K>(outputType),
        tempStore,
        PartitionStructure.UNENFORCED,
        new ActionCallback.Default() {
          @Override
          public void prepare(Action.PreExecuteContext context) throws IOException {
            store.getStore().acquireBaseCreationAttempt();
          }
        }
    );
  }

  private static class AllJoin<K extends Comparable> extends MSJFunction<K> {

    private final Fetch fetch;

    public AllJoin(Class fieldType) throws IllegalAccessException, InstantiationException {
      super(new Fields(ThriftBucketScheme.getFieldName(fieldType)));
      fetch = fetch(fieldType);
    }

    @Override
    public void operate(FunctionCall functionCall, MSJGroup<K> group) {
      Iterator iter = fetch.fetch(group);
      TupleEntryCollector collector = functionCall.getOutputCollector();
      while (iter.hasNext()) {
        collector.add(new Tuple(iter.next()));
      }
    }
  }
}
