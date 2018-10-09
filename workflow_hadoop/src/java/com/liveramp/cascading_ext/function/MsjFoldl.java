package com.liveramp.cascading_ext.function;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBase;

import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.fields.SingleField;
import com.liveramp.java_support.functional.Fns;
import com.liveramp.java_support.functional.ReducerFn;
import com.rapleaf.cascading_ext.msj_tap.merger.MSJGroup;
import com.rapleaf.cascading_ext.msj_tap.operation.MSJFunction;

public class MsjFoldl<ACC extends TBase, ELEM extends TBase> extends MSJFunction<BytesWritable> {
  private final ReducerFn<ACC, ELEM> reducerFn;
  private final Class<ACC> initialProto;
  private final Class<ELEM> elemProto;

  public MsjFoldl(ReducerFn<ACC, ELEM> reducerFn, SingleField<String> outputField, Class<ACC> initialProto, Class<ELEM> elemProto) {
    super(outputField);
    this.reducerFn = reducerFn;
    this.initialProto = initialProto;
    this.elemProto = elemProto;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void operate(FunctionCall functionCall, MSJGroup<BytesWritable> group) {
    List<Iterator<ELEM>> iterators = Lists.newArrayList();
    try {
      for (int i = 0; i < group.getNumIterators(); i++) {
        iterators.add(group.getThriftIterator(i, elemProto.newInstance()));
      }
      final Iterator<ELEM> allIterators = Iterators.concat(iterators.iterator());

      final ACC initial = initialProto.newInstance();
      final ACC res = Fns.foldl(reducerFn, initial, allIterators);

      functionCall.getOutputCollector().add(new Tuple(res));

    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
