package com.rapleaf.cascading_ext.workflow2.action;

import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.rapleaf.support.SerializationHelper;

public class SortByThriftField extends SubAssembly {
  private static final Fields SERIALIZED_FIELD = new Fields("__serialized_field");

  public SortByThriftField(Pipe pipe, Fields fields) {
    if (fields.size() != 1) {
      throw new IllegalArgumentException("Can only sort by one thrift field!");
    }

    pipe = new Each(pipe, fields, new SerializeValue(), Fields.ALL);
    pipe = new GroupBy(pipe, SERIALIZED_FIELD);
    pipe = new Each(pipe, Fields.VALUES, new Identity());

    setTails(pipe);
  }

  private static class SerializeValue extends BaseOperation implements Function {
    private static transient TSerializer serializer = null;

    private SerializeValue() {
      super(SERIALIZED_FIELD);
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
      super.prepare(flowProcess, operationCall);
      serializer = SerializationHelper.getFixedSerializer();
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      TBase obj = (TBase) functionCall.getArguments().get(0);
      try {
        functionCall.getOutputCollector().add(new Tuple(new BytesWritable(serializer.serialize(obj))));
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
