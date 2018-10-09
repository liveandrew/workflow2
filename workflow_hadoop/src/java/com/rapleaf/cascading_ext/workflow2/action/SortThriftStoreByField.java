package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;

import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.liveramp.cascading_ext.util.FieldHelper;
import com.rapleaf.cascading_ext.assembly.SortByThriftField;
import com.rapleaf.cascading_ext.datastore.UnitDataStore;
import com.rapleaf.cascading_ext.function.ExpandThrift;
import com.rapleaf.cascading_ext.workflow2.CascadingAction2;

/**
 * Sorts any datastore by any field (thrift, binary, int, etc.) selecting GroupBy or SortByThriftField when appropriate.
 * Automatically fails when you try to sort by a list, map, etc.
 *
 * @param <T> thrift type of datastore
 */
public class SortThriftStoreByField<T extends TBase<?, ?>> extends CascadingAction2 {
  public SortThriftStoreByField(
      String checkpointToken,
      String tmpRoot,
      UnitDataStore<T> input,
      UnitDataStore<T> sortedOutput,
      Class<T> thriftClass,
      TFieldIdEnum field,
      Map<Object, Object> properties) throws IllegalAccessException, InstantiationException {
    super(checkpointToken, tmpRoot, properties);


    FieldMetaData fieldMetaData = FieldMetaData.getStructMetaDataMap(thriftClass).get(field);

    if (fieldMetaData == null) {
      throw new RuntimeException("Field " + field.getFieldName() + " is not valid for " + thriftClass.getName() + ".");
    }

    FieldValueMetaData valueMetaData = fieldMetaData.valueMetaData;

    Pipe pipe = bindSource("input", input);
    pipe = new Each(pipe, FieldHelper.fieldOf(thriftClass), new ExpandThrift(thriftClass), new Fields(FieldHelper.fieldNameOf(thriftClass), field.getFieldName()));

    if (valueMetaData.isContainer()) {
      throw new RuntimeException("Cannot sort by a container.");
    } else if (valueMetaData.isStruct()) {
      pipe = new SortByThriftField(pipe, field.getFieldName());
    } else {
      pipe = new GroupBy(pipe, new Fields(field.getFieldName()));
    }

    complete("sort-store-by-thrift-field", pipe, sortedOutput);
  }
}
