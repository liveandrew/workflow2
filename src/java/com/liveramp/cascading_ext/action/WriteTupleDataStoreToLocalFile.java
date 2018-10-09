package com.liveramp.cascading_ext.action;

import java.io.PrintWriter;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class WriteTupleDataStoreToLocalFile extends Action {

  private final TupleDataStore store;
  private final String localFilePath;

  public WriteTupleDataStoreToLocalFile(String checkpointToken, TupleDataStore store, String localFilePath) {
    super(checkpointToken);
    this.store = store;
    this.localFilePath = localFilePath;

    readsFrom(store);
  }

  @Override
  protected void execute() throws Exception {
    PrintWriter writer = new PrintWriter(localFilePath);
    Fields fields = store.getFields();
    for (TupleEntry entry : HRap.getTuples(store.getTap())) {
      List<String> values = Lists.newArrayList();
      for (Comparable field : fields) {
        values.add(entry.getObject(field).toString());
      }
      writer.println(Joiner.on("\t").join(values));
    }
    writer.close();
  }
}
