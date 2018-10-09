package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.PartitionedDataStore;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CreatePartitionStructure extends Action {

  private final PartitionStructure structure;
  private final PartitionedDataStore<?> store;

  public CreatePartitionStructure(
      String checkpointToken,
      PartitionStructure structure,
      PartitionedDataStore<?> store) {
    super(checkpointToken);
    this.structure = structure;
    this.store = store;
  }

  @Override
  protected void execute() throws Exception {
    store.getBucket().buildIndexFromPartfiles(structure);
  }
}
