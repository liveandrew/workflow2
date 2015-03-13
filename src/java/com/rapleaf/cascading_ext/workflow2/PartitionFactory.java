package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;

public interface PartitionFactory {
  public PartitionStructure create() throws IOException;

  public class Now implements PartitionFactory {

    private final PartitionStructure struct;
    public Now(PartitionStructure struct){
      this.struct = struct;
    }

    @Override
    public PartitionStructure create() {
      return struct;
    }
  }

}
