package com.rapleaf.cascading_ext.workflow2.action;

import java.io.Serializable;

import org.apache.thrift.TBase;

import com.rapleaf.cascading_ext.map_side_join.partitioning.PartitionerConfig;

public interface SplitGenerator<T> extends Serializable {

  public String getSplit(T record);

  public class Empty<T> implements SplitGenerator<T>{
    @Override
    public String getSplit(T record) {
      return "";
    }
  }

  public class Partitioned<S, T extends TBase, P> implements SplitGenerator<T>{

    private final PartitionerConfig<S, T, P> partitioner;

    public Partitioned(PartitionerConfig<S, T, P> partitioner){
      this.partitioner = partitioner;
    }

    @Override
    public String getSplit(T record) {
      return partitioner.getSplit(partitioner.getCategory(record));
    }
  }

}
