package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import org.apache.thrift.TBase;

import com.rapleaf.cascading_ext.datastore.PartitionedDataStore;
import com.rapleaf.cascading_ext.map_side_join.partitioning.PartitionerConfig;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.bucket.BucketIndex;

public class AddEmptyPartitionedDirs<T extends TBase, C extends T> extends Action {

  private static final String EMPTY_PART_DIR = "part-00000_0";

  private final PartitionedDataStore<C> store;
  private final Map<Long, Integer> expectedVersions;
  private final PartitionerConfig<?, T> config;
  private final PartitionedDataStore<C> emptyPartitioned;

  //  TODO config doesn't really need to be passed in, but # partitions  should be injected wherever we use the  partitioner
  public AddEmptyPartitionedDirs(String checkpointToken, String tmpDir,
                                 PartitionedDataStore<C> store,
                                 Map<Long, Integer> expectedVersions,
                                 PartitionerConfig<?, T> config,
                                 Class<C> recordClass) {
    super(checkpointToken, tmpDir);

    this.store = store;
    this.expectedVersions = expectedVersions;
    this.config = config;

    this.emptyPartitioned = new PartitionedDataStore<C>(getTmpRoot() + "/tmp_bucket",
        recordClass
    );

    createsTemporary(emptyPartitioned);
  }


  @Override
  protected void execute() throws Exception {

    Bucket empty = emptyPartitioned.getBucket();
    for (Long audience : expectedVersions.keySet()) {
      int version = expectedVersions.get(audience);

      if (!store.containsAudienceVersion(audience, version)) {

        String subfolder = audience + "/" + version;
        Bucket.BucketFileOutputStream out = empty.openWrite(subfolder + "/" + EMPTY_PART_DIR);

        out.sync();
        long start = out.pos();
        out.close();

        Map<String, BucketIndex.Partition> partitions = Maps.newHashMap();
        for (int i = 0; i < config.getNumVirtualPartitions(); i++) {
          partitions.put(audience + "-" + i, new BucketIndex.Partition(start, out.getResultFile().getName()));
        }

        empty.writeIndex(subfolder, new BucketIndex(partitions));
      }
    }

    store.persistFrom(emptyPartitioned, ArrayListMultimap.<Long, Integer>create());

  }
}
