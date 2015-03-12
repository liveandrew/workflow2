package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.commons.collections.list.ListBuilder;
import com.liveramp.commons.collections.map.MapBuilder;
import com.liveramp.util.generated.StringOrNone;
import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.datastore.PartitionedDataStore;
import com.rapleaf.cascading_ext.map_side_join.extractors.TByteArrayExtractor;
import com.rapleaf.cascading_ext.map_side_join.partitioning.IdentityAggregator;
import com.rapleaf.cascading_ext.map_side_join.partitioning.PartitionerConfig;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.tap.bucket2.partitioner.AudienceVersion;
import com.rapleaf.cascading_ext.workflow2.action.CreatePartitionStructure;

public class TestCreatePartitionStructure extends CascadingExtTestCase {


  @Test
  public void test() throws IOException {

    PartitionedDataStore<StringOrNone> store = new PartitionedDataStore<StringOrNone>(
        getTestRoot() + "/store",
        StringOrNone.class
    );

    PartitionerConfig<StringOrNone, StringOrNone, AudienceVersion> partitioner = new PartitionerConfig<StringOrNone, StringOrNone, AudienceVersion>(1, StringOrNone.class, new IdentityAggregator<StringOrNone>()) {
      @Override
      public byte[] getKey(StringOrNone value) {
        throw new NotImplementedException();
      }

      @Override
      public AudienceVersion getCategory(StringOrNone value) {
        throw new NotImplementedException();
      }

      @Override
      public String getGroupPrefix(AudienceVersion category) {
        return Long.toString(category.getAudience());
      }

      @Override
      public String getSplit(AudienceVersion category) {
        return category.getAudience() + "/" + category.getVersion();
      }
    };

    PartitionStructure struct = partitioner.getExpectedPartitions(new ListBuilder<AudienceVersion>()
        .add(new AudienceVersion(10l, 1))
        .add(new AudienceVersion(20l, 2))
        .get());

    CreatePartitionStructure action = new CreatePartitionStructure(
        "create",
        struct,
        store
    );

    execute(action);

    RemoteIterator<LocatedFileStatus> files = FileSystemHelper.getFS().listFiles(new Path(store.getPath()), true);
    while (files.hasNext()){
      System.out.println(files.next().getPath());
    }

    InputConf<BytesWritable> conf = store.getInputConf(new TByteArrayExtractor(StringOrNone._Fields.STRING_VALUE), MapBuilder.of(10l, 1).put(20l, 2).get());

    System.out.println(conf.getInputPartitions());

    Assert.assertEquals(Sets.newHashSet(10l, 20l), store.listSplits());
    Assert.assertEquals(Lists.newArrayList(1), store.listVersions(10l));
    Assert.assertEquals(Lists.newArrayList(2), store.listVersions(20l));



  }

}
