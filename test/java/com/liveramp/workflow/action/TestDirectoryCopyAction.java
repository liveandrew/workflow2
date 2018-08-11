package com.liveramp.workflow.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.junit.Test;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.types.new_person_data.PIN;

import static org.junit.Assert.*;

public class TestDirectoryCopyAction extends WorkflowTestCase {

  @Test
  public void testIt() throws IOException, TException {

    //  set up a dir
    Bucket inputBucket = builder().getBucketDataStore("input_path", PIN.class).getBucket();
    ThriftBucketHelper.writeToBucket(inputBucket,
        PIN.email("ben@gmail.com"),
        PIN.email("ben@altavista.com")
    );

    Path dstPath = new Path(getTestRootPath(), "dst_path");

    Path inputRoot = inputBucket.getInstanceRoot();
    WorkflowRunner runner = execute(new DirectoryCopyAction("test", inputRoot, dstPath));

    //  assert we recorded counters
    assertTrue(runner.getPersistence().getFlatCounters().size() > 0);

    List<FileStatus> inputFiles = Lists.newArrayList(getFS().listStatus(inputRoot));
    List<FileStatus> dstFiles = Lists.newArrayList(getFS().listStatus(dstPath));

    //  check we have dst files
    assertEquals(2, dstFiles.size());
    assertEquals(inputFiles.size(), dstFiles.size());

    //  check they are the same
    for (FileStatus file : inputFiles) {
      assertEquals(file.getLen(), getFS().getFileStatus(new Path(dstPath, file.getPath().getName())).getLen());
    }

  }


}