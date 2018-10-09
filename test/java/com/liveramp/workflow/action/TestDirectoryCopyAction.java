package com.liveramp.workflow.action;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.thrift.TException;
import org.junit.Test;

import com.liveramp.cascading_tools.util.TrackedDistCpOptions;
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
    WorkflowRunner runner = execute(new DirectoryCopyAction("test", Lists.newArrayList(inputRoot), dstPath, new TrackedDistCpOptions(), 1L));

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

  @Test
  public void testMultiple() throws Exception {

    //  set up a dir
    Bucket inputBucket1 = builder().getBucketDataStore("tmp/input_path1", PIN.class).getBucket();
    ThriftBucketHelper.writeToBucket(inputBucket1,
        PIN.email("ben@altavista.com")
    );

    Bucket inputBucket2 = builder().getBucketDataStore("tmp/input_path2", PIN.class).getBucket();
    ThriftBucketHelper.writeToBucket(inputBucket1,
        PIN.email("ben@gmail.com")
    );

    Path dstPath = new Path(getTestRootPath(), "dst_path");

    WorkflowRunner runner = execute(new DirectoryCopyAction("test",
        Lists.newArrayList(inputBucket1.getInstanceRoot(), inputBucket2.getInstanceRoot()),
        dstPath,
        new TrackedDistCpOptions().setAsRawPaths(true),
        1L
    ));

    //  assert we recorded counters
    assertTrue(runner.getPersistence().getFlatCounters().size() > 0);

    List<FileStatus> inputFiles = Lists.newArrayList();
    inputFiles.addAll(Lists.newArrayList(getFS().listStatus(inputBucket1.getInstanceRoot())));
    inputFiles.addAll(Lists.newArrayList(getFS().listStatus(inputBucket2.getInstanceRoot())));

    List<LocatedFileStatus> files = Lists.newArrayList();
    RemoteIterator<LocatedFileStatus> iter = getFS().listFiles(dstPath, true);

    while (iter.hasNext()) {
      files.add(iter.next());
    }

    //  4 total files
    assertEquals(inputFiles.size(), files.size());
    //  check we have 2 root files
    assertEquals(2, getFS().listStatus(dstPath).length);
  }


  @Test
  public void testLocalCopy() throws IOException, TException {


    Bucket inputBucket = builder().getBucketDataStore("input_path", PIN.class).getBucket();
    ThriftBucketHelper.writeToBucket(inputBucket,
        PIN.email("ben@gmail.com"),
        PIN.email("ben@altavista.com")
    );

    Path dstPath = new Path(getTestRootPath(), "dst_path");

    Path inputRoot = inputBucket.getInstanceRoot();
    WorkflowRunner runner = execute(new DirectoryCopyAction("test", inputRoot, dstPath));

    //  assert no MR job
    assertEquals(0, runner.getPersistence().getFlatCounters().size());

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