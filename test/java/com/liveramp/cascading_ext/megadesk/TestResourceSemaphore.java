package com.liveramp.cascading_ext.megadesk;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingCluster;
import org.apache.thrift.TBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.cascading_ext.mockery.Mockery;
import com.liveramp.util.generated.StringOrNone;
import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedThriftBucketDataStoreHelper;
import com.rapleaf.cascading_ext.serialization.ThriftRawComparator;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnable;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.action.CleanUpOlderVersions;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestResourceSemaphore extends CascadingExtTestCase {

  private CuratorFramework framework;
  private TestingCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new TestingCluster(3);
    cluster.start();

    framework = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryNTimes(3, 100));
    framework.start();
  }

  @After
  public void tearDown() throws Exception {
    framework.close();
    cluster.close();
  }

  @Test
  public void testSemaphore() throws Exception {

    ResourceSemaphore lock1 = new ResourceSemaphoreImpl(framework, "resource1", "name1");
    ResourceSemaphore lock2 = new ResourceSemaphoreImpl(framework, "resource1", "name1");

    lock1.lock();
    assertTrue(lock2.hasReaders());
    lock1.release();
    assertFalse(lock2.hasReaders());
    lock1.lock();
    lock1.lock();
    lock1.lock();
    assertTrue(lock2.hasReaders());
    lock1.release();
    assertFalse(lock2.hasReaders());

    System.out.println("Done");

  }

  @Test
  public void testWorkflowInterop() throws Exception {

    VersionedBucketDataStore<StringOrNone> versionedStore =
        Mockery.versionBucket(getTestRoot() + "/store", StringOrNone.string_value("version1"));

    assertEquals(1, versionedStore.getAllCompleteVersions().length);


    AtomicBoolean keepGoing = new AtomicBoolean(true);
    AtomicBoolean barrier = new AtomicBoolean(false);

    Step action = new Step(new LongRunningAction("action", keepGoing, barrier, versionedStore));

    StoreReaderLockProvider lockProvider = new CuratorStoreReaderLockProvider(framework);
    WorkflowOptions options = new TestWorkflowOptions().setLockProvider(lockProvider);

    WorkflowRunner runner = new WorkflowRunner("test", getTestRoot() + "/wf1", options, action);
    Thread thread = new Thread(new WorkflowRunnable(runner));
    thread.start();

    waitOnAction(barrier);

    VersionedThriftBucketDataStoreHelper.writeSortedToNewVersion(versionedStore,
        new ThriftRawComparator<TBase>(),
        StringOrNone.string_value("version2")
    );

    assertEquals(2, versionedStore.getAllCompleteVersions().length);

    attemptToClean(versionedStore, options);

    barrier.set(false);

    assertEquals(2, versionedStore.getAllCompleteVersions().length);
    keepGoing.set(false);

    waitOnAction(barrier);

    attemptToClean(versionedStore, options);

    assertEquals(1, versionedStore.getAllCompleteVersions().length);

    System.out.println("Done");
  }

  private void attemptToClean(VersionedBucketDataStore<StringOrNone> versionedStore, WorkflowOptions options) throws IOException {
    Step clean = new Step(new CleanUpOlderVersions("clean", getTestRoot(), 1, versionedStore));
    WorkflowRunner runner2 = new WorkflowRunner("test", getTestRoot() + "/wf2", options, clean);
    runner2.run();
  }

  private void waitOnAction(AtomicBoolean barrier) throws InterruptedException {
    while (!barrier.get()) {
      Thread.sleep(100);
    }
  }


  private static class LongRunningAction extends Action {

    private final AtomicBoolean keepGoing;
    private final AtomicBoolean barrier;
    private final BucketDataStore store;

    private LongRunningAction(String checkpointToken, AtomicBoolean keepGoing, AtomicBoolean barrier, BucketDataStore store) {
      super(checkpointToken);
      this.keepGoing = keepGoing;
      this.barrier = barrier;
      this.store = store;

      readsFrom(store);
    }


    @Override
    protected void execute() throws Exception {
      barrier.set(true);
      while (keepGoing.get()) {
        Thread.sleep(100);
      }
      barrier.set(true);
    }
  }
}
