package com.liveramp.cascading_ext.megadesk;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import com.liveramp.workflow.state.WorkflowDbPersistenceFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingCluster;
import org.apache.thrift.TBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.util.generated.StringOrNone;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStoreImpl;
import com.rapleaf.cascading_ext.datastore.VersionedThriftBucketDataStoreHelper;
import com.rapleaf.cascading_ext.serialization.ThriftRawComparator;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.cascading_ext.workflow2.action.CleanUpOlderVersions;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestResourceSemaphore extends WorkflowTestCase {

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

    final TestingCluster cluster = new TestingCluster(3);
    cluster.start();

    VersionedBucketDataStore<StringOrNone> versionedStore =
        new VersionedBucketDataStoreImpl<StringOrNone>(getFS(), "store", getTestRoot() + "/input", "", StringOrNone.class);

    VersionedThriftBucketDataStoreHelper.writeToNewVersion(versionedStore, StringOrNone.string_value("version1"));
    assertEquals(1, versionedStore.getAllCompleteVersions().length);


    AtomicBoolean keepGoing = new AtomicBoolean(true);
    AtomicBoolean barrier = new AtomicBoolean(false);

    StoreReaderLockProvider lockProvider = new StoreReaderLockProvider() {
      @Override
      public StoreReaderLocker create() {

        CuratorFramework framework = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryNTimes(3, 100));
        framework.start();

        return new CuratorStoreLocker(Lists.newArrayList(framework));
      }
    };
    HadoopWorkflowOptions options = new TestWorkflowOptions().setLockProvider(lockProvider);


    Step action = new Step(new LongRunningAction("action", keepGoing, barrier, versionedStore));


    final WorkflowRunner runner = new WorkflowRunner(TestResourceSemaphore.class,
        new WorkflowDbPersistenceFactory(),
        options, action);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          runner.run();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
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

  private void attemptToClean(VersionedBucketDataStore<StringOrNone> versionedStore, HadoopWorkflowOptions options) throws IOException {
    new WorkflowRunner("Test Workflow",
        new WorkflowDbPersistenceFactory(),
        options,
        new Step(new CleanUpOlderVersions("clean", getTestRoot(), 1, versionedStore))).run();
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
