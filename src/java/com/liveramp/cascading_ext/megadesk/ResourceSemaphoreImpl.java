package com.liveramp.cascading_ext.megadesk;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;

public class ResourceSemaphoreImpl implements ResourceSemaphore {

  private final InterProcessSemaphoreV2 semaphore;
  private final List<Lease> leases = Lists.newArrayList();
  public static final String READER_LOCK_BASE_PATH = "/clockwork/datastore_reader_locks/";
  private final String path;
  private final String name;

  public ResourceSemaphoreImpl(CuratorFramework framework, String resourceUniqueIdentifer, String name) {
    this.name = name;
    this.path = READER_LOCK_BASE_PATH + resourceUniqueIdentifer;
    this.semaphore = new InterProcessSemaphoreV2(framework, path, Integer.MAX_VALUE);
    initNode(semaphore);
  }

  private void initNode(InterProcessSemaphoreV2 semaphore) {
    try {
      Lease acquire = semaphore.acquire();
      semaphore.getParticipantNodes();
      semaphore.returnLease(acquire);
      //semaphore.getParticipantNodes();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void lock() {
    try {
      Lease lease = semaphore.acquire();
      leases.add(lease);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void release() {
    try {
      semaphore.returnAll(leases);
      leases.clear();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasReaders() {
    try {
      return semaphore.getParticipantNodes().size() > 0;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "ResourceSemaphoreImpl{" +
        "path='" + path + '\'' +
        ", name='" + name + '\'' +
        '}';
  }
}
