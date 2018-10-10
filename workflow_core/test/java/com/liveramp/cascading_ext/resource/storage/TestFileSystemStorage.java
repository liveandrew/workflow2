package com.liveramp.cascading_ext.resource.storage;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.resource.Storage;
import com.liveramp.commons.collections.map.MapBuilder;
import com.liveramp.resource_core.FileSystemManager;
import com.liveramp.resource_core.ResourceCoreTestCase;
import com.rapleaf.types.new_person_data.PIN;

import static org.junit.Assert.assertEquals;

public class TestFileSystemStorage extends ResourceCoreTestCase{

  private final static PIN email1 = PIN.email("thomas@kielbus.com");
  private final static PIN email2 = PIN.email("alice@kielbus.com");
  private final static PIN email3 = PIN.email("alice2@gmail.com");
  private final static List<Integer> listOfIntegers = Lists.newArrayList(1, 2, 3, 4);
  private final static List<Long> listOfLongs = Lists.newArrayList(1L, 2L, 3L, 4L);

  private FileSystemStorage.Factory factory;


  private final String TEST_ROOT = "/tmp/tests/" + ResourceCoreTestCase.class.getName() + "/" + this.getName() + "_AUTOGEN/";

  protected FileSystemStorage.Factory createStorage() {
    return new FileSystemStorage.Factory();
  }

  @Before
  public void prepare() {
    factory = createStorage();
  }


  @Before
  public final void setUpTestRoot() throws IOException {
    FileUtils.deleteDirectory(new File(TEST_ROOT));
  }

  protected static final Map<String, Integer> map = new MapBuilder<String, Integer>()
      .of("x", 1).put("y", 2).put("z", 3).get();
  protected static final Set<Long> set = Sets.newHashSet(Long.MAX_VALUE, Long.MIN_VALUE, -1L, 0L);
  protected static final String MAP_NAME = "MapResource";
  protected static final String SET_NAME = "SetResource";
  protected static final String NAME = "Some name";

  @Test
  public void testStoreAndRetrieve() throws IOException {
    Storage storage = factory.forResourceRoot(TEST_ROOT);
    storage.store(MAP_NAME, map);
    Map<String, Integer> retrievedMap = storage.retrieve(MAP_NAME);
    assertCollectionEquivalent(map.entrySet(), retrievedMap.entrySet());
  }

  @Test
  public void testStoreAndRetrieve2() throws IOException {
    Storage storage = factory.forResourceRoot(TEST_ROOT);
    storage.store(SET_NAME, set);
    Set<Long> retrievedSet = storage.retrieve(SET_NAME);
    assertCollectionEquivalent(retrievedSet, set);
  }

  @Test
  public void testPersistence() throws IOException {
    Storage storage = factory.forResourceRoot(TEST_ROOT);
    storage.store(SET_NAME, set);
    createStorage();
    assert(storage.isStored(SET_NAME));
    Set<Long> retrievedSet = storage.retrieve(SET_NAME);
    assertCollectionEquivalent(retrievedSet, set);
  }


  @Test
  public void testSwitchingBetweenJavaAndJsonSerialization() throws IOException {
    ResourceManager resourceManager = FileSystemManager.fileSystemResourceManager(TEST_ROOT)
        .create(0L, "Test");

    Resource<PIN> resource1 = resourceManager.resource(email1, "email1");
    Resource<PIN> resource2 = resourceManager.resource(email2, "email2");

    ReadResource<PIN> readResource1 = resourceManager.getReadPermission(resource1);
    ReadResource<PIN> readResource2 = resourceManager.getReadPermission(resource2);

    PIN readEmail1 = resourceManager.read(readResource1);
    PIN readEmail2 = resourceManager.read(readResource2);

    resourceManager.write(resourceManager.getWritePermission(resource2), email3);

    PIN readEmail3 = resourceManager.read(readResource2);

    assertEquals(email1, readEmail1);
    assertEquals(email2, readEmail2);
    assertEquals(email3, readEmail3);
  }

  @Test
  public void testJsonForListsOfNumbers() throws IOException {
    ResourceManager resourceManager = FileSystemManager.fileSystemResourceManager(TEST_ROOT)
        .create(0L, "Test");

    Resource<List<Integer>> listOfIntegersResource = resourceManager.resource(listOfIntegers, "list-of-integers");
    assertCollectionEquivalent(listOfIntegers, resourceManager.read(resourceManager.getReadPermission(listOfIntegersResource)));

    Resource<List<Long>> listOfLongsResource = resourceManager.resource(listOfLongs, "list-of-longs");
    assertCollectionEquivalent(listOfLongs, resourceManager.read(resourceManager.getReadPermission(listOfLongsResource)));
  }
}