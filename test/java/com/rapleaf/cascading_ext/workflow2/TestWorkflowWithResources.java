package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.cedarsoftware.util.io.JsonReader;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import com.liveramp.cascading_ext.resource.DbStorage;
import com.liveramp.cascading_ext.resource.DbStorageRootDeterminer;
import com.liveramp.cascading_ext.resource.HdfsStorage;
import com.liveramp.cascading_ext.resource.HdfsStorageRootDeterminer;
import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.resource.ResourceDeclarerContainer;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.resource.ResourceStorages;
import com.liveramp.cascading_ext.resource.RootManager;
import com.liveramp.cascading_ext.resource.WriteResource;
import com.liveramp.workflow_state.InitializedDbPersistence;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.DbPersistenceFactory;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.ResourceRoot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestWorkflowWithResources extends WorkflowTestCase {

  //Context object with annotated methods. The context does not need
  // anything in particular except for an empty constructor
  private static class SimpleContext {
    public SimpleContext() {
    }

    public Set<Integer> resourceNumbers() {
      return Sets.newHashSet();
    }

    public HashSet<String> configStrings() {
      return Sets.newHashSet("audience1");
    }
  }


  private ResourceDeclarer<String, ResourceRoot> getDeclarer(IRlDb rldb, DbStorage storage) throws IOException {

    ResourceDeclarerContainer<String, ResourceRoot> declarer = new ResourceDeclarerContainer<>(
        storage,
        new ResourceDeclarerContainer.MethodNameTagger(),
        new RootManager<>(
            new DbStorageRootDeterminer(rldb))
    );

    return declarer;
  }

  @NotNull
  private DbStorage getStorage(IRlDb rldb) {
    return ResourceStorages.dbStorage(rldb, Maps.<Class, JsonReader.ClassFactory>newHashMap());
  }

  private HdfsStorage getHdfsStorage() {
    return ResourceStorages.hdfsStorage();
  }

  private ResourceDeclarer<String, String> getDeclarer(IRlDb rldb, HdfsStorage storage, String workflowRoot) throws IOException {

    ResourceDeclarerContainer<String, String> declarer = new ResourceDeclarerContainer<>(
        storage,
        new ResourceDeclarerContainer.MethodNameTagger(),
        new RootManager<>(
            new HdfsStorageRootDeterminer(workflowRoot))
    );

    return declarer;
  }


  @Test
  public void testHdfsResourceVersions() throws IOException {

    String tmpRoot = getTestRoot() + "/workflow";

    IRlDb rldb = new DatabasesImpl().getRlDb();

    HdfsStorage storage = getHdfsStorage();
    ResourceDeclarer<String, String> declarer = getDeclarer(rldb, storage, tmpRoot);
    Resource<Integer> resource = declarer.<Integer>emptyResource("resource");
    Step step = new Step(new SetResource("step-1", resource));
    Step step2 = new Step(new FailingAction("step-2"), step);

    WorkflowRunner runner = new WorkflowRunner(
        "Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions().setResourceManager(declarer),
        Sets.newHashSet(step2)
    );

    try {
      runner.run();
    } catch (Exception e) {
      //  no-op
    }

    String origRoot = storage.getRoot();
    Path rootPath = new Path(origRoot);

    assertTrue(getFS().exists(rootPath));
    assertEquals(Long.parseLong(rootPath.getName()), runner.getPersistence().getExecutionId());
    assertEquals(InitializedDbPersistence.class.getName(), rootPath.getParent().getName());

    storage = getHdfsStorage();
    declarer = getDeclarer(rldb, storage, tmpRoot);
    resource = declarer.emptyResource("resource");
    step = new Step(new SetResource("step-1", resource));
    step2 = new Step(new ReadResouce("step-2", resource), step);


    new WorkflowRunner("Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions().setResourceManager(declarer),
        Sets.newHashSet(step2)
    ).run();

    String rootRecord = storage.getRoot();

    assertEquals(rootRecord, origRoot);

    storage = getHdfsStorage();
    declarer = getDeclarer(rldb, storage, tmpRoot);
    resource = declarer.emptyResource("resource");
    step = new Step(new SetResource("step-1", resource));
    step2 = new Step(new ReadResouce("step-2", resource), step);


    new WorkflowRunner("Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions().setResourceManager(getDeclarer(rldb, storage, tmpRoot)),
        Sets.newHashSet(step2)
    ).run();

    assertFalse(storage.getRoot() == origRoot);

  }

  public static class SetResource extends Action {

    private final WriteResource<Integer> resource;

    public SetResource(String checkpointToken, Resource<Integer> resourcez) {
      super(checkpointToken);
      this.resource = creates(resourcez);
    }

    @Override
    protected void execute() throws Exception {
      set(resource, 1);
    }
  }

  public static class ReadResouce extends Action {

    private final ReadResource<Integer> resource;

    public ReadResouce(String checkpointToken, Resource<Integer> resource) {
      super(checkpointToken);
      this.resource = readsFrom(resource);
    }

    @Override
    protected void execute() throws Exception {
      assertEquals(1, get(resource).intValue());
    }
  }

  @Test
  public void testDbResourceVersions() throws IOException {

    IRlDb rldb = new DatabasesImpl().getRlDb();
    DbStorage storage = getStorage(rldb);

    Step step = new Step(new NoOpAction("step-1"));
    Step step2 = new Step(new FailingAction("step-2"), step);

    WorkflowRunner runner = new WorkflowRunner(
        "Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions().setResourceManager(getDeclarer(rldb, storage)),
        Sets.newHashSet(step2)
    );

    try {
      runner.run();
    } catch (Exception e) {
      //  no-op
    }

    ResourceRoot root = storage.getRoot();
    long origId = root.getId();

    assertEquals(root.getVersion().longValue(), runner.getPersistence().getExecutionId());
    assertEquals(InitializedDbPersistence.class.getName(), root.getVersionType());
    assertEquals(null, root.getName());

    step = new Step(new NoOpAction("step-1"));
    step2 = new Step(new NoOpAction("step-2"), step);

    storage = getStorage(rldb);

    new WorkflowRunner("Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions().setResourceManager(getDeclarer(rldb, storage)),
        Sets.newHashSet(step2)
    ).run();

    ResourceRoot rootRecord = storage.getRoot();
    assertEquals(rootRecord.getId(), origId);

    step = new Step(new NoOpAction("step-1"));
    step2 = new Step(new NoOpAction("step-2"), step);

    storage = getStorage(rldb);

    new WorkflowRunner("Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions().setResourceManager(getDeclarer(rldb, storage)),
        Sets.newHashSet(step2)
    ).run();

    assertFalse(storage.getRoot().getId() == origId);

  }

  @Test
  public void testContextTool() throws IOException {

    InitializedWorkflow<InitializedDbPersistence> workflow = initializeWorkflow();
    ResourceManager<String, ResourceRoot> resourceManager = workflow.getManager();

    SimpleContext context = new SimpleContext();

    context = resourceManager.manage(context);

    // This object has a little bit of magic attached to help with naming the Resource,
    // but behaves totally normally
    Set<String> strings = context.configStrings();
    strings.size();

    //We are even allowed to mutate the object, but these changes will not be saved!
    strings.add("audience2");
    Assert.assertEquals(Sets.newHashSet("audience1", "audience2"), strings);

    //To do persistent things, we need a resource
    Resource<HashSet<String>> stringResource = resourceManager.resource(context.configStrings());

    //We first get permission, then read - notice our change from earlier didn't stick
    ReadResource<HashSet<String>> readPermission = resourceManager.getReadPermission(stringResource);
    HashSet<String> read = resourceManager.read(readPermission);
    Assert.assertEquals(Sets.newHashSet("audience1"), read);

    //Write permission lets us read and write
    WriteResource<HashSet<String>> writePermission = resourceManager.getWritePermission(stringResource);
    read = resourceManager.read(writePermission);
    Assert.assertEquals(Sets.newHashSet("audience1"), read);
    resourceManager.write(writePermission, Sets.newHashSet("audience1", "audience2"));
    read = resourceManager.read(writePermission);
    Assert.assertEquals(Sets.newHashSet("audience1", "audience2"), read);


    //Contexts/annotated methods are for convenience, you can also get a resource for anything if you provide a name
    Resource<Integer> myResource = resourceManager.resource(10, "myResource");

    //Examples of how it's used inside of a workflow
    final Resource<Set<Integer>> someNumbers = resourceManager.resource(context.resourceNumbers());


    Step writesResource = new Step(new Action("check") {
      WriteResource<Set<Integer>> numbers;

      {
        numbers = creates(someNumbers);
      }

      @Override
      protected void execute() throws Exception {
        Set<Integer> integers = get(numbers);
        integers.add(10);
        set(numbers, integers);
      }
    });

    Step readsResource = new Step(new Action("check2") {
      ReadResource<Set<Integer>> numbers;

      {
        numbers = readsFrom(someNumbers);
      }

      @Override
      protected void execute() throws Exception {
        Assert.assertEquals(Sets.newHashSet(10), get(numbers)); //We added a number in a different step and can see it here
      }
    }, writesResource);


    execute(workflow, Sets.newHashSet(readsResource));
  }
}
