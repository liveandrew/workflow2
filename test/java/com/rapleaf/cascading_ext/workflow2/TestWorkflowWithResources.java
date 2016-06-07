package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.cedarsoftware.util.io.JsonReader;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import com.liveramp.cascading_ext.resource.DbPersistenceRestartDeterminer;
import com.liveramp.cascading_ext.resource.DbStorage;
import com.liveramp.cascading_ext.resource.DbStorageRootDeterminer;
import com.liveramp.cascading_ext.resource.NameAndScope;
import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.ResourceDeclarerContainer;
import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.resource.ResourceManagers;
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
            new DbStorageRootDeterminer(rldb),
            new DbPersistenceRestartDeterminer(rldb),
            new NameAndScope("Test Workflow", null))
    );

    return declarer;
  }

  @NotNull
  private DbStorage getStorage(IRlDb rldb) {
    return ResourceStorages.dbStorage(rldb, Maps.<Class, JsonReader.ClassFactory>newHashMap());
  }

  @Test
  public void testResourceVersions() throws IOException {

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
  public void testMigration() throws IOException {

    IRlDb rldb = new DatabasesImpl().getRlDb();


    // start and fail the workflow using an in memory manager, so we don't have a version around

    Step step = new Step(new NoOpAction("step-1"));
    Step step2 = new Step(new FailingAction("step-2"), step);

    try {
      new WorkflowRunner("Test Workflow",
          new DbPersistenceFactory(),
          new TestWorkflowOptions().setResourceManager(ResourceManagers.inMemoryResourceManager("Test Workflow", rldb)),
          Sets.newHashSet(step2)
      ).run();
    }catch(Exception e){
      // no-op
    }

    //  set up old resource root (using name)

    ResourceRoot oldRoot = rldb.resourceRoots().create("Test Workflow",
        System.currentTimeMillis(),
        System.currentTimeMillis(), null, null, null
    );

    //  restart using db manager and make sure it resumes using the old root

    step = new Step(new NoOpAction("step-1"));
    step2 = new Step(new NoOpAction("step-2"), step);

    DbStorage storage = getStorage(rldb);

    new WorkflowRunner("Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions().setResourceManager(getDeclarer(rldb, storage)),
        Sets.newHashSet(step2)
    ).run();

    //  assert that workflow resumes with it
    assertEquals(oldRoot.getId(), storage.getRoot().getId());

    //  next execution doesn't

    step = new Step(new NoOpAction("step-1"));
    step2 = new Step(new NoOpAction("step-2"), step);

    storage = getStorage(rldb);

    WorkflowRunner runner = new WorkflowRunner("Test Workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions().setResourceManager(getDeclarer(rldb, storage)),
        Sets.newHashSet(step2)
    );
    runner.run();

    ResourceRoot root = storage.getRoot();

    assertEquals(root.getVersion().longValue(), runner.getPersistence().getExecutionId());
    assertEquals(InitializedDbPersistence.class.getName(), root.getVersionType());
    assertEquals(null, root.getName());
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
