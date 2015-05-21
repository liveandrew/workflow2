package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.junit.Test;

import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.resource.ResourceManagers;
import com.liveramp.cascading_ext.resource.WriteResource;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.ResourceRoot;

public class TestWorkflowWithResources extends WorkflowTestCase {

  //Context object with annotated methods. The context does not need
  // anything in particular except for an empty constructor
  private static class SimpleContext {
    public SimpleContext() {}

    public Set<Integer> resourceNumbers() {
      return Sets.newHashSet();
    }

    public HashSet<String> configStrings() {
      return Sets.newHashSet("audience1");
    }
  }


  @Test
  public void testContextTool() throws IOException {
    IRlDb rlDb = new DatabasesImpl().getRlDb();

    ResourceManager<String, ResourceRoot> resourceManager = ResourceManagers.dbResourceManager("name", null, rlDb);
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


    execute(Sets.newHashSet(readsResource), resourceManager);
  }
}
