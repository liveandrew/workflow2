package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import com.liveramp.workflow.state.WorkflowDbPersistenceFactory;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.resource.ResourceManagers;
import com.liveramp.cascading_ext.resource.WriteResource;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.rldb.IRlDb;

import static org.junit.Assert.fail;

public class TestResourceWorkflowIntegration extends WorkflowTestCase {

  private static final String name = ResourceTest.class.getName();
  private static final Set<Long> contextNumbers = Sets.newHashSet(2L, 4L, 8L, 16L);
  private static final Set<Long> previousNumbers = Sets.newHashSet(0L, 1L);
  private static final Set<Long> expectedNumbers = Sets.newHashSet(1L, 2L, 3L, 4L, 5L);
  private static boolean shouldFail;
  private IRlDb rlDb;

  @Before
  public void before() throws IOException {
    shouldFail = false;
    rlDb = new DatabasesImpl().getRlDb();
    rlDb.deleteAll();
  }

  @Test
  public void testHdfsStorage() throws IOException {
    testStorage(new RMFactory() {
      @Override
      public ResourceDeclarer make() throws IOException {
        return ResourceManagers.hdfsResourceManager(getTestRoot() + "/" + name, name, null, rlDb);
      }
    });
  }

  @Test
  public void testDbStorage() throws IOException {
    testStorage(new RMFactory() {
      @Override
      public ResourceDeclarer make() throws IOException {
        return ResourceManagers.dbResourceManager(name, null, rlDb);
      }
    });
  }

  private <T> void testStorage(RMFactory manager) throws IOException {

    InitializedWorkflow workflow = new WorkflowDbPersistenceFactory().initialize(name,
        new TestWorkflowOptions().setResourceManager(manager.make())
    );
    execute(workflow, getSteps(workflow.getManager(), previousNumbers));

    // should fail the first time
    shouldFail = true;

    workflow = new WorkflowDbPersistenceFactory().initialize(name,
        new TestWorkflowOptions().setResourceManager(manager.make())
    );

    try {
      execute(workflow, getSteps(workflow.getManager(), expectedNumbers));
      fail();
    } catch (RuntimeException e) {
      //  expected
    }

    shouldFail = false;

    workflow = new WorkflowDbPersistenceFactory().initialize(name,
        new TestWorkflowOptions().setResourceManager(manager.make())
    );

    execute(workflow, getSteps(workflow.getManager(), expectedNumbers));

  }

  private Set<Step> getSteps(ResourceManager manager, Set<Long> numbersToWrite) throws IOException {
    return Sets.newHashSet(new Step(new ResourceTest(manager, getTestRoot(), numbersToWrite)));
  }

  private static class ResourceTest extends MultiStepAction {
    private MyContext context;

    public ResourceTest(ResourceManager manager, String tmpRoot, Set<Long> numbersToWrite) {
      super("checkpoints", tmpRoot);
      context = manager.manage(new MyContext());

      Resource<Set<Long>> resource = manager.resource(context.numbers());

      Step step1 = new Step(new CreatesAction("create_step", resource, numbersToWrite));
      Step step2 = new Step(new ThrowsAction("throw_step"), step1);
      Step step3 = new Step(new ReadsAction("read_step", resource, numbersToWrite), step2);

      setSubStepsFromTail(step3);
    }
  }

  private static class MyContext {
    public MyContext() {
    }

    ;

    public Set<Long> numbers() {
      return contextNumbers;
    }
  }

  private static class CreatesAction extends Action {
    private final Set<Long> numbersToWrite;
    private WriteResource<Set<Long>> numbers;

    public CreatesAction(String checkpointToken, Resource<Set<Long>> numbers, Set<Long> numbersToWrite) {
      super(checkpointToken);
      this.numbersToWrite = numbersToWrite;
      this.numbers = creates(numbers);
    }

    @Override
    protected void execute() throws Exception {
      set(numbers, numbersToWrite);
    }
  }

  private static class ThrowsAction extends Action {

    public ThrowsAction(String checkpointToken) {
      super(checkpointToken);
    }

    @Override
    protected void execute() throws Exception {
      if (shouldFail) {
        throw new RuntimeException("Failing intentionally");
      }
    }
  }

  private static class ReadsAction extends Action {
    private ReadResource<Set<Long>> numbers;
    private Set<Long> expectedNumbers;

    public ReadsAction(String checkpointToken, Resource<Set<Long>> numbers, Set<Long> expectedNumbers) {
      super(checkpointToken);
      this.numbers = readsFrom(numbers);
      this.expectedNumbers = expectedNumbers;
    }

    @Override
    protected void execute() throws Exception {
      assertCollectionEquivalent(expectedNumbers, get(numbers));
    }
  }

  private interface RMFactory {

    ResourceDeclarer make() throws IOException;

  }
}
