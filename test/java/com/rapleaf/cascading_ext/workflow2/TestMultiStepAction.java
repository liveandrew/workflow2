package com.rapleaf.cascading_ext.workflow2;

import java.util.Arrays;
import java.util.HashSet;

import com.rapleaf.cascading_ext.CascadingExtTestCase;

public class TestMultiStepAction extends CascadingExtTestCase {
  private Step a;
  private Step b;
  private Step g;
  private Step c ;
  private Step d;
  private Step e;
  private Step f;
  
  private MultiStepAction msa;

  public final class NullAction2 extends Action {
    public NullAction2(String checkpoint, String tmpRoot) {
      super(checkpoint, tmpRoot);
    }

    @Override
    public void execute() {}
  }

  public void setUp() throws Exception {
    super.setUp();

    msa = new MultiStepAction("msa", getTestRoot());

    b = new Step(new NullAction2("b", msa.getTmpRoot()));
    a = new Step(new NullAction2("a", msa.getTmpRoot()));
    c = new Step(new NullAction2("c", msa.getTmpRoot()), a);
    d = new Step(new NullAction2("d", msa.getTmpRoot()), a, b);
    e = new Step(new NullAction2("e", msa.getTmpRoot()), b);
    f = new Step(new NullAction2("f", msa.getTmpRoot()), c, e);
    g = new Step(new NullAction2("g", msa.getTmpRoot()));

    msa.setSubSteps(Arrays.asList(a, b, c, d, e, f, g));
  }
  
  public void testGetHeadSteps() throws Exception {

    //  assert that the tmp root is set
    assertEquals(getTestRoot()+"/msa-tmp-stores", msa.getTmpRoot());
    assertEquals(getTestRoot()+"/msa-tmp-stores/a-tmp-stores", a.getAction().getTmpRoot());

    assertEquals(new HashSet<Step>(Arrays.asList(a, b, g)), msa.getHeadSteps());
  }

  public void testGetTailSteps() throws Exception {
    assertEquals(new HashSet<Step>(Arrays.asList(d, f, g)), msa.getTailSteps());
  }

  public void testNoDuplicateTokens() throws Exception {
    try {
      new MultiStepAction("blah", Arrays.asList(a, a));
      fail();
    } catch (Exception e) {
      // cool!
    }
  }
}
