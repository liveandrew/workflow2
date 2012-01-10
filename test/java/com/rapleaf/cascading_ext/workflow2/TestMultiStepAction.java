package com.rapleaf.cascading_ext.workflow2;

import java.util.Arrays;
import java.util.HashSet;

import com.rapleaf.cascading_ext.CascadingExtTestCase;

public class TestMultiStepAction extends CascadingExtTestCase {
  private final static Step a = new Step(new NullAction("a"));
  private final static Step b = new Step(new NullAction("b"));
  private final static Step g = new Step(new NullAction("g"));
  private final static Step c = new Step(new NullAction("c"), a);
  private final static Step d = new Step(new NullAction("d"), a, b);
  private final static Step e = new Step(new NullAction("e"), b);
  private final static Step f = new Step(new NullAction("f"), c, e);

  private final static MultiStepAction msa = new MultiStepAction("blah", Arrays.asList(a, b, c, d,
    e, f, g));

  public void testGetHeadSteps() throws Exception {
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
