package com.rapleaf.cascading_ext.workflow2;

import org.junit.Test;

import static junit.framework.Assert.fail;

public class TestStep extends WorkflowTestCase {

  @Test
  public void testDoesntAcceptNullDeps() {
    try {
      new Step(new NullAction("1"), (Step)null);
      fail("should have thrown an exception");
    } catch (NullPointerException npe) {
      // yay!
    }

    try {
      Step s = new Step(new NullAction("2"));
      new Step(new NullAction("1"), s, null);
      fail("should have thrown an exception");
    } catch (NullPointerException npe) {
      // yay!
    }
  }
}
