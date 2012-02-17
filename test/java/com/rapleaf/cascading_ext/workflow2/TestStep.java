package com.rapleaf.cascading_ext.workflow2;

import com.rapleaf.cascading_ext.CascadingExtTestCase;

public class TestStep extends CascadingExtTestCase {
  public void testDoesntAcceptNullDeps() {
    try {
      new Step(new NullAction("1"), null);
      fail("should have thrown an exception");
    } catch (NullPointerException npe) {
      // yay!
    }

    try {
      Step s = new Step(new NullAction("2"));
      new Step(new NullAction("1"), s, (Step) null);
      fail("should have thrown an exception");
    } catch (NullPointerException npe) {
      // yay!
    }
  }
}
