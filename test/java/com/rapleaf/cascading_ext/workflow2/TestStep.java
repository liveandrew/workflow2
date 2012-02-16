package com.rapleaf.cascading_ext.workflow2;

import com.rapleaf.cascading_ext.CascadingExtTestCase;

public class TestStep extends CascadingExtTestCase {
  public void testDoesntAcceptNullDeps() {
    try {
      new Step("1", new NullAction(), null);
      fail("should have thrown an exception");
    } catch (NullPointerException npe) {
      // yay!
    }
    
    try {
      Step s = new Step("2", new NullAction());
      new Step("1", new NullAction(), s, (Step) null);
      fail("should have thrown an exception");
    } catch (NullPointerException npe) {
      // yay!
    }
  }
}
