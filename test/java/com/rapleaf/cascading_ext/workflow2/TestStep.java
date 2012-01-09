package com.rapleaf.support.workflow2;

import com.rapleaf.java_support.JavaSupportTestCase;

public class TestStep extends JavaSupportTestCase {
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
