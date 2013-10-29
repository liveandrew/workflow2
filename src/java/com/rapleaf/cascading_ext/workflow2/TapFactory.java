package com.rapleaf.cascading_ext.workflow2;

import cascading.tap.Tap;
import com.rapleaf.cascading_ext.datastore.DataStore;

import java.io.IOException;

public interface TapFactory {

  /**
   * TapFactories should expect this method to be called multiple times, including during workflow
   * construction time.
   *
   * @throws IOException
   */
  public Tap createTap() throws IOException;

  public static class SimpleFactory implements TapFactory {

    private final DataStore store;
    public SimpleFactory(DataStore store){
      this.store = store;
    }

    @Override
    public Tap createTap() {
      return store.getTap();
    }
  }
}
