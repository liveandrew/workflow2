package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

import cascading.tap.Tap;

import com.liveramp.cascading_ext.tap.NullTap;
import com.rapleaf.cascading_ext.datastore.DataStore;

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

  public static class NullTapFactory implements TapFactory {
    @Override
    public Tap createTap() throws IOException {
      return new NullTap();
    }
  }

}
