package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.liveramp.cascading_ext.tap.NullTap;
import com.rapleaf.cascading_ext.datastore.DataStore;

public abstract class TapFactory {

  /**
   * TapFactories should expect this method to be called multiple times, including during workflow
   * construction time.
   *
   * @throws IOException
   */
  public abstract Tap createTap() throws IOException;

  //  override this if createTap is an expensive operation and you know the source fields
  public Fields getSourceFields() throws IOException {
    return createTap().getSourceFields();
  }

  public static class SimpleFactory extends TapFactory {

    private final DataStore store;
    public SimpleFactory(DataStore store){
      this.store = store;
    }

    @Override
    public Tap createTap() {
      return store.getTap();
    }
  }

  public static class NullTapFactory extends TapFactory {
    @Override
    public Tap createTap() throws IOException {
      return new NullTap();
    }
  }

}
