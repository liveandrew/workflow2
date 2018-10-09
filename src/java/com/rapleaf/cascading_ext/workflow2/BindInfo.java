package com.rapleaf.cascading_ext.workflow2;

import java.util.List;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.tap.TapFactory;

public class BindInfo {
  private final TapFactory tapFactory;
  private final ActionCallback callback;
  private final List<DataStore> inputs;

  public BindInfo(TapFactory tapFactory, ActionCallback callback, List<DataStore> inputs) {
    this.tapFactory = tapFactory;
    this.callback = callback;
    this.inputs = inputs;
  }

  public TapFactory getTapFactory() {
    return tapFactory;
  }

  public ActionCallback getCallback() {
    return callback;
  }

  public List<DataStore> getInputs() {
    return inputs;
  }
}
