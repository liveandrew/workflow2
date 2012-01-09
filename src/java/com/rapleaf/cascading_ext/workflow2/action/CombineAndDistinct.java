package com.rapleaf.cascading_ext.workflow2.action;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import cascading.pipe.Pipe;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.assembly.Distinct;
import com.rapleaf.support.datastore.DataStore;
import com.rapleaf.support.workflow2.Action;

public class CombineAndDistinct extends Action {

  private final Collection<DataStore> stores;
  private final DataStore output;
  private final Fields distinct;

  public CombineAndDistinct(String checkpointToken,
      Collection<DataStore> stores,
      Fields distinctFields,
      DataStore output) {
    super(checkpointToken);

    this.stores = stores;
    this.output = output;
    this.distinct = distinctFields;

    creates(output);
  }

  @Override
  protected void execute() throws Exception {

    List<Tap> taps = new ArrayList<Tap>();
    for (DataStore store : stores) {
      taps.add(store.getTap());
    }

    Pipe pipes = new Pipe("pipes");
    pipes = new Distinct(pipes, distinct);

    completeWithProgress(CascadingHelper.getFlowConnector().connect(
      new MultiSourceTap(taps.toArray(new Tap[0])), output.getTap(), pipes));
  }

}
