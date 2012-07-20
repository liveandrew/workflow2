package com.rapleaf.cascading_ext.workflow2.action;

import cascading.flow.Flow;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.function.ExpandThrift;
import com.rapleaf.cascading_ext.function.pin_and_owners.SplitPINAndOwners;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.types.new_person_data.PINAndOwner;

public class PINFromPINAndOwnersAction extends Action {
  BucketDataStore pinAndOwnerSets;
  BucketDataStore pins;

  public PINFromPINAndOwnersAction(String checkpointToken,
                                   String tmpRoot,
                                   BucketDataStore pinAndOwnerSets,
                                   BucketDataStore pins) {
    super(checkpointToken, tmpRoot);
    this.pinAndOwnerSets = pinAndOwnerSets;
    this.pins = pins;
  }

  @Override
  protected void execute() throws Exception {
    Pipe pinAndOwners = new Pipe("pinAndOwnerSets");
    pinAndOwners = new Each(pinAndOwners, new Fields("pin_and_owners"), new SplitPINAndOwners(), new Fields("pin_and_owner"));
    pinAndOwners = new Each(pinAndOwners, new Fields("pin_and_owner"), new ExpandThrift(PINAndOwner.class), new Fields("pin"));

    Flow flow = CascadingHelper.getFlowConnector().connect(this.getClass().getSimpleName(), pinAndOwnerSets.getTap(), pins.getTap(), pinAndOwners);
    runningFlow(flow);
    flow.complete();
  }
}

