package com.rapleaf.cascading_ext.workflow2.action;

import cascading.flow.Flow;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.assembly.Distinct;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.function.ExpandThrift;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;

public class DistinctDIEsOnPINAndEID extends Action {

  private final BucketDataStore indistinctDIEs;
  private final BucketDataStore distinctDIEs;
  
  public DistinctDIEsOnPINAndEID(String checkpointToken, BucketDataStore indistinctDIEs, BucketDataStore distinctDIEs) {
    super(checkpointToken);

    this.indistinctDIEs = indistinctDIEs;
    this.distinctDIEs = distinctDIEs;
  
    readsFrom(indistinctDIEs);
    creates(distinctDIEs);
  }

  @Override
  protected void execute() throws Exception {

    Pipe merged = new Pipe("to-distinct");
    merged = new Each(merged, new Fields("die"), new ExpandThrift(DustinInternalEquiv.class), new Fields("die", "eid", "pin"));
    merged = new Distinct(merged, new Fields("eid", "pin"));

    completeWithProgress(CascadingHelper.getFlowConnector().connect(indistinctDIEs.getTap(),
        distinctDIEs.getTap(), merged));
  }
}
