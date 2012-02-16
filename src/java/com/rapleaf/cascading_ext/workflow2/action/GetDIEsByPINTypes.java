package com.rapleaf.cascading_ext.workflow2.action;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.filter.die.SelectDIEByPINDomains;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.types.new_person_data.PIN;

public class GetDIEsByPINTypes extends Action {
  
  private final BucketDataStore napDIEs;
  private final EnumSet<PIN._Fields> pinTypes;
  private final Collection<BucketDataStore> inputDIEs;
  
  public GetDIEsByPINTypes(
      EnumSet<PIN._Fields> pinTypes,
      Collection<BucketDataStore> inputDIEs,
      BucketDataStore outputDIEs) {
    super();
    
    this.napDIEs = outputDIEs;
    this.inputDIEs = inputDIEs;
    
    this.pinTypes = pinTypes;
    for (BucketDataStore store : inputDIEs) {
      readsFrom(store);
    }
    
    creates(outputDIEs);
  }
  
  @Override
  protected void execute() throws Exception {
    
    Pipe naps = new Pipe("dies");
    naps = new Each(naps, new Fields("die"), new SelectDIEByPINDomains(pinTypes));
    
    List<Tap> taps = new ArrayList<Tap>();
    for (BucketDataStore store : inputDIEs) {
      taps.add(store.getTap());
    }
    
    CascadingHelper.getFlowConnector().connect(new MultiSourceTap(taps.toArray(new Tap[0])),
        napDIEs.getTap(), naps).complete();
  }
}
