package com.rapleaf.cascading_ext.workflow2.action;

import java.util.List;

import com.google.common.collect.Lists;

import com.rapleaf.cascading_ext.msj_tap.compaction.Compactor;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.TMSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;

public class CleanUpMsjStores extends MultiStepAction {

  public class MSJDataStoreCompactionAction extends Action {

    private final MSJDataStore store;

    public MSJDataStoreCompactionAction(MSJDataStore store) {
      super("compact_" + store.getPath().replaceAll("/", "_"));
      this.store = store;
    }

    @Override
    protected void execute() throws Exception {
      Compactor compactor = new Compactor();
      compactor.add(store.getCompactionTask());
      compactor.run();
    }
  }

  public CleanUpMsjStores(String checkpointToken, TMSJDataStore... stores) {
    super(checkpointToken);

    List<Step> steps = Lists.newArrayList();
    for (TMSJDataStore store : stores) {
      steps.add(new Step(new MSJDataStoreCompactionAction(store)));
    }

    setSubStepsFromTails(steps);
  }
}
