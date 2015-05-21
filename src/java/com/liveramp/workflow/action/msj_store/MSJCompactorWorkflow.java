package com.liveramp.workflow.action.msj_store;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.workflow.msj_store.CompactionAction;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;

public class MSJCompactorWorkflow extends MultiStepAction {

  public MSJCompactorWorkflow(String checkpointToken, String tmpRoot,
                              List<MSJDataStore> storesToCompact) throws IOException {
    super(checkpointToken, tmpRoot);

    List<Step> compactionSteps = Lists.newArrayList();

    for (MSJDataStore store : storesToCompact) {
      //noinspection unchecked
      compactionSteps.add(new Step(new CompactionAction(store.getName(), getTmpRoot(), store)));
    }

    setSubStepsFromTails(compactionSteps);

  }

}
