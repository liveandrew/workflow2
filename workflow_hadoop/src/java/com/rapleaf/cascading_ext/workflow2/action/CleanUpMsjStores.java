package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.thrift.TBase;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow.msj_store.CompactionAction2;
import com.rapleaf.cascading_ext.msj_tap.store.TMSJDataStore;
import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;

public class CleanUpMsjStores<T extends TBase> extends MultiStepAction {

  public CleanUpMsjStores(String checkpointToken, String tmpRoot, int numVersionsToKeep, boolean runCompaction, AlertsHandler alertsHandler, TMSJDataStore... stores) throws IOException {
    super(checkpointToken, tmpRoot);

    List<Step> steps = Lists.newArrayList();
    for (TMSJDataStore store : stores) {
      Set<Step> preClean = Sets.newHashSet();
      if (runCompaction) {
        preClean.add(new Step(CompactionAction2.build("compact_" + store.getPath().replaceAll("/", "_"), getTmpRoot(), store.getRecordClass(), store)));
      }
      steps.add(new Step(new DeleteOldVersions(store, numVersionsToKeep), preClean));
    }

    setSubStepsFromTails(steps);
  }
}
