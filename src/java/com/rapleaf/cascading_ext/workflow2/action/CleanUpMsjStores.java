package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.rapleaf.cascading_ext.msj_tap.compaction.Compactor;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.TMSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;

public class CleanUpMsjStores extends MultiStepAction {

  public class MSJDataStoreCompactionAction extends Action {

    private final MSJDataStore store;
    private final AlertsHandler alertsHandler;

    public MSJDataStoreCompactionAction(MSJDataStore store, AlertsHandler alertsHAndler) {
      super("compact_" + store.getPath().replaceAll("/", "_"));
      this.store = store;
      this.alertsHandler = alertsHAndler;
    }

    @Override
    protected void execute() throws Exception {
      Compactor compactor = new Compactor(alertsHandler);
      compactor.add(store.getCompactionTask());
      compactor.run();
    }
  }

  private class DeleteOldVersions extends Action {

    private final int numVersionsToKeep;
    private final MSJDataStore store;

    private DeleteOldVersions(MSJDataStore store, int numVersionsToKeep) {
      super("delete_old_versions_" + store.getPath().replaceAll("/", "_"));
      this.numVersionsToKeep = numVersionsToKeep;
      this.store = store;
      if (numVersionsToKeep < 1) {
        throw new IllegalArgumentException("At least one version must be kept");
      }
    }

    @Override
    protected void execute() throws Exception {
      FileSystem fs = FileSystemHelper.getFS();
      FileStatus[] statuses = fs.listStatus(new Path(store.getPath()));
      SortedSet<Integer> versions = Sets.newTreeSet();
      for (FileStatus status : statuses) {
        String dirName = status.getPath().getName();
        if (!dirName.equals("msj.meta")) {
          versions.add(Integer.parseInt(dirName.split("_")[1]));
        }
      }
      Iterator<Integer> iterator = versions.iterator();
      for (int i = 0; i < versions.size() - numVersionsToKeep; i++) {
        Integer version = iterator.next();
        TrashHelper.deleteUsingTrashIfEnabled(fs, new Path(store.getPath() + "/base_" + version));
        TrashHelper.deleteUsingTrashIfEnabled(fs, new Path(store.getPath() + "/mailbox_" + version));
      }
    }
  }

  public CleanUpMsjStores(String checkpointToken, String tmpRoot, int numVersionsToKeep, boolean runCompaction, AlertsHandler alertsHandler, TMSJDataStore... stores) {
    super(checkpointToken, tmpRoot);

    List<Step> steps = Lists.newArrayList();
    for (TMSJDataStore store : stores) {
      if (runCompaction) {
        steps.add(new Step(new MSJDataStoreCompactionAction(store, alertsHandler)));
      }
      steps.add(new Step(new DeleteOldVersions(store, numVersionsToKeep)));
    }

    setSubStepsFromTails(steps);
  }
}
