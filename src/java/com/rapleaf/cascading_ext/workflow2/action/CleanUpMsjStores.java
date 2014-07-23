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

  private class DeleteOldVersions extends Action {

    private final int numVersionsToKeep;
    private final MSJDataStore store;

    private DeleteOldVersions(MSJDataStore store, int numVersionsToKeep) {
      super("compact_" + store.getPath().replaceAll("/", "_"));
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
        versions.add(Integer.parseInt(dirName.split("_")[1]));
      }
      Iterator<Integer> iterator = versions.iterator();
      for (int i = 0; i < versions.size() - numVersionsToKeep; i++) {
        Integer version = iterator.next();
        fs.delete(new Path(store.getPath() + "/base_" + version), true);
        fs.delete(new Path(store.getPath() + "/mailbox_" + version), true);
      }
    }
  }

  public CleanUpMsjStores(String checkpointToken, int numVersionsToKeep, boolean runCompaction, TMSJDataStore... stores) {
    super(checkpointToken);

    List<Step> steps = Lists.newArrayList();
    for (TMSJDataStore store : stores) {
      if (runCompaction) {
        steps.add(new Step(new MSJDataStoreCompactionAction(store)));
      }
      steps.add(new Step(new DeleteOldVersions(store, numVersionsToKeep)));
    }

    setSubStepsFromTails(steps);
  }
}
