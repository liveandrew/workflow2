package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Iterator;
import java.util.SortedSet;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

class DeleteOldVersions extends Action {

  private final int numVersionsToKeep;
  private final MSJDataStore store;

  DeleteOldVersions(MSJDataStore store, int numVersionsToKeep) {
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
