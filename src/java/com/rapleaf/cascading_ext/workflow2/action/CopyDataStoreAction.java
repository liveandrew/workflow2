package com.rapleaf.cascading_ext.workflow2.action;

import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_tools.util.DirectoryDistCp;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

// The generic type here enforces that src and dst are of the same type
public class CopyDataStoreAction<T extends DataStore> extends Action {
  private final T src;
  private final T dst;
  private final DirectoryDistCp distCp;

  public CopyDataStoreAction(final String checkpointToken,
                             final String tmpRoot,
                             final T src,
                             final T dst) {
    super(checkpointToken, tmpRoot);
    this.src = src;
    this.dst = dst;
    this.distCp = new DirectoryDistCp();

    readsFrom(src);
    creates(dst);
  }

  @Override
  protected void execute() throws Exception {
    distCp.copyDirectory(new Path(src.getPath()), new Path(dst.getPath()));
  }
}
