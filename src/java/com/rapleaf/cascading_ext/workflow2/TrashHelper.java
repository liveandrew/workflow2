package com.rapleaf.cascading_ext.workflow2;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.CascadingHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.TrashPolicyDefault;

import java.io.IOException;

public class TrashHelper {
  public static void moveToTrash(FileSystem fs, Path path) throws IOException {
    if(!Trash.moveToAppropriateTrash(fs, path, CascadingHelper.get().getJobConf())){
      throw new RuntimeException("Trash disabled or path already in trash: " + path);
    }
  }

  public static void moveToTrashIfEnabled(FileSystem fs, Path path) throws IOException {
    if(isEnabled()){
      moveToTrash(fs, path);
    }else{
      fs.delete(path, true);
    }
  }

  public static boolean isEnabled() throws IOException {

    //  check if enabled locally
    Integer interval = Integer.parseInt(CascadingHelper.get().getJobConf().get("fs.trash.interval"));

    if (interval != 0) {
      return true;
    }

    //  it could also be configured on the namenode rather than locally, so look there
    FileSystem fs = FileSystemHelper.getFS();
    long trashInterval = fs.getServerDefaults(new Path("/tmp/some_dummy_path")).getTrashInterval();

    return trashInterval != 0;
  }
}
