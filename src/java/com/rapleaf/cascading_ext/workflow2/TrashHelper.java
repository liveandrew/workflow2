package com.rapleaf.cascading_ext.workflow2;

import com.rapleaf.cascading_ext.CascadingHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;

import java.io.IOException;

public class TrashHelper {
  public static void moveToTrash(FileSystem fs, Path path) throws IOException {
    Trash trash = new Trash(fs.getConf());
    if (!trash.moveToTrash(path)) {
      throw new RuntimeException("Trash disabled or path already in trash: "+path);
    }
  }

  public static boolean isEnabled() throws IOException {
    Integer interval = Integer.parseInt(CascadingHelper.get().getJobConf().get("fs.trash.interval"));
    return interval != 0;
  }
}