package com.rapleaf.cascading_ext.workflow2.action;

import com.google.common.collect.Sets;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.stream.RecordOutputStream;
import com.rapleaf.support.FileSystemHelper;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;

public class AddMissingPartitionsToBucket extends Action {
  private final int numPartitions;
  private final BucketDataStore dataStore;
  private static final Pattern FILE_PART_PATTERN = Pattern.compile("part\\-(\\d+)(?:(?:\\.|_)|$)");

  public AddMissingPartitionsToBucket(String checkpointToken, int numPartitions, BucketDataStore dataStore) {
    super(checkpointToken);
    this.numPartitions = numPartitions;
    this.dataStore = dataStore;

    writesTo(dataStore);
  }

  @Override
  protected void execute() throws Exception {
    Set<Integer> existingPartitions = Sets.newHashSet();

    for (Path path : dataStore.getBucket().getStoredFiles()) {
      existingPartitions.add(getFilePart(path.toString()));
    }

    Bucket bucket = Bucket.open(FileSystemHelper.getFS(), dataStore.getPath());

    for (int partition = 0; partition < numPartitions; partition++) {
      if (!existingPartitions.contains(partition)) {
        RecordOutputStream os = bucket.openWrite(buildPartFile(partition));
        os.close();
      }
    }
  }

  protected static Integer getFilePart(String fileName) {
    Matcher matcher = FILE_PART_PATTERN.matcher(new Path(fileName).getName());
    if (matcher.find()) {
      return Integer.valueOf(matcher.group(1));
    } else {
      throw new IllegalArgumentException("File name does not have a file part!: " + fileName);
    }
  }

  protected String buildPartFile(Integer partition) {
    return String.format("part-%05d_0", partition);
  }
}
