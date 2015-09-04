package com.rapleaf.cascading_ext.compression;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.util.generated.RenameAction;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.types.spruce.LogApp;

@SuppressWarnings("unchecked")

public class CompressLogsAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(CompressLogsAction.class);

  private static final Pattern FILE_PART_PATTERN = Pattern.compile("part(?:\\-|_)(\\d+)");

  private static final Map<String, Class<? extends CompressionCodec>> SUPPORTED_CODECS;
  private final FileSystem fs;
  private final String inputPath;
  private final CoercionConfig config;
  private final String backupRoot;
  private String tmpPath;
  private final Map<String, Object> flowProperties;

  static {
    SUPPORTED_CODECS = new HashMap<>();
    SUPPORTED_CODECS.put("gzip", GzipCodec.class);
    SUPPORTED_CODECS.put("lzo", LzoCodec.class);
    SUPPORTED_CODECS.put("lzo_deflate", LzoCodec.class);
    SUPPORTED_CODECS.put("lzop", LzopCodec.class);
    SUPPORTED_CODECS.put("snappy", SnappyCodec.class);
    SUPPORTED_CODECS.put("bzip2", BZip2Codec.class);
    SUPPORTED_CODECS.put("none", null);
  }

  public CompressLogsAction(String inputPath,
                            String tmpRoot,
                            String backupRoot,
                            String inputCodec,
                            String outputCodec,
                            LogApp logApp,
                            DateTime date,
                            Map<String, Object> flowProperties) throws IOException {
    super(String.format("%s-%s-%s", CompressLogsAction.class.getSimpleName(), logApp, date.toString("yyyy_MM_dd")), tmpRoot);
    this.inputPath = inputPath;
    this.backupRoot = backupRoot;
    this.config = new CoercionConfig(getCodecForName(inputCodec), getCodecForName(outputCodec));
    this.fs = FileSystem.get(new Configuration());
    this.tmpPath = getTmpRoot() + "/" + UUID.randomUUID().toString();
    this.flowProperties = flowProperties;
  }

  @Override
  protected void execute() throws Exception {
    if (!isValidCodec()) {
      LOG.error("Directory is not using given codec: %s vs %s", inputPath, config.getSourceExtension());
      return;
    }

    String jobName = String.format("%s [(%s) %s]", getClass().getSimpleName(), config.getName(), inputPath);
    try {
      final String tmpOutputParts = getTmpPath() + "/parts";
      final String tmpOutputRenames = getTmpPath() + "/rename_actions";

      Pipe pipe = new Each(
          "pipe",
          new Fields("line"),
          new CompressAndEmitRenameAction(tmpOutputRenames));

      Tap source = new Hfs(
          new TextLine(
              new Fields("line"),
              new Fields("line")),
          inputPath);

      Tap sink = new Hfs(
          new TextLine(
              new Fields("line"),
              new Fields("line"),
              config.getCompressionMode()),
          tmpOutputParts);

      Map<Object, Object> properties = new HashMap<Object, Object>();
      properties.put("mapreduce.input.fileinputformat.split.minsize", Long.MAX_VALUE);
      properties.put("io.compression.codecs", getCodecsProperty());
      properties.putAll(flowProperties);

      if (config.getSinkCodec() != null) {
        properties.put("mapred.output.compression.codec", config.getSinkCodec().getClass().getCanonicalName());
      }

      completeWithProgress(buildFlow(properties).connect(jobName, source, sink, pipe));

      renameOutputParts(tmpOutputRenames, tmpOutputParts);

      // create backup directory
      Path backupLocation = new Path(backupRoot + "/" + inputPath);
      LOG.info("creating directory " + backupLocation.getParent());
      FileSystemHelper.safeMkdirs(fs, backupLocation.getParent());

      // this backing up is temporary until we're confident there are no issues in production
      LOG.info("moving " + inputPath + " to backup location: " + backupLocation);
      FileSystemHelper.safeRename(fs, new Path(inputPath), backupLocation, 10, 5 * 1000L);

      LOG.info("moving " + tmpOutputParts + " to final location: " + inputPath);
      FileSystemHelper.safeRename(fs, new Path(tmpOutputParts), new Path(inputPath), 10, 5 * 1000L);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        LOG.info("Deleting tmpPath: " + getTmpPath());
        TrashHelper.deleteUsingTrashIfEnabled(fs, new Path(getTmpPath()));
      } catch (IOException e) {
        LOG.error("Could not delete: " + getTmpPath(), e);
      }
    }
  }

  private void renameOutputParts(String renamesFile, String dir) throws IOException {
    // Map from task index to source path.  In other words,
    // it says, for each map task, which file was consumed by
    // that task.  We then use this map to preserve
    // the original file names
    Map<Integer, String> partNumToSourcePath = new HashMap<Integer, String>();

    TupleEntryIterator iterator = new Hfs(new SequenceFile(new Fields("rename_action")), renamesFile).openForRead(
        CascadingHelper.get().getFlowProcess());

    while (iterator.hasNext()) {
      RenameAction renameAction = (RenameAction)iterator.next().getObject(0);
      partNumToSourcePath.put(renameAction.get_task_num(), renameAction.get_src_path());
    }

    for (FileStatus status : fs.listStatus(new Path(dir))) {
      Path path = status.getPath();

      if (isDummyFile(path)) {
        TrashHelper.deleteUsingTrashIfEnabled(fs, path);
        continue;
      }

      String sourcePath = getSourcePathForFile(path, partNumToSourcePath);
      Path destPath = new Path(path.getParent(), config.getDestFileName(sourcePath));

      LOG.info("Renaming: {} to {}", path, destPath);
      FileSystemHelper.safeRename(fs, path, destPath, 10, 5 * 1000L);
    }
  }

  private String getSourcePathForFile(Path path, Map<Integer, String> partNumToSourcePath) {
    Matcher matcher = FILE_PART_PATTERN.matcher(path.getName());
    Integer partNum;

    if (matcher.find()) {
      partNum = Integer.valueOf(matcher.group(1));
    } else {
      throw new RuntimeException("Could not find a part from: " + path);
    }

    String sourcePath = partNumToSourcePath.get(partNum);
    if (sourcePath != null) {
      return sourcePath;
    } else {
      throw new RuntimeException("Found null path for part: " + partNum + " from path: " + path);
    }
  }

  private boolean isDummyFile(Path path) throws IOException {
    final String strPath = path.toString();
    return strPath.endsWith("/_SUCCESS") || strPath.endsWith("_logs");
  }

  private String getTmpPath() {
    return tmpPath;
  }

  private boolean isValidCodec() throws IOException {
    boolean valid;

    if (config.getSourceCodec() == null) {
      valid = !isCompressed(fs, inputPath);
    } else {
      valid = validateExtension(fs, inputPath, config.getSourceCodec());
    }
    return valid;
  }

  public static boolean isCompressed(FileSystem fs, String dir) throws IOException {
    for (Class<? extends CompressionCodec> codecClass : SUPPORTED_CODECS.values()) {
      if (codecClass != null) {
        if (validateExtension(fs, dir, getCodec(codecClass))) {
          return true; // source is compressed using codecClass
        }
      }
    }
    return false;
  }

  private static CompressionCodec getCodec(Class<? extends CompressionCodec> codecClass) {
    try {
      return codecClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static CompressionCodec getCodecForName(String name) {
    if (!SUPPORTED_CODECS.containsKey(name)) {
      throw new RuntimeException("Unsupported codec: " + name);
    } else {
      Class<? extends CompressionCodec> codecClass = SUPPORTED_CODECS.get(name);
      if (codecClass == null) {
        return null;
      } else {
        return getCodec(codecClass);
      }
    }
  }

  public static boolean validateExtension(FileSystem fs, String dir, CompressionCodec codec) throws IOException {
    FileStatus[] statuses = fs.listStatus(new Path(dir));
    String extension = codec == null ? "" : codec.getDefaultExtension();

    for (FileStatus status : statuses) {
      if (!status.getPath().toString().endsWith(extension)) {
        return false;
      }
    }

    return true;
  }

  public String getCodecsProperty() {
    StringBuilder property = new StringBuilder();

    for (Class<? extends CompressionCodec> compressionCodecClass : SUPPORTED_CODECS.values()) {
      if (compressionCodecClass != null) {
        property.append(compressionCodecClass.getCanonicalName());
        property.append(",");
      }
    }

    String propertyStr = property.toString();

    // remove trailing ","
    return propertyStr.substring(0, propertyStr.length() - 1);
  }

  public static Map<String, Class<? extends CompressionCodec>> getSupportedCodecs() {
    return SUPPORTED_CODECS;
  }
}
