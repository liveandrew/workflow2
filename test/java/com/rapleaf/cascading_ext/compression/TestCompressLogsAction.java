package com.rapleaf.cascading_ext.compression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTime;
import org.junit.Test;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.types.spruce.LogApp;

import static org.junit.Assert.assertEquals;

public class TestCompressLogsAction extends WorkflowTestCase {
  private final String INPUT_PATH = getTestRoot() + "/input-lines";
  private static final String COMPRESSION_EXTENSION = readCompressionExtension();

  private void execute(String inputPath, String tmpRoot, String backupRoot, String inputCodec, String outputCodec) throws IOException {
    new WorkflowRunner(
        CompressLogsAction.class,
        new TestWorkflowOptions(),
        new Step(new CompressLogsAction(inputPath, tmpRoot, backupRoot, inputCodec, outputCodec, LogApp.S2S_DATA_SYNCER, DateTime.now()))
    ).run();
  }

  @Test
  public void testCompression() throws Exception {

    List<String> inputLines = Lists.newArrayList("first line", "second line", "third line", "fourth line", "fifth line");

    writeLines(INPUT_PATH, false, inputLines, "some_strange_filename_05000", null);

    String backupRoot = getTestRoot() + "/backup1";

    execute(INPUT_PATH, getTestRoot() + "/tmp1", backupRoot, "none", "bzip2");

    assertEquals(inputLines, readLines(INPUT_PATH));

    assertEquals(
        "some_strange_filename_05000" + COMPRESSION_EXTENSION,
        new Path(getFirstDataFile(INPUT_PATH)).getName());

    List<String> backupLines = readLines(backupRoot + "/" + INPUT_PATH);

    assertEquals(inputLines, backupLines);
  }

  @Test
  public void testDecompression() throws IOException {
    List<String> inputLines = Lists.newArrayList("first line", "second line", "third line", "fourth line", "fifth line");

    writeLines(INPUT_PATH, true, inputLines, "some_strange_filename_05000", BZip2Codec.class);

    String backupRoot = getTestRoot() + "/backup2";

    execute(INPUT_PATH, getTestRoot() + "/tmp2", backupRoot, "bzip2", "none");

    assertEquals(inputLines, readLines(INPUT_PATH));

    assertEquals(
        "some_strange_filename_05000" + "",
        new Path(getFirstDataFile(INPUT_PATH)).getName());

    List<String> backupLines = readLines(backupRoot + "/" + INPUT_PATH);

    assertEquals(inputLines, backupLines);
  }

  @Test
  public void testRoundTrip() throws IOException {
    List<String> inputLines = Lists.newArrayList("first line", "second line", "third line", "fourth line", "fifth line");

    writeLines(INPUT_PATH, false, inputLines, "some_strange_filename_05000", null);

    execute(INPUT_PATH, getTestRoot() + "/tmp3", getTestRoot() + "/backup3", "none", "bzip2");
    execute(INPUT_PATH, getTestRoot() + "/tmp4", getTestRoot() + "/backup4", "bzip2", "lzo");
    execute(INPUT_PATH, getTestRoot() + "/tmp5", getTestRoot() + "/backup5", "lzo", "bzip2");
    execute(INPUT_PATH, getTestRoot() + "/tmp6", getTestRoot() + "/backup6", "bzip2", "none");

    String backupRoot = getTestRoot() + "/backup6";

    assertEquals(inputLines, readLines(INPUT_PATH));

    assertEquals(
        "some_strange_filename_05000" + "",
        new Path(getFirstDataFile(INPUT_PATH)).getName());

    List<String> backupLines = readLines(backupRoot + INPUT_PATH);

    assertEquals(inputLines, backupLines);
  }

  private void writeLines(String path, boolean compress, List<String> lines, String name, Class<? extends CompressionCodec> codec) throws IOException {
    TextLine scheme;

    if (compress) {
      scheme = new TextLine(new Fields("line"), new Fields("line"), TextLine.Compress.ENABLE);
    } else {
      scheme = new TextLine(new Fields("line"));
    }

    JobConf conf = CascadingHelper.get().getJobConf();
    if (codec != null) {
      conf.set("mapred.output.compression.codec", codec.getCanonicalName());
    }

    TupleEntryCollector coll = new Hfs(scheme, path).openForWrite(new HadoopFlowProcess(conf));

    for (String line : lines) {
      coll.add(new Tuple(line));
    }

    coll.close();

    String extension = "";
    if (codec != null) {
      try {
        extension = codec.newInstance().getDefaultExtension();
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    fs.rename(new Path(path + "/part-00000" + extension), new Path(path + "/" + name + extension));
  }

  private List<String> readLines(String path) throws IOException {
    TupleEntryIterator iterator = new Hfs(new TextLine(new Fields("offset", "line")), path).openForRead(CascadingHelper.get().getFlowProcess());

    List<String> lines = new ArrayList<String>();

    while (iterator.hasNext()) {
      lines.add(iterator.next().getString(1));
    }

    iterator.close();

    return lines;
  }

  private String getFirstDataFile(String path) throws IOException {
    FileStatus[] statuses = fs.listStatus(new Path(path));

    for (FileStatus status : statuses) {
      String pathStr = status.getPath().toString();

      if (!pathStr.contains("_SUCCESS")) {
        return pathStr;
      }
    }

    throw new RuntimeException("Did not find any files under " + path);
  }

  private static String readCompressionExtension() {
    return new BZip2Codec().getDefaultExtension();
  }
}
