package com.rapleaf.cascading_ext.workflow2.helpers;

import com.google.common.base.Function;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.liveramp.cascading_ext.function.DirectFn;
import com.rapleaf.cascading_ext.CascadingHelper;

public class ApplyFunctionToFile {

  public static void transform(String hdfsInputPath, Function<String, String> fn, String hdfsOutputPath) {
    Tap input = new Hfs(new TextLine(), hdfsInputPath);

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, new Fields("line"), new DirectFn<>(new Fields("output"), fn), Fields.RESULTS);

    Tap output = new Hfs(new TextLine(new Fields("line"), new Fields("output")), hdfsOutputPath, SinkMode.REPLACE);

    CascadingHelper.get().getFlowConnector().connect("TransformFile", input, output, pipe).complete();
  }
}
