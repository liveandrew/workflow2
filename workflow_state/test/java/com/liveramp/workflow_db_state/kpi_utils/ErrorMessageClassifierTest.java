package com.liveramp.workflow_db_state.kpi_utils;

import com.liveramp.commons.collections.list.ListBuilder;
import org.junit.Test;

import java.util.List;

import static com.liveramp.workflow_db_state.kpi_utils.ErrorMessageClassifier.classifyFailedStepAttempt;
import static com.liveramp.workflow_db_state.kpi_utils.ErrorMessageClassifier.classifyTaskFailure;
import static org.junit.Assert.assertTrue;

/**
 * Created by lerickson on 7/27/16.
 */
public class ErrorMessageClassifierTest {

  @Test
  public void testClassifyFailedStepAttempt() throws Exception {

    List<String> trueStrings = new ListBuilder<String>()
        .add("I am unable to read from input identifier")
        .add("Failed to determine whether resource MyNameIsFrank is stored")
        .add("java.net.SocketTimeoutException: 500 millis timeout while waiting for channel to be ready for read.")
        .add("java.io.FileNotFoundException: walnut_pics.zip (Too many open files)")
        .add("Error: cascading.tuple.TupleException: unable to sink into output identifier: /data/om/tmp/WATERFALLER_QA_PROCESSOR/WATERFALLER_QA_PROCESSOR/count-esp-reach-tmp-stores/compute-reach-tmp-stores/temp-stores/explode-esps\n\tat cascading.tuple.TupleEntrySchemeCollector.collect(TupleEntrySchemeCollector.java:160)\n\tat cascading.tuple.TupleEntryCollector.safeCollect(TupleEntryCollector.java:119)\n\tat cascading.tuple.TupleEntryCollector.add(TupleEntryCollector.java:71)\n\tat cascading.tuple.TupleEntrySchemeCollector.add(TupleEntrySchemeCollector.java:134)\n\tat cascading.flow.stream.SinkStage.receive(SinkStage.java:90)\n\tat cascading.flow.stream.SinkStage.receive(SinkStage.java:37)\n\tat cascading.flow.stream.BufferEveryWindow$1.collect(BufferEveryWindow.java:68)\n\tat cascading.tuple.TupleEntryCollector.safeCollect(TupleEntryCollector.java:119)\n\tat cascading.tuple.TupleEntryCollector.add(TupleEntryCollector.java:107)\n\tat com.rapleaf.esp_summer.qa.compute_qa_reach.ExpandDistinctFieldsPerWaterfallLevel.emit(ExpandDistinctFieldsPerWaterfallLevel.java:99)\n\tat com.rapleaf.esp_summer.qa.compute_qa_reach.ExpandDistinctFieldsPerWaterfallLevel.operate(ExpandDistinctFieldsPerWaterfallLevel.java:64)\n\tat cascading.flow.stream.BufferEveryWindow.receive(BufferEveryWindow.java:125)\n\tat cascading.flow.stream.BufferEveryWindow.receive(BufferEveryWindow.java:41)\n\tat cascading.flow.hadoop.stream.HadoopGroupGate.run(HadoopGroupGate.java:93)\n\tat cascading.flow.hadoop.FlowReducer.reduce(FlowReducer.java:133)\n\tat org.apache.hadoop.mapred.ReduceTask.runOldReducer(ReduceTask.java:444)\n\tat org.apache.hadoop.mapred.ReduceTask.run(ReduceTask.java:392)\n\tat org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:168)\n\tat java.security.AccessController.doPrivileged(Native Method)\n\tat javax.security.auth.Subject.doAs(Subject.java:415)\n\tat org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1642)\n\tat org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:163)\nCaused by: java.net.SocketTimeoutException: 70000 millis timeout while waiting for channel to be ready for read. ch : java.nio.channels.SocketChannel[connected local=/10.100.130.172:46847 remote=/10.100.130.172:50010]\n\tat org.apache.hadoop.net.SocketIOWithTimeout.doIO(SocketIOWithTimeout.java:164)\n\tat org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:161)\n\tat org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:131)\n\tat org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:118)\n\tat java.io.FilterInputStream.read(FilterInputStream.java:83)\n\tat java.io.FilterInputStream.read(FilterInputStream.java:83)\n\tat org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed(PBHelper.java:2110)\n\tat org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.transfer(DFSOutputStream.java:1083)\n\tat org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.addDatanode2ExistingPipeline(DFSOutputStream.java:1053)\n\tat org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.setupPipelineForAppendOrRecovery(DFSOutputStream.java:1194)\n\tat org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.processDatanodeError(DFSOutputStream.java:945)\n\tat org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.run(DFSOutputStream.java:496)\n")
        .get();

    List<String> falseStrings = new ListBuilder<String>()
        .add("Container killed by the ApplicationMaster.\nContainer killed on request. Exit code is 143\nContainer exited with a non-zero exit code 143\n")
        .add("Error: Java heap space")
        .add("Error: cascading.pipe.OperatorException: [pipe][com.example.tutorial.Transform2.<init>(Transform2.java:66)] operator Each failed executing operation\n\tat cascading.flow.stream.FunctionEachStage.receive(FunctionEachStage.java:107)\n\tat cascading.flow.stream.FunctionEachStage.receive(FunctionEachStage.java:39)\n\tat cascading.flow.stream.SourceStage.map(SourceStage.java:102)\n\tat cascading.flow.stream.SourceStage.run(SourceStage.java:58)\n\tat cascading.flow.hadoop.FlowMapper.run(FlowMapper.java:127)\n\tat org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:450)\n\tat org.apache.hadoop.mapred.MapTask.run(MapTask.java:343)\n\tat org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:168)\n\tat java.security.AccessController.doPrivileged(Native Method)\n\tat javax.security.auth.Subject.doAs(Subject.java:415)\n\tat org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1642)\n\tat org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:163)\nCaused by: java.lang.NullPointerException\n\tat com.liveramp.types.audience.S2SIntegration$IdSync$Builder.setPartnerUid(S2SIntegration.java:2709)\n\tat com.example.tutorial.Transform2$Funct.toIdSyncCookie(Transform2.java:137)\n\tat com.example.tutorial.Transform2$Funct.operate(Transform2.java:95)\n\tat com.rapleaf.cascading_ext.msj_tap.operation.BaseMSJFunction.operate(BaseMSJFunction.java:67)\n\tat cascading.flow.stream.FunctionEachStage.receive(FunctionEachStage.java:99)\n\t... 11 more\n")
        .get();

    for (String ts:
        trueStrings) {
      assertTrue(classifyFailedStepAttempt(ts));
      assertTrue(classifyFailedStepAttempt(ts,null,null));
    }
    for (String fs:
        falseStrings) {
      assertTrue(!classifyFailedStepAttempt(fs));
      assertTrue(!classifyFailedStepAttempt(fs,null,null));
    }
  }

  @Test
  public void testClassifyTaskFailure() throws Exception {
    List<String> trueStrings = new ListBuilder<String>()
        .add("mkdir of And it was a sad story when my father died. Because me mother called me on the phone and she said, \"You know, your dad died.\" And this was exactly two months before a contest. \"Are you coming home for the funeral?\" She said. I said: \"No. It's too late. He's dead and nothing can be done. I'm sorry I can't come.\" And I didn't explain the reasons why, because how do you explain to a mother whose husband died, you just can't be bothered now because of a contest? -Schwarzenneger failed")
        .add("Error: java.io.IOException: Failed to finalize bucket file /baa/baa/black/sheep")
        .add("Not able to initialize app-cache directories in any of the configured local directories for user svc-om")
        .add("Not able to initialize app-cache directories in any of the configured local directories for user rapleaf\n")
        .add("/bin/ls")
        .add("Error: cascading.tuple.TupleException: unable to sink into output identifier: /data/om/tmp/WATERFALLER_QA_PROCESSOR/WATERFALLER_QA_PROCESSOR/count-esp-reach-tmp-stores/compute-reach-tmp-stores/temp-stores/explode-esps\n\tat cascading.tuple.TupleEntrySchemeCollector.collect(TupleEntrySchemeCollector.java:160)\n\tat cascading.tuple.TupleEntryCollector.safeCollect(TupleEntryCollector.java:119)\n\tat cascading.tuple.TupleEntryCollector.add(TupleEntryCollector.java:71)\n\tat cascading.tuple.TupleEntrySchemeCollector.add(TupleEntrySchemeCollector.java:134)\n\tat cascading.flow.stream.SinkStage.receive(SinkStage.java:90)\n\tat cascading.flow.stream.SinkStage.receive(SinkStage.java:37)\n\tat cascading.flow.stream.BufferEveryWindow$1.collect(BufferEveryWindow.java:68)\n\tat cascading.tuple.TupleEntryCollector.safeCollect(TupleEntryCollector.java:119)\n\tat cascading.tuple.TupleEntryCollector.add(TupleEntryCollector.java:107)\n\tat com.rapleaf.esp_summer.qa.compute_qa_reach.ExpandDistinctFieldsPerWaterfallLevel.emit(ExpandDistinctFieldsPerWaterfallLevel.java:99)\n\tat com.rapleaf.esp_summer.qa.compute_qa_reach.ExpandDistinctFieldsPerWaterfallLevel.operate(ExpandDistinctFieldsPerWaterfallLevel.java:64)\n\tat cascading.flow.stream.BufferEveryWindow.receive(BufferEveryWindow.java:125)\n\tat cascading.flow.stream.BufferEveryWindow.receive(BufferEveryWindow.java:41)\n\tat cascading.flow.hadoop.stream.HadoopGroupGate.run(HadoopGroupGate.java:93)\n\tat cascading.flow.hadoop.FlowReducer.reduce(FlowReducer.java:133)\n\tat org.apache.hadoop.mapred.ReduceTask.runOldReducer(ReduceTask.java:444)\n\tat org.apache.hadoop.mapred.ReduceTask.run(ReduceTask.java:392)\n\tat org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:168)\n\tat java.security.AccessController.doPrivileged(Native Method)\n\tat javax.security.auth.Subject.doAs(Subject.java:415)\n\tat org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1642)\n\tat org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:163)\nCaused by: java.net.SocketTimeoutException: 70000 millis timeout while waiting for channel to be ready for read. ch : java.nio.channels.SocketChannel[connected local=/10.100.130.172:46847 remote=/10.100.130.172:50010]\n\tat org.apache.hadoop.net.SocketIOWithTimeout.doIO(SocketIOWithTimeout.java:164)\n\tat org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:161)\n\tat org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:131)\n\tat org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:118)\n\tat java.io.FilterInputStream.read(FilterInputStream.java:83)\n\tat java.io.FilterInputStream.read(FilterInputStream.java:83)\n\tat org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed(PBHelper.java:2110)\n\tat org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.transfer(DFSOutputStream.java:1083)\n\tat org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.addDatanode2ExistingPipeline(DFSOutputStream.java:1053)\n\tat org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.setupPipelineForAppendOrRecovery(DFSOutputStream.java:1194)\n\tat org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.processDatanodeError(DFSOutputStream.java:945)\n\tat org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.run(DFSOutputStream.java:496)\n")
        .get();

    List<String> falseStrings = new ListBuilder<String>()
        .add("Container killed by the ApplicationMaster.\nContainer killed on request. Exit code is 143\nContainer exited with a non-zero exit code 143\n")
        .add("Error: Java heap space")
        .add("Error: cascading.pipe.OperatorException: [pipe][com.example.tutorial.Transform2.<init>(Transform2.java:66)] operator Each failed executing operation\n\tat cascading.flow.stream.FunctionEachStage.receive(FunctionEachStage.java:107)\n\tat cascading.flow.stream.FunctionEachStage.receive(FunctionEachStage.java:39)\n\tat cascading.flow.stream.SourceStage.map(SourceStage.java:102)\n\tat cascading.flow.stream.SourceStage.run(SourceStage.java:58)\n\tat cascading.flow.hadoop.FlowMapper.run(FlowMapper.java:127)\n\tat org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:450)\n\tat org.apache.hadoop.mapred.MapTask.run(MapTask.java:343)\n\tat org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:168)\n\tat java.security.AccessController.doPrivileged(Native Method)\n\tat javax.security.auth.Subject.doAs(Subject.java:415)\n\tat org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1642)\n\tat org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:163)\nCaused by: java.lang.NullPointerException\n\tat com.liveramp.types.audience.S2SIntegration$IdSync$Builder.setPartnerUid(S2SIntegration.java:2709)\n\tat com.example.tutorial.Transform2$Funct.toIdSyncCookie(Transform2.java:137)\n\tat com.example.tutorial.Transform2$Funct.operate(Transform2.java:95)\n\tat com.rapleaf.cascading_ext.msj_tap.operation.BaseMSJFunction.operate(BaseMSJFunction.java:67)\n\tat cascading.flow.stream.FunctionEachStage.receive(FunctionEachStage.java:99)\n\t... 11 more\n")
        .add("/bin/ls but not by itself")
        .get();

    for (String ts:
        trueStrings) {
      assertTrue(classifyTaskFailure(ts));
    }
    for (String fs:
        falseStrings) {
      assertTrue(!classifyTaskFailure(fs));
    }
  }



}