package com.liveramp.workflow.serialization;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.FlowProcess;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.test.HadoopCommonJunit4TestCase;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.db_schemas.rldb.models.CookieMonsterReaderDaysum;
import com.rapleaf.db_schemas.rldb.models.ElmoDaysum;
import com.rapleaf.db_schemas.rldb.models.ElmoPublisherVolume;
import com.rapleaf.db_schemas.rldb.models.SpruceQaDaysum;
import com.rapleaf.types.new_person_data.IntList;

import static org.junit.Assert.assertEquals;

public class TestJackModelSerialization extends WorkflowTestCase {

  private final String input = getTestRoot() + "/input";

  @Test
  public void testSerialization() throws Exception {

    IntList list1 = new IntList().set_ints(Lists.newArrayList(1, 1, 1));
    IntList list2 = new IntList().set_ints(Lists.newArrayList(2, 2, 2));
    IntList list3 = new IntList().set_ints(Lists.newArrayList(3, 3, 3));

    List<IntList> daysums1 = runFlowAndGetOutput(getTestRoot()+"/output1", list1, list2, list1, list3);
    List<IntList> daysums2 = runFlowAndGetOutput(getTestRoot()+"/output2", list1, list3, list2, list1);

    // Check that the output is sorted consistently
    assertEquals(daysums1, daysums2);
  }

  private List<IntList> runFlowAndGetOutput(String output, IntList... objs) throws Exception {
    FlowProcess<JobConf> fp = CascadingHelper.get().getFlowProcess();

    Hfs source = new Hfs(new SequenceFile(new Fields("jack_obj")), input);
    TupleEntryCollector tec = source.openForWrite(fp);

    for (IntList obj : objs) {
      tec.add(new Tuple(obj));
    }
    tec.close();

    Hfs sink = new Hfs(new SequenceFile(new Fields("jack_obj")), output);

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, new Fields("jack_obj"), new Identity());
    pipe = new GroupBy(pipe, new Fields("jack_obj"));

    HadoopCommonJunit4TestCase.flowConnector().connect(source, sink, pipe).complete();

    TupleEntryIterator it = sink.openForRead(fp);

    List<IntList> daysums = new ArrayList<IntList>();
    while (it.hasNext()) {
      TupleEntry te = it.next();
      IntList desObj = (IntList) te.getObject(0);
      daysums.add(desObj);
    }
    return daysums;
  }

}
