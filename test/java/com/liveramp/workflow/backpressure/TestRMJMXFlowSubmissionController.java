package com.liveramp.workflow.backpressure;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import com.liveramp.cascading_tools.util.RMJMXPendingContainerChecker;
import com.liveramp.java_support.functional.Fn;
import com.liveramp.java_support.functional.IOFunction;
import com.rapleaf.java_support.CommonJUnit4TestCase;

import static org.junit.Assert.*;

public class TestRMJMXFlowSubmissionController extends CommonJUnit4TestCase {

  public TestRMJMXFlowSubmissionController() {
    super(Level.INFO);
  }

  @Test
  public void testBlocking() {
    List<String> sequence = Lists.newArrayList("10", "5");
    Iterator<String> itr = sequence.iterator();
    IOFunction<String, String> jsonRetriever = queue -> "{\"beans\":[{\"PendingContainers\":" + itr.next() + "}]}";

    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(9, TimeUnit.SECONDS, 1, jsonRetriever);

    Configuration conf = new Configuration();
    conf.set(MRJobConfig.QUEUE_NAME, "root.team.queue");

    long start = System.currentTimeMillis();
    controller.blockUntilSubmissionAllowed(conf);
    long passed = System.currentTimeMillis() - start;
    Assert.assertTrue(passed > 1000); //we slept for 1 second if this went correctly
    Assert.assertFalse(passed > 2000); //we didn't sleep twice
    Assert.assertFalse(itr.hasNext()); //we checked the pending containers twice and exhausted our dummy sequence
  }

  @Test
  public void testDoesntBlock() {
    IOFunction<String, String> jsonRetriever = queue -> "{\"beans\":[{\"PendingContainers\":5}]}";

    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(9, TimeUnit.SECONDS, 1, jsonRetriever);

    Configuration conf = new Configuration();
    conf.set(MRJobConfig.QUEUE_NAME, "root.team.queue");

    long start = System.currentTimeMillis();
    controller.blockUntilSubmissionAllowed(conf);
    long passed = System.currentTimeMillis() - start;
    Assert.assertTrue(passed < 500); //we shouldn't have slept at all
  }

  @Test
  public void determineSleepMilliseconds() throws Exception {
    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(5000, TimeUnit.MINUTES, 1, s -> s);
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(0), controller.determineSleepMilliseconds(3000));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(1), controller.determineSleepMilliseconds(5001));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(2), controller.determineSleepMilliseconds(10001));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(2), controller.determineSleepMilliseconds(15000));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(3), controller.determineSleepMilliseconds(20001));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(4), controller.determineSleepMilliseconds(40001));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(4), controller.determineSleepMilliseconds(40001));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(7), controller.determineSleepMilliseconds(400000));
  }

  @Test
  public void getContainersFromJson() throws Exception {
    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(5000, TimeUnit.MINUTES, 1, s -> s);
    Assert.assertEquals(10,
        controller.getContainersFromJson("some.queue", "{\"beans\":[{\"PendingContainers\":10}]}"));
    //if we get nothing back, react defensively and return 0 to let jobs run
    Assert.assertEquals(0,
        controller.getContainersFromJson("some.queue", "{\"beans\":[]}"));
  }

  @Test
  public void createJMXURL() throws Exception {
    String queue = "root.applications.dm.field_prep";
    assertEquals("q0=root,q1=applications,q2=dm,q3=field_prep", RMJMXFlowSubmissionController.createJMXURLSuffix(queue));

    queue = "root.applications.ow.acs";
    assertEquals("q0=root,q1=applications,q2=ow,q3=acs", RMJMXFlowSubmissionController.createJMXURLSuffix(queue));
  }

}