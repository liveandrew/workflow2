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

import com.liveramp.java_support.functional.IOFunction;
import com.rapleaf.java_support.CommonJUnit4TestCase;

import static org.junit.Assert.*;

public class TestRMJMXFlowSubmissionController extends CommonJUnit4TestCase {

  public TestRMJMXFlowSubmissionController() {
    super(Level.INFO);
  }

  @Test
  public void testBlocking() {
    List<String> sequence = Lists.newArrayList("1", "10", "1", "5");
    Iterator<String> itr = sequence.iterator();
    IOFunction<String, String> jsonRetriever = queue -> "{\"beans\":[{\"AppsRunning\":" + itr.next() + ",\"PendingContainers\":" + itr.next() + "}]}";

    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(
            9,
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS, 100,
            TimeUnit.HOURS, 1,
            jsonRetriever);

    Configuration conf = new Configuration();
    conf.set(MRJobConfig.QUEUE_NAME, "root.team.queue");

    long start = System.currentTimeMillis();
    controller.blockUntilSubmissionAllowed(conf);
    long passed = System.currentTimeMillis() - start;
    Assert.assertTrue(passed >= 100); //we slept for 1 wait period if this went correctly
    Assert.assertTrue(passed < 200); //we didn't sleep twice
    Assert.assertFalse(itr.hasNext()); //we checked the pending containers twice and exhausted our dummy sequence
  }

  @Test
  public void testBlockingMaxApps() {
    List<String> sequence = Lists.newArrayList("10", "1", "1", "1");
    Iterator<String> itr = sequence.iterator();
    IOFunction<String, String> jsonRetriever = queue -> "{\"beans\":[{\"AppsRunning\":" + itr.next() + ",\"PendingContainers\":" + itr.next() + "}]}";

    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(
            9,
            5,
            TimeUnit.MILLISECONDS, 100,
            TimeUnit.HOURS, 1,
            jsonRetriever);

    Configuration conf = new Configuration();
    conf.set(MRJobConfig.QUEUE_NAME, "root.team.queue");

    long start = System.currentTimeMillis();
    controller.blockUntilSubmissionAllowed(conf);
    long passed = System.currentTimeMillis() - start;
    Assert.assertTrue(passed >= 100); //we slept for 1 wait period if this went correctly
    Assert.assertTrue(passed < 1500); //we didn't sleep twice
    Assert.assertFalse(itr.hasNext()); //we checked the pending containers twice and exhausted our dummy sequence
  }


  @Test
  public void testDoesntBlock() {
    IOFunction<String, String> jsonRetriever = queue -> "{\"beans\":[{\"AppsRunning\":1\",PendingContainers\":5}]}";

    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(
            9,
            Integer.MAX_VALUE,
            TimeUnit.SECONDS, 1,
            TimeUnit.HOURS, 1,
            jsonRetriever);

    Configuration conf = new Configuration();
    conf.set(MRJobConfig.QUEUE_NAME, "root.team.queue");

    long start = System.currentTimeMillis();
    controller.blockUntilSubmissionAllowed(conf);
    long passed = System.currentTimeMillis() - start;
    Assert.assertTrue(passed < 500); //we shouldn't have slept at all
  }

  @Test
  public void testMaxWait() {
    //Setup mocks so that there are always too many containers, so we should hit max wait
    IOFunction<String, String> jsonRetriever = queue -> "{\"beans\":[{\"AppsRunning\":\"1\",\"PendingContainers\":5}]}";

    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(
            4,
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS, 10,
            TimeUnit.MILLISECONDS, 100,
            jsonRetriever);

    Configuration conf = new Configuration();
    conf.set(MRJobConfig.QUEUE_NAME, "root.team.queue");

    long start = System.currentTimeMillis();
    controller.blockUntilSubmissionAllowed(conf);
    long passed = System.currentTimeMillis() - start;
    Assert.assertTrue(passed < 120 && passed >= 100);
  }

  @Test
  public void testMaxWaitPremptsWait() {
    //Setup mocks so that there are always too many containers, so we should hit max wait
    IOFunction<String, String> jsonRetriever = queue -> "{\"beans\":[{\"AppsRunning\":\"1\",\"PendingContainers\":5}]}";

    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(
            4,
            Integer.MAX_VALUE,
            TimeUnit.SECONDS, 1,
            TimeUnit.MILLISECONDS, 100,
            jsonRetriever);

    Configuration conf = new Configuration();
    conf.set(MRJobConfig.QUEUE_NAME, "root.team.queue");

    long start = System.currentTimeMillis();
    controller.blockUntilSubmissionAllowed(conf);
    long passed = System.currentTimeMillis() - start;
    Assert.assertTrue(passed < 120 && passed >= 100);
  }

  @Test
  public void determineSleepMilliseconds() throws Exception {
    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(5000, Integer.MAX_VALUE, TimeUnit.MINUTES, 1, TimeUnit.HOURS, 1, s -> s);
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(0), controller.determineSleep(3000, 5000, Integer.MAX_VALUE));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(1), controller.determineSleep(5001, 5000, Integer.MAX_VALUE));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(2), controller.determineSleep(10001, 5000, Integer.MAX_VALUE));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(2), controller.determineSleep(15000, 5000, Integer.MAX_VALUE));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(3), controller.determineSleep(20001, 5000, Integer.MAX_VALUE));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(4), controller.determineSleep(40001, 5000, Integer.MAX_VALUE));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(4), controller.determineSleep(40001, 5000, Integer.MAX_VALUE));
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(7), controller.determineSleep(400000, 5000, Integer.MAX_VALUE));

    //Can't be larger than max wait
    Assert.assertEquals(6000, controller.determineSleep(Integer.MAX_VALUE, 5000, 6000));

    //Can't be negative
    Assert.assertEquals(0, controller.determineSleep(Integer.MAX_VALUE, 5000, -6000));

    // Changing the log factor extends sleep period
    Assert.assertEquals(60000, controller.determineSleep(400, 300, Integer.MAX_VALUE, 2));
    Assert.assertEquals(300000, controller.determineSleep(400, 300, Integer.MAX_VALUE, 1.07));
    Assert.assertEquals(900000, controller.determineSleep(800, 300, Integer.MAX_VALUE, 1.07));

  }

  @Test
  public void getContainersFromJson() throws Exception {
    RMJMXFlowSubmissionController controller =
        new RMJMXFlowSubmissionController(5000, Integer.MAX_VALUE, TimeUnit.MINUTES, 1, TimeUnit.HOURS, 1, s -> s);
    Assert.assertEquals(new RMJMXFlowSubmissionController.QueueInfo(10, 1),
        controller.getInfoFromJson("some.queue", "{\"beans\":[{\"AppsRunning\":\"1\",\"PendingContainers\":10}]}"));
    //if we get nothing back, react defensively and return 0 to let jobs run
    Assert.assertEquals(new RMJMXFlowSubmissionController.QueueInfo(0, 0),
        controller.getInfoFromJson("some.queue", "{\"beans\":[]}"));
  }

  @Test
  public void createJMXURL() throws Exception {
    String queue = "root.applications.dm.field_prep";
    assertEquals("q0=root,q1=applications,q2=dm,q3=field_prep", RMJMXFlowSubmissionController.createJMXURLSuffix(queue));

    queue = "root.applications.ow.acs";
    assertEquals("q0=root,q1=applications,q2=ow,q3=acs", RMJMXFlowSubmissionController.createJMXURLSuffix(queue));
  }

}