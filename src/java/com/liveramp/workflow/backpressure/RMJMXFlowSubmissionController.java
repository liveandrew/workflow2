package com.liveramp.workflow.backpressure;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.utils.EnsurePath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.functional.IOFunction;
import com.liveramp.java_support.web.LRHttpUtils;
import com.liveramp.zk_tools.CuratorFrameworkDefaults;

public class RMJMXFlowSubmissionController implements FlowSubmissionController {

  private static Logger LOG = LoggerFactory.getLogger(RMJMXFlowSubmissionController.class);
  private static final String JMX_QUERY_URL_BASE = "http://ds-jt01.liveramp.net/cached/jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,";

  private final long pendingContainerLimit;
  private final long runningAppLimit;
  private final TimeUnit sleepUnit;
  private final long containerSleepAmount;
  private final long appSleepAmount;

  private IOFunction<String, String> jsonRetriever;
  private Function<QueueParameters, SubmissionSemaphore> semaphoreFactory;
  private final TimeUnit maximumWaitUnit;
  private final long maximumWaitAmount;
  private final long maxWaitDurationMillis;

  private static final double LOG_FACTOR = 2;

  public static RMJMXFlowSubmissionController production() {
    return production(TimeUnit.HOURS, 12);
  }

  public static RMJMXFlowSubmissionController production(int appLimit, TimeUnit maxWaitUnit, long maxWaitAmount) {
    return new
        RMJMXFlowSubmissionController(
        7500,
        appLimit,
        TimeUnit.MINUTES,
        1,
        5,
        maxWaitUnit,
        maxWaitAmount,
        LRHttpUtils::GETRequest,
        ProductionCuratorSemaphore::new);
  }

  public static RMJMXFlowSubmissionController production(TimeUnit maxWaitUnit, long maxWaitAmount) {
    return new
        RMJMXFlowSubmissionController(
        7500,
        150,
        TimeUnit.MINUTES,
        1,
        5,
        maxWaitUnit,
        maxWaitAmount,
        LRHttpUtils::GETRequest,
        ProductionCuratorSemaphore::new);
  }

  RMJMXFlowSubmissionController(
      long pendingContainerLimit,
      long runningAppLimit,
      TimeUnit sleepUnit, long containerSleepAmount, long appSleepAmount,
      TimeUnit maximumWaitUnit, long maximumWaitAmount,
      IOFunction<String, String> jsonRetriever,
      Function<QueueParameters, SubmissionSemaphore> semaphoreFactory) {
    this.pendingContainerLimit = pendingContainerLimit;
    this.sleepUnit = sleepUnit;
    this.containerSleepAmount = containerSleepAmount;
    this.appSleepAmount = appSleepAmount;
    this.maximumWaitAmount = maximumWaitAmount;
    this.maximumWaitUnit = maximumWaitUnit;
    this.jsonRetriever = jsonRetriever;
    this.semaphoreFactory = semaphoreFactory;
    this.maxWaitDurationMillis = maximumWaitUnit.toMillis(maximumWaitAmount);
    this.runningAppLimit = runningAppLimit;
  }

  @Override
  public Runnable blockUntilSubmissionAllowed(Configuration flowConfig) {
    Runnable cleanupCallback = () -> {
    };
    try {
      String mapReduceQueue = flowConfig.get(MRJobConfig.QUEUE_NAME);
      SubmissionSemaphore semaphore = semaphoreFactory.apply(new QueueParameters(mapReduceQueue, runningAppLimit));
      cleanupCallback = () -> {
        LOG.info("Releasing submission semaphore lease");
        semaphore.releaseShare();
      };
      long waitStart = System.currentTimeMillis();
      long maxWaitTimestamp = waitStart + maxWaitDurationMillis;
      LOG.info("Acquiring submission semaphore...");
      semaphore.blockUntilShareIsAvailable(maxWaitDurationMillis);
      LOG.info("Obtained semaphore lease");
      LOG.info("Retrieving queue info");
      long startInfoGet = System.currentTimeMillis();
      QueueInfo queueInfo = getQueueInfo(mapReduceQueue);
      LOG.info("Took " + (System.currentTimeMillis() - startInfoGet) + " to retrieve queue info first time");
      while (isOverLimit(queueInfo) && System.currentTimeMillis() < maxWaitTimestamp) {
        long sleepMillis = Math.max(
            determineSleep(queueInfo.pendingContainers, pendingContainerLimit, containerSleepAmount, maxWaitTimestamp - System.currentTimeMillis(), LOG_FACTOR),
            determineSleep(queueInfo.runningApps, runningAppLimit, appSleepAmount, maxWaitTimestamp - System.currentTimeMillis(), LOG_FACTOR)
        );

        LOG.info("Queue info: ");
        LOG.info("\t" + queueInfo.pendingContainers + " pending containers in queue, configured limit: " + pendingContainerLimit);
        LOG.info("\t" + queueInfo.runningApps + " running apps in queue, configured limit: " + runningAppLimit);
        LOG.info("\t" + "Delaying job submission for " + sleepUnit.convert(sleepMillis, TimeUnit.MILLISECONDS) + " " + sleepUnit.name() + " ");

        TimeUnit.MILLISECONDS.sleep(sleepMillis);
        queueInfo = getQueueInfo(mapReduceQueue);
      }
      if ((System.currentTimeMillis() - waitStart) >= maxWaitDurationMillis) {
        LOG.warn("Waited for more than the max wait period of " + maximumWaitAmount + " " + maximumWaitUnit.name() + ". Launching job.");
      }
    } catch (Exception e) {
      LOG.error("Error while blocking for job submission. Allowing job to launch", e);
    }
    LOG.info("Allowing flow submission");
    return cleanupCallback;
  }

  private boolean isOverLimit(QueueInfo queueInfo) {
    return queueInfo.pendingContainers > pendingContainerLimit || queueInfo.runningApps > runningAppLimit;
  }

  long determineSleep(long metric, long limit, long sleepAmount, long maxSleepMillis) {
    return determineSleep(metric, limit, sleepAmount, maxSleepMillis, 2);
  }

  long determineSleep(long metric, long limit, long sleepAmount, long maxSleepMillis, double logFactor) {
    //If there's a large backlog, we should wait longer before checking again if it's clear.
    // Use a log base 2 to ensure we never wait for a really massive time
    double pendingRatio = metric / (double)limit;
    double logOfRatio = Math.log(pendingRatio) / Math.log(logFactor); //log base 2

    long computedSleepMilliseconds = (long)(sleepUnit.toMillis(sleepAmount) * Math.ceil(logOfRatio));

    //We also compute how many milliseconds until our max wait runs out, and use that if it's smaller
    return Math.min(computedSleepMilliseconds, Math.max(maxSleepMillis, 0));
  }

  public static class QueueParameters {
    private final String queueName;
    private final long maxApplications;

    public QueueParameters(String queueName, long maxApplications) {
      this.queueName = queueName;
      this.maxApplications = maxApplications;
    }

    public String getQueueName() {
      return queueName;
    }

    public long getMaxApplications() {
      return maxApplications;
    }
  }


  public static class QueueInfo {
    private final long pendingContainers;
    private final long runningApps;

    public QueueInfo(long pendingContainers, long runningApps) {
      this.pendingContainers = pendingContainers;
      this.runningApps = runningApps;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      QueueInfo queueInfo = (QueueInfo)o;

      if (pendingContainers != queueInfo.pendingContainers) {
        return false;
      }
      return runningApps == queueInfo.runningApps;
    }

    @Override
    public int hashCode() {
      int result = (int)(pendingContainers ^ (pendingContainers >>> 32));
      result = 31 * result + (int)(runningApps ^ (runningApps >>> 32));
      return result;
    }
  }

  private QueueInfo getQueueInfo(String mapReduceQueue) {
    try {
      String jmxUrl = JMX_QUERY_URL_BASE + createJMXURLSuffix(mapReduceQueue);
      String jsonString = jsonRetriever.apply(jmxUrl);
      return getInfoFromJson(mapReduceQueue, jsonString);
    } catch (Exception e) {
      LOG.error("Error while blocking for job submission - allowing job to launch", e);
      return new QueueInfo(0, 0);
    }
  }

  QueueInfo getInfoFromJson(String mapReduceQueue, String jsonString) throws JSONException {
    JSONTokener tokener = new JSONTokener(jsonString);
    JSONObject jsonObject = new JSONObject(tokener);
    JSONArray jsonArray = jsonObject.getJSONArray("beans");

    long pendingContainers = 0;
    long runningApps = 0;

    if (jsonArray.length() > 0) {
      JSONObject obj = jsonArray.getJSONObject(0);
      pendingContainers = obj.getLong("PendingContainers");
      runningApps = obj.getLong("AppsRunning");
    } else {
      LOG.error("Queue " + mapReduceQueue + " not found - defaulting to reporting 0 pending containers and apps and allowing job to launch");
    }

    return new QueueInfo(pendingContainers, runningApps);

  }

  static String createJMXURLSuffix(String queue) {
    String[] queueParts = queue.split("\\.");
    List<String> urlParts = Lists.newArrayList();
    for (int i = 0; i < queueParts.length; i++) {
      urlParts.add("q" + i + "=" + queueParts[i]);
    }
    return StringUtils.join(urlParts, ",");
  }

  public static class ProductionCuratorSemaphore implements SubmissionSemaphore {

    public static final String SEMAPHORE_ROOT = "/workflow/flowSubmissionController/queueSempahores/";
    private InterProcessSemaphoreV2 semaphore;
    private Lease lease;
    private final int maxApplications;

    public ProductionCuratorSemaphore(QueueParameters parameters) {
      String curatorPath = createPath(parameters.getQueueName());
      CuratorFramework production = CuratorFrameworkDefaults.production();
      ensurePathExists(curatorPath, production);
      maxApplications = (int)parameters.getMaxApplications();
      this.semaphore = new InterProcessSemaphoreV2(production, curatorPath, maxApplications);
    }

    private void ensurePathExists(String curatorPath, CuratorFramework production) {
      EnsurePath ensurePath = new EnsurePath(curatorPath);
      try {
        ensurePath.ensure(production.getZookeeperClient());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @NotNull
    private static String createPath(String queue) {
      return SEMAPHORE_ROOT + queue.replaceAll("[.]", "");
    }

    @Override
    public void blockUntilShareIsAvailable(long timeoutMillseconds) {
      try {
        //we can get a null result from this method, indicating the timeout was reached.
        // We actually just want to behave the same way in that case though, so we don't check the result here
        LOG.info("Estimating " + semaphore.getParticipantNodes() + "/" + maxApplications + " are being used");
        this.lease = semaphore.acquire(timeoutMillseconds, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        LOG.error("Error during semaphore acquisition - allowing flow to launch:", e);
      }
    }

    @Override
    public void releaseShare() {
      if (this.lease != null) {
        semaphore.returnLease(lease);
      }
    }
  }
}
