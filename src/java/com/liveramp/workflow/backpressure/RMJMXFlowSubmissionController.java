package com.liveramp.workflow.backpressure;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.functional.IOFunction;
import com.liveramp.java_support.web.LRHttpUtils;

public class RMJMXFlowSubmissionController implements FlowSubmissionController {

  private static Logger LOG = LoggerFactory.getLogger(RMJMXFlowSubmissionController.class);
  private static final String JMX_QUERY_URL_BASE = "http://ds-jt01.liveramp.net:8088/jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,";

  private final long pendingContainerLimit;
  private final TimeUnit sleepUnit;
  private final long sleepAmount;
  private IOFunction<String, String> jsonRetriever;

  public static RMJMXFlowSubmissionController production() {
    return new RMJMXFlowSubmissionController(5000, TimeUnit.MINUTES, 1, LRHttpUtils::GETRequest);
  }


  RMJMXFlowSubmissionController(long pendingContainerLimit, TimeUnit sleepUnit, long sleepAmount, IOFunction<String, String> jsonRetriever) {
    this.pendingContainerLimit = pendingContainerLimit;
    this.sleepUnit = sleepUnit;
    this.sleepAmount = sleepAmount;
    this.jsonRetriever = jsonRetriever;
  }

  @Override
  public void blockUntilSubmissionAllowed(Configuration flowConfig) {
    try {
      String mapReduceQueue = flowConfig.get(MRJobConfig.QUEUE_NAME);
      long pendingContainersForQueue = getPendingContainersForQueue(mapReduceQueue);
      while (pendingContainersForQueue > pendingContainerLimit) {
        long sleepMillis = determineSleepMilliseconds(pendingContainersForQueue);
        LOG.info("There are " + pendingContainersForQueue + " pending containers in the queue compared to the limit of " + pendingContainerLimit
            + ". Delaying job submission for " + sleepUnit.convert(sleepMillis, TimeUnit.MILLISECONDS) + " " + sleepUnit.name());
        TimeUnit.MILLISECONDS.sleep(sleepMillis);
        pendingContainersForQueue = getPendingContainersForQueue(mapReduceQueue);
      }
    } catch (InterruptedException e) {
      LOG.error("Error while blocking for job submission. Allowing job to launch", e);
    }
  }

  long determineSleepMilliseconds(long pendingContainersForQueue) {
    //If there's a large backlog, we should wait longer before checking again if it's clear.
    // Use a log base 2 to ensure we never wait for a really massive time
    double pendingRatio = pendingContainersForQueue / (double)pendingContainerLimit;
    double logOfRatio = Math.log(pendingRatio) / Math.log(2); //log base 2
    double sleepMultiplier = Math.ceil(logOfRatio);
    return (long)(sleepUnit.toMillis(sleepAmount) * sleepMultiplier);
  }

  private long getPendingContainersForQueue(String mapReduceQueue) {
    try {
      String jmxUrl = JMX_QUERY_URL_BASE + createJMXURLSuffix(mapReduceQueue);
      String jsonString = jsonRetriever.apply(jmxUrl);
      return getContainersFromJson(mapReduceQueue, jsonString);
    } catch (Exception e) {
      LOG.error("Error while blocking for job submission - allowing job to launch", e);
      return 0;
    }
  }

  long getContainersFromJson(String mapReduceQueue, String jsonString) throws JSONException {
    JSONTokener tokener = new JSONTokener(jsonString);
    JSONObject jsonObject = new JSONObject(tokener);
    JSONArray jsonArray = jsonObject.getJSONArray("beans");

    if (jsonArray.length() > 0) {
      JSONObject obj = jsonArray.getJSONObject(0);
      return obj.getLong("PendingContainers");
    } else {
      LOG.error("Queue " + mapReduceQueue + " not found - defaulting to reporting 0 pending containers and allowing job to launch");
      return 0;
    }
  }

  static String createJMXURLSuffix(String queue) {
    String[] queueParts = queue.split("\\.");
    List<String> urlParts = Lists.newArrayList();
    for (int i = 0; i < queueParts.length; i++) {
      urlParts.add("q" + i + "=" + queueParts[i]);
    }
    return StringUtils.join(urlParts, ",");
  }
}
