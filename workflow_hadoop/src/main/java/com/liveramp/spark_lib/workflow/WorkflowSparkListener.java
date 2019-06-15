package com.liveramp.spark_lib.workflow;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import com.liveramp.cascading_ext.flow.JobPersister;
import com.liveramp.cascading_ext.yarn.YarnApiHelper;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.state.LaunchedJob;

import static com.liveramp.cascading_ext.yarn.YarnApiHelper.ApplicationInfo;

public class WorkflowSparkListener extends SparkListener {

  public static final String MR_STATS_GROUP = "org.apache.hadoop.mapreduce.JobCounter";
  public static final String MR_MEM_MILLIS_COUNTER = "MB_MILLIS_MAPS";
  public static final String MR_VCORE_MILLIS_COUNTER = "VCORES_MILLIS_MAPS";
  private static final int MAX_APP_INFO_RETRIES = 4;
  private static final Integer YARN_RETRY_SLEEP = 3;
  private JobPersister persister = null;
  private String appId = null;
  private LaunchedJob launchedJob = null;

  private static Logger LOG = LoggerFactory.getLogger(WorkflowSparkListener.class);
  private String yarnApiAddress;

  public WorkflowSparkListener(SparkConf conf) {
    yarnApiAddress = SparkHadoopUtil.get().newConfiguration(conf).get("yarn.resourcemanager.webapp.address");
  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    Option<String> appIdOpt = applicationStart.appId();
    if (appIdOpt.isDefined()) {
      appId = appIdOpt.get();
      Optional<ApplicationInfo> yarnAppInfo = getYarnAppInfoWithRetries(appId);
      this.launchedJob = new LaunchedJob(
          appId,
          applicationStart.appName(),
          yarnAppInfo.isPresent() ? yarnAppInfo.get().getTrackingUrl() : "");
      tryStartWorkflowTracking();
    } else {
      LOG.error("App ID not defined! Can't track this spark job");
    }
  }

  public void onApplicationEnd() {
    if (appId != null) {
      TwoNestedMap<String, String, Long> counters = new TwoNestedMap<>();
      Optional<ApplicationInfo> yarnAppInfo = getYarnAppInfoWithRetries(appId);
      if (yarnAppInfo.isPresent()) {
        counters.put(MR_STATS_GROUP, MR_MEM_MILLIS_COUNTER, TimeUnit.SECONDS.toMillis(yarnAppInfo.get().getMbSeconds()));
        counters.put(MR_STATS_GROUP, MR_VCORE_MILLIS_COUNTER, TimeUnit.SECONDS.toMillis(yarnAppInfo.get().getVcoreSeconds()));
        counters.put(YarnApiHelper.YARN_STATS_GROUP, YarnApiHelper.YARN_VCORE_SECONDS_COUNTER, yarnAppInfo.get().getVcoreSeconds());
        counters.put(YarnApiHelper.YARN_STATS_GROUP, YarnApiHelper.YARN_MEM_SECONDS_COUNTER, yarnAppInfo.get().getMbSeconds());
        try {
          persister.onCounters(appId, counters);
        } catch (IOException e) {
          LOG.error("Error in application end", e);
          throw new RuntimeException(e);
        }
      }
    } else {
      LOG.error("App ID not defined! Can't track this spark job");
    }
  }

  private Optional<ApplicationInfo> getYarnAppInfoWithRetries(String appId) {
    if (yarnApiAddress != null) {
      Optional<ApplicationInfo> result = YarnApiHelper.getYarnAppInfo(yarnApiAddress, appId);
      int retries = 0;
      while (!result.isPresent() && retries < MAX_APP_INFO_RETRIES) {
        LOG.info("Unable to get application info from YARN. Sleeping for " + YARN_RETRY_SLEEP + " seconds and retrying");
        retries++;
        try {
          TimeUnit.SECONDS.sleep(YARN_RETRY_SLEEP);
        } catch (InterruptedException e) {
          //ignore and try again
        }
        result = YarnApiHelper.getYarnAppInfo(yarnApiAddress, appId);
      }
      return result;
    } else {
      LOG.error("YARN api address not set");
      return Optional.empty();
    }
  }

  public void setPersister(JobPersister persister) {
    this.persister = persister;
    tryStartWorkflowTracking();
  }

  //This entire construction is because of the way SparkContext starts
  //It immediately, synchronously starts your app, so you can only set an
  //app start listener by class name, so we record the info we want to submit
  //and then wait for the submission object later
  private synchronized void tryStartWorkflowTracking() {
    if (persister != null && launchedJob != null) {
      try {
        persister.onRunning(launchedJob);
        launchedJob = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
