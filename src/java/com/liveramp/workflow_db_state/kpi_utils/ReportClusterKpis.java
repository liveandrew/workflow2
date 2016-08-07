package com.liveramp.workflow_db_state.kpi_utils;

import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.datadog_client.statsd.DogClient;
import com.timgroup.statsd.StatsDClient;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by lerickson on 8/3/16.
 */
public class ReportClusterKpis {

  private final static long INTERVAL_MILLIS = TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES);
  private final static String METRIC_NAME_STEM = "cluster.kpi";

  public static void main(String[] args) throws IOException {
    long currentTimeMillis = System.currentTimeMillis();
    IDatabases db = new DatabasesImpl();
    InfrastructureFailureRates.InfrastructureFailureInfo task = InfrastructureFailureRates.getTaskFailureInfo(currentTimeMillis-INTERVAL_MILLIS,currentTimeMillis,db);
    InfrastructureFailureRates.InfrastructureFailureInfo app = InfrastructureFailureRates.getAppFailureInfo(currentTimeMillis-INTERVAL_MILLIS,currentTimeMillis,db);
    StatsDClient client = DogClient.getProduction();
    client.gauge(METRIC_NAME_STEM+".tasks.infrastructure_failures",task.getNumInfrastructureFailures());
    client.gauge(METRIC_NAME_STEM+".tasks.total",task.getSampleSize());
    client.gauge(METRIC_NAME_STEM+".apps.infrastructure_failures",app.getNumInfrastructureFailures());
    client.gauge(METRIC_NAME_STEM+".apps.total",app.getSampleSize());
  }
}
