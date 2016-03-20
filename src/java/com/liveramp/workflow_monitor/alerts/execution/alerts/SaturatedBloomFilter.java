//package com.liveramp.workflow_monitor.alerts.execution.alerts;
//
//import com.google.common.collect.Multimap;
//
//import com.liveramp.commons.collections.nested_map.TwoNestedMap;
//import com.liveramp.workflow_monitor.alerts.execution.JobThresholdAlert;
//import com.liveramp.workflow_state.WorkflowRunnerNotification;
//
//public class SaturatedBloomFilter extends JobThresholdAlert {
//
//  protected SaturatedBloomFilter(double threshold, WorkflowRunnerNotification notification, Multimap<String, String> countersToFetch) {
//    super(threshold, notification, countersToFetch);
//  }
//
//  @Override
//  protected Double calculateStatistic(TwoNestedMap<String, String, Long> counters) {
//
//  }
//
//  @Override
//  protected String getMessage(double value) {
//    // TODO implement
//  }
//
//}