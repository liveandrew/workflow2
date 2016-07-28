package com.liveramp.workflow_core.runner;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.collections.properties.OverridableProperties;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.TrackerURLBuilder;

public class BaseWorkflowRunner {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowRunner.class);

  public static final String JOB_PRIORITY_PARAM = "mapred.job.priority";
  public static final String JOB_POOL_PARAM = "mapreduce.job.queuename";

  private final WorkflowStatePersistence persistence;

  private final ExecuteConfig context;

  private final int stepPollInterval;

  //  set this if something fails in a step (outside user-code) so we don't keep trying to start steps
  private List<Exception> internalErrors = new CopyOnWriteArrayList<Exception>();

  private OverridableProperties workflowJobProperties;

  /**
   * how many components will we allow to execute simultaneously?
   */
  private final int maxConcurrentSteps;

  private final DirectedGraph<Step, DefaultEdge> dependencyGraph;

  /**
   * semaphore used to control the max number of running components
   */
  private final Semaphore semaphore;

  /**
   * components that haven't yet been started
   */
  private final Set<StepRunner> pendingSteps = new HashSet<StepRunner>();
  /**
   * components that have been started and not yet finished
   */
  private final Set<StepRunner> runningSteps = new HashSet<StepRunner>();
  /**
   * started and completed successfully
   */
  private final Set<StepRunner> completedSteps = new HashSet<StepRunner>();

  private final MultiShutdownHook shutdownHook;

  private boolean alreadyRun;
  private final TrackerURLBuilder trackerURLBuilder;



}

