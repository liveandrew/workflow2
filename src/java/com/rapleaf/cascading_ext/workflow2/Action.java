package com.rapleaf.support.workflow2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopStepStats;
import cascading.stats.FlowStats;
import cascading.stats.StepStats;

import com.rapleaf.support.NestedTimer;
import com.rapleaf.support.NestedTimer.Token;
import com.rapleaf.support.datastore.DataStore;

public abstract class Action {
  private static final Logger LOG = Logger.getLogger(Action.class);
  
  private final String checkpointToken;
  private final String tmpRoot;
  
  private final Set<DataStore> readsFromDatastores = new HashSet<DataStore>();
  private final Set<DataStore> createsDatastores = new HashSet<DataStore>();
  private final Set<DataStore> createsTemporaryDatastores = new HashSet<DataStore>();
  private final Set<DataStore> writesToDatastores = new HashSet<DataStore>();
  
  private String lastStatusMessage = "";
  
  private int pctComplete;
  
  private long startTimestamp;
  private long endTimestamp;
  
  private List<Flow> flows = new ArrayList<Flow>();
  
  private FileSystem fs;
  
  public Action(String checkpointToken) {
    this.checkpointToken = checkpointToken;
    this.tmpRoot = null;
  }
  
  public Action(String checkpointToken, String tmpRoot) {
    this.checkpointToken = checkpointToken;
    this.tmpRoot = tmpRoot + "/" + checkpointToken + "-tmp-stores";
  }
  
  protected FileSystem getFS() throws IOException{
    if(fs == null){
      fs = FileSystem.get(new Configuration());
    }
    
    return fs;
  }
  
  public void runningFlow(Flow flow) {
    flows.add(flow);
  }
  
  protected void readsFrom(DataStore store) {
    readsFromDatastores.add(store);
  }
  
  protected void creates(DataStore store) {
    createsDatastores.add(store);
  }
  
  protected void createsTemporary(DataStore store) {
    createsTemporaryDatastores.add(store);
  }
  
  protected void writesTo(DataStore store) {
    writesToDatastores.add(store);
  }
  
  protected abstract void execute() throws Exception;
  
  protected final void internalExecute() {
    Token tok = NestedTimer.split(this.getClass().getSimpleName());
    this.getClass().toString();
    try {
      startTimestamp = System.currentTimeMillis();
      prepDirs();
      execute();
      tok.stop();
    } catch (Throwable e) {
      LOG.fatal("Action " + checkpointToken + " failed due to exception!", e);
      throw new RuntimeException(e);
    } finally {
      endTimestamp = System.currentTimeMillis();
      tok.finishPending();
    }
  }
  
  private void prepDirs() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    for (Set<DataStore> datastores : Arrays.asList(createsDatastores, createsTemporaryDatastores)) {
      for (DataStore datastore : datastores) {
        LOG.info("Deleting directory " + datastore.getPath());
        fs.delete(new Path(datastore.getPath()), true);
      }
    }
  }
  
  public String getCheckpointToken() {
    return checkpointToken;
  }
  
  public String getTmpRoot() {
    return tmpRoot;
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + " [checkpointToken=" + checkpointToken + "]";
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((checkpointToken == null) ? 0 : checkpointToken.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Action other = (Action) obj;
    if (checkpointToken == null) {
      if (other.checkpointToken != null)
        return false;
    } else if (!checkpointToken.equals(other.checkpointToken))
      return false;
    return true;
  }
  
  public Set<DataStore> getReadsFromDatastores() {
    return readsFromDatastores;
  }
  
  public Set<DataStore> getCreatesDatastores() {
    return createsDatastores;
  }
  
  public Set<DataStore> getWritesToDatastores() {
    return writesToDatastores;
  }
  
  /**
   * Set an application-specific status message to display. This will be
   * visible in the logs as well as through the UI. This is a great place to
   * report progress or choices.
   * 
   * @param statusMessage
   */
  protected void setStatusMessage(String statusMessage) {
    LOG.info("Status Message: " + statusMessage);
    lastStatusMessage = statusMessage;
  }
  
  public String getStatusMessage() {
    return lastStatusMessage;
  }
  
  protected void setPercentComplete(int pctComplete) {
    this.pctComplete = (int) Math.min(Math.max(0, pctComplete), 100);
  }
  
  public int getPercentComplete() {
    return this.pctComplete;
  }
  
  private final static String DEFAULT_JOB_TRACKER = "ds-jt";
  private String JOB_TRACKER = null;
  
  public List<String> getJobTrackerLinks() {
    
    List<String> links = new LinkedList<String>();
    List<String> ids = new ArrayList<String>();
    List<String> names = new ArrayList<String>();
    
    int flowCount = 1;
    for (Flow flow : flows) {
      
      if (JOB_TRACKER == null) {
        JOB_TRACKER = flow.getProperty("mapred.job.tracker");
        if (JOB_TRACKER != null && JOB_TRACKER != "" && JOB_TRACKER.split(":").length > 0) {
          String[] parts = JOB_TRACKER.split(":");
          JOB_TRACKER = "http://" + parts[0];
        } else {
          JOB_TRACKER = "http://" + DEFAULT_JOB_TRACKER;
        }
      }
      
      int count = 1;
      for (StepStats st : flow.getFlowStats().getStepStats()) {
        HadoopStepStats hdStepStats = (HadoopStepStats) st;
        
        try {
          String stepId = hdStepStats.getJobID();
          
          String name = "Flow " + new Integer(flowCount).toString() + " (" + count + "/" + flow.getFlowStats().getStepStats().size() + ")";
          
          ids.add(stepId);
          names.add(name);
          
        } catch (NullPointerException e) {
          // getJobID on occasion throws a null pointer exception, ignore it
        }
        
        count++ ;
      }
      flowCount++ ;
    }
    
    for (int i = 0; i < ids.size(); i++ ) {
      links.add("<a href=\"" + JOB_TRACKER + "/jobdetails.jsp?jobid=" + ids.get(i) + "&refresh=30\">[" + names.get(i) + "]</a><br>");
    }
    
    return links;
  }
  
  /**
   * Given a running Flow, compute what percent complete it is. The percent of
   * completion is defined as the number of FlowSteps that have completed
   * divided by the total number of FlowSteps times the maxPct value. This
   * normalizes the percent complete to the max possible percent of the total
   * component's work represented by this one Flow.
   * 
   * @param flow
   * @param maxPct
   * @return
   */
  public static int getFlowProgress(Flow flow, int maxPct) {
    FlowStats flowstats = flow.getFlowStats();
    int numComplete = 0;
    List<StepStats> stepStatsList = flowstats.getStepStats();
    for (StepStats stepStats : stepStatsList) {
      if (stepStats.isFinished()) {
        numComplete++ ;
      }
    }
    
    return (int) ((double) numComplete / flowstats.getStepsCount() * maxPct);
  }
  
  private class FlowProgressMonitor extends Thread {
    private boolean _keepRunning;
    private final Flow _flow;
    private final int _startPct;
    private final int _maxPct;
    
    public FlowProgressMonitor(Flow flow, int startPct, int maxPct) {
      super("FlowProgressMonitor for " + flow.getName());
      setDaemon(true);
      _keepRunning = true;
      _flow = flow;
      _startPct = startPct;
      _maxPct = maxPct;
    }
    
    @Override
    public void run() {
      while (_keepRunning) {
        try {
          setPercentComplete(_startPct + getFlowProgress(_flow, _maxPct - _startPct));
          sleep(15000);
        } catch (InterruptedException e) {
          // just want to stop sleeping and pay attention
        }
      }
    }
    
    public void stopAndInterrupt() {
      _keepRunning = false;
      interrupt();
    }
  }
  
  /**
   * Complete the provided Flow while monitoring and reporting its progress.
   * This method will call setPercentComplete with values between 0 and 100
   * incrementally based on the completion of the Flow's steps.
   * 
   * @param flow
   */
  protected void completeWithProgress(Flow flow) {
    completeWithProgress(flow, 0, 100);
  }
  
  /**
   * Complete the provided Flow while monitoring and reporting its progress.
   * This method will call setPercentComplete with values between startPct and
   * maxPct incrementally based on the completion of the Flow's steps.
   * 
   * @param flow
   * @param startPct
   * @param maxPct
   */
  protected void completeWithProgress(Flow flow, int startPct, int maxPct) {
    runningFlow(flow);
    
    flow.start();
    
    FlowProgressMonitor fpm = new FlowProgressMonitor(flow, startPct, maxPct);
    fpm.start();
    
    flow.complete();
    fpm.stopAndInterrupt();
  }
  
  public long getStartTimestamp() {
    return startTimestamp;
  }
  
  public long getEndTimestamp() {
    return endTimestamp;
  }
  
  public Set<DataStore> getCreatesTemporaryDatastores() {
    return createsTemporaryDatastores;
  }
}
