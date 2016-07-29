package com.rapleaf.cascading_ext.workflow2.options;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.mapred.JobConf;

import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.cascading_tools.properties.PropertiesUtil;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.liveramp.workflow_core.ContextStorage;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilter;

public class WorkflowOptions extends BaseWorkflowOptions<WorkflowOptions> {

  private StoreReaderLockProvider lockProvider;
  private CounterFilter counterFilter;

  protected WorkflowOptions() {
    super(CascadingHelper.get().getDefaultHadoopProperties(),  toProperties(CascadingHelper.get().getJobConf()));
  }

  private static Map<Object, Object> toProperties(JobConf conf){
    Map<Object, Object> props = Maps.newHashMap();
    for (Map.Entry<String, String> entry : conf) {
      props.put(entry.getKey(), entry.getValue());
    }
    return props;
  }

  public WorkflowOptions configureTeam(TeamList team, String subPool) {
    addWorkflowProperties(PropertiesUtil.teamPool(team, subPool));
    setAlertsHandler(team);
    return this;
  }

  public CounterFilter getCounterFilter() {
    return counterFilter;
  }

  @Deprecated
  //  all counters are tracked by default now, so this will actually disable tracking most counters.  if you really want this, let me know  -- ben
  public WorkflowOptions setCounterFilter(CounterFilter filter) {
    this.counterFilter = filter;
    return this;
  }

  public StoreReaderLockProvider getLockProvider() {
    return lockProvider;
  }

  public WorkflowOptions setLockProvider(StoreReaderLockProvider lockProvider) {
    this.lockProvider = lockProvider;
    return this;
  }

}
