package com.rapleaf.cascading_ext.workflow2.action;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.RingGroups;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class WaitForHankDeploy extends Action {

  private static final Logger LOG = Logger.getLogger(WaitForHankDeploy.class);

  private final Coordinator coordinator;
  private final List<String> ringGroupNames;

  public WaitForHankDeploy(String checkpointToken, Coordinator coordinator, String... ringGroupNames) {
    this(checkpointToken, coordinator, Lists.newArrayList(ringGroupNames));
  }

  public WaitForHankDeploy(String checkpointToken, Coordinator coordinator, Iterable<String> ringGroupNames) {
    super(checkpointToken);
    this.coordinator = coordinator;
    this.ringGroupNames = Lists.newArrayList(ringGroupNames);
  }

  @Override
  protected void execute() throws Exception {
    Set<RingGroup> ringsToWaitFor = Sets.newHashSet();

    for (String ringGroupName : ringGroupNames) {
      ringsToWaitFor.add(coordinator.getRingGroup(ringGroupName));
    }

    boolean firstIteration = true;

    while (!ringsToWaitFor.isEmpty()) {
      if (!firstIteration) {
        LOG.info("Some RingGroups are still updating, sleeping for 5 minutes");
        TimeUnit.MINUTES.sleep(5);
        firstIteration = false;
      }
      LOG.info("Checking for deploy completeness...");
      Iterator<RingGroup> itr = ringsToWaitFor.iterator();
      while (itr.hasNext()) {
        RingGroup ringGroup = itr.next();
        if (RingGroups.isServingOnlyUpToDate(ringGroup)) {
          itr.remove();
        }
      }
    }
    LOG.info("Checking for deploy complete!");
  }
}
