package com.rapleaf.cascading_ext.workflow2.action;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.RingGroups;
import com.rapleaf.cascading_ext.workflow2.Action;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class WaitForHankDeploy extends Action {

  private static final Logger LOG = Logger.getLogger(WaitForHankDeploy.class);
  private static final long FIVE_MINUTES = 300000l;

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
        Thread.sleep(FIVE_MINUTES);
      }
      LOG.info("Checking for deploy completeness...");
      Iterator<RingGroup> itr = ringsToWaitFor.iterator();
      while (itr.hasNext()) {
        RingGroup ringGroup = itr.next();

        if(ringGroup == null){
          itr.remove();
        } else if (RingGroups.isServingOnlyUpToDate(ringGroup)) {
          LOG.info("Noting domain as up-to-date: "+ringGroup.getName());
          itr.remove();
        }
      }

      firstIteration = false;

      LOG.info("Waiting for ring groups to finish deploying: "+ Collections2.transform(ringsToWaitFor, new Function<RingGroup, String>() {
        @Override
        public String apply( RingGroup ringGroup) {
          if(ringGroup != null){
            return ringGroup.getName();
          }
          return null;
        }
      }));

    }
    LOG.info("Checking for deploy complete!");
  }
}
