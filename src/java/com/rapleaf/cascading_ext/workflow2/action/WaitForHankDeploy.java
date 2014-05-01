package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.RingGroups;
import com.rapleaf.cascading_ext.workflow2.Action;

public class WaitForHankDeploy extends Action {

  private static final Logger LOG = Logger.getLogger(WaitForHankDeploy.class);
  private static final long FIVE_MINUTES = 300000l;

  private final Coordinator coordinator;
  private final Map<String, Integer> domainToVersion;

  public WaitForHankDeploy(String checkpointToken, Coordinator coordinator, Map<String, Integer> domainToVersion) {
    super(checkpointToken);

    this.coordinator = coordinator;
    this.domainToVersion = domainToVersion;
  }


  @Override
  protected void execute() throws Exception {
    Map<RingGroup, List<DomainAndVersion>> ringsToWaitFor = Maps.newHashMap();
    for (RingGroup ringGroup : coordinator.getRingGroups()) {
      Set<Domain> domainGroup = ringGroup.getDomainGroup().getDomains();
      List<DomainAndVersion> versions = getDomainsAndVersions(domainGroup);
      if (!versions.isEmpty()) {
        ringsToWaitFor.put(ringGroup, versions);
      }
    }

    boolean firstIteration = true;

    while (!ringsToWaitFor.isEmpty()) {
      if (!firstIteration) {
        LOG.info("Some RingGroups are still updating, sleeping for 5 minutes");
        Thread.sleep(FIVE_MINUTES);
      }
      LOG.info("Checking for deploy completeness...");
      Iterator<Map.Entry<RingGroup, List<DomainAndVersion>>> itr = ringsToWaitFor.entrySet().iterator();
      while (itr.hasNext()) {
        Map.Entry<RingGroup, List<DomainAndVersion>> entry = itr.next();
        RingGroup ringGroup = entry.getKey();
        List<DomainAndVersion> versions = entry.getValue();
        if (ringGroup == null) {
          itr.remove();
        } else {
          if (RingGroups.isServingOnlyUpToDateOrMoreRecent(ringGroup, versions)) {
            LOG.info("Noting domain as up-to-date: " + ringGroup.getName());
            itr.remove();
          }
        }
      }

      firstIteration = false;

      LOG.info("Waiting for ring groups to finish deploying: " + Collections2.transform(ringsToWaitFor.keySet(), new Function<RingGroup, String>() {
        @Override
        public String apply(RingGroup ringGroup) {
          if (ringGroup != null) {
            return ringGroup.getName();
          }
          return null;
        }
      }));

    }

    LOG.info("Checking for deploy complete!");
  }

  private List<DomainAndVersion> getDomainsAndVersions(Set<Domain> domainGroup) {
    List<DomainAndVersion> versions = Lists.newArrayList();
    for (Map.Entry<String, Integer> entry : domainToVersion.entrySet()) {
      Domain domain = coordinator.getDomain(entry.getKey());
      if (domainGroup.contains(domain)) {
        versions.add(new DomainAndVersion(domain, entry.getValue()));
      }
    }
    return versions;
  }
}
