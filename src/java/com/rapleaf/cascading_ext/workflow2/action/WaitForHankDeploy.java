package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.RingGroups;
import com.rapleaf.cascading_ext.workflow2.Action;

public class WaitForHankDeploy extends Action {

  private static final Logger LOG = LoggerFactory.getLogger(WaitForHankDeploy.class);
  public static final int MINUTES_BETWEEN_CHECKS = 5;

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

      // Do not wait on liveramp-production ring group, it takes too long to deploy
      // TODO: https://jira.liveramp.net/browse/OM-156
      if (ringGroup.getName().equals("liveramp-production")) {
        continue;
      }

      Set<Domain> domainGroup = ringGroup.getDomainGroup().getDomains();
      if (domainGroup == null) {
        LOG.info("No domains found for group: " + ringGroup.getDomainGroup().getName());
        continue;
      }

      List<DomainAndVersion> versions = getDomainsAndVersions(domainGroup);
      if (!versions.isEmpty()) {
        ringsToWaitFor.put(ringGroup, versions);
      }
    }

    boolean firstIteration = true;

    while (!ringsToWaitFor.isEmpty()) {
      if (!firstIteration) {
        LOG.info("Some RingGroups are still updating, sleeping for " + MINUTES_BETWEEN_CHECKS + " minutes");
        TimeUnit.MINUTES.sleep(MINUTES_BETWEEN_CHECKS);
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
            LOG.info("Noting ring group as up-to-date: " + ringGroup.getName());
            itr.remove();
          } else {
            LOG.info("Awaiting update completion on hosts: " + RingGroups.getHostsNotUpToDate(ringGroup, versions));
          }
        }
      }

      firstIteration = false;

      LOG.info("Waiting for ring groups to finish deploying: " + Collections2.transform(ringsToWaitFor.entrySet(), new Function<Map.Entry<RingGroup, List<DomainAndVersion>>, String>() {
        @Override
        public String apply(Map.Entry<RingGroup, List<DomainAndVersion>> entry) {
          return (entry.getKey() != null) ? entry.getKey().getName() + ": " + entry.getValue() : null;
        }
      }));

    }

    LOG.info("Checking for deploy complete!");
  }

  private List<DomainAndVersion> getDomainsAndVersions(Set<Domain> domainGroup) {
    List<DomainAndVersion> versions = Lists.newArrayList();
    for (Map.Entry<String, Integer> entry : domainToVersion.entrySet()) {
      Domain domain = coordinator.getDomain(entry.getKey());
      if (domain != null) {  // TODO get rid of this chekc, just adding this because I removed mid_to_pam and there are still messages with it
        if (domainGroup.contains(domain)) {
          versions.add(new DomainAndVersion(domain, entry.getValue()));
        }
      }
    }
    return versions;
  }
}
