package com.rapleaf.cascading_ext.workflow2.action;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.rapleaf.cascading_ext.datastore.HankDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroups;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeployDomains extends Action {

  private final Coordinator coordinator;
  private final List<String> domainNames;

  public DeployDomains(String checkpointToken, Coordinator coordinator, String... domainNames) {
    super(checkpointToken);
    this.coordinator = coordinator;
    this.domainNames = Lists.newArrayList(domainNames);
  }

  public DeployDomains(String checkpointToken, Coordinator coordinator, HankDataStore... domainStores) {
    super(checkpointToken);
    this.coordinator = coordinator;
    domainNames = Lists.newArrayList();
    for (HankDataStore store : domainStores) {
      domainNames.add(store.getDomainName());
    }
  }

  @Override
  protected void execute() throws Exception {
    HashMultimap<DomainGroup, Domain> updateMap = HashMultimap.create();

    for (String domainName : domainNames) {
      Domain domain = coordinator.getDomain(domainName);
      Set<DomainGroup> domainGroups = getDomainGroups(domain);
      for (DomainGroup domainGroup : domainGroups) {
        updateMap.put(domainGroup, domain);
      }
    }

    for (Map.Entry<DomainGroup, Collection<Domain>> entry : updateMap.asMap().entrySet()) {
      DomainGroups.fastForwardDomains(entry.getKey(), entry.getValue());
    }

  }

  private Set<DomainGroup> getDomainGroups(Domain domain) throws IOException {
    Set<DomainGroup> domainGroups = coordinator.getDomainGroups();
    Set<DomainGroup> relevantGroups = Sets.newHashSet();

    if (domainGroups != null) {
      for (DomainGroup group : domainGroups) {
        if (group.getDomains().contains(domain)) {
          relevantGroups.add(group);
        }
      }
    }
    return relevantGroups;
  }
}
