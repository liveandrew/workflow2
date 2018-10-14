package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.liveramp.workflow.state.WorkflowDbPersistenceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.yaml.YamlClientConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainGroups;
import com.liveramp.hank.coordinator.Domains;
import com.rapleaf.cascading_ext.datastore.HankDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;

public class DeployDomains extends Action {

  private final Logger LOG = LoggerFactory.getLogger(DeployDomains.class);
  private final Coordinator coordinator;
  private final List<String> domainNames;

  public DeployDomains(String checkpointToken, Coordinator coordinator, String... domainNames) {
    this(checkpointToken, coordinator, Lists.newArrayList(domainNames));
  }

  public DeployDomains(String checkpointToken, Coordinator coordinator, Iterable<String> domainNames) {
    super(checkpointToken);
    this.coordinator = coordinator;
    this.domainNames = Lists.newArrayList(domainNames);
  }

  public DeployDomains(String checkpointToken, Coordinator coordinator, HankDataStore... domainStores) {
    this(checkpointToken, coordinator, Lists.newArrayList(domainStores));
  }

  public DeployDomains(String checkpointToken, Coordinator coordinator, Collection<HankDataStore> domainStores) {
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
      for (Domain domain : entry.getValue()) {
        LOG.info("Fast Forwarding Domain :" + domain.getName() + " to version " + Domains.getLatestVersionNotOpenNotDefunct(domain).getVersionNumber());
      }
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
