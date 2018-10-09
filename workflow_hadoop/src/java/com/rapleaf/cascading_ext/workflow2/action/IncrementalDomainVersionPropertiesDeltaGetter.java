package com.rapleaf.cascading_ext.workflow2.action;

import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.RunnableWithCoordinator;
import com.liveramp.hank.hadoop.DomainBuilderProperties;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;

import java.io.IOException;

public class IncrementalDomainVersionPropertiesDeltaGetter implements RunnableWithCoordinator {

  private final String domainName;
  private IncrementalDomainVersionProperties result;

  public IncrementalDomainVersionPropertiesDeltaGetter(String domainName) {
    this.domainName = domainName;
  }

  @Override
  public void run(Coordinator coordinator) throws IOException {
    Domain domain = DomainBuilderProperties.getDomain(coordinator, domainName);
    result = new IncrementalDomainVersionProperties.Delta(domain);
  }

  public IncrementalDomainVersionProperties getResult() {
    return result;
  }
}
