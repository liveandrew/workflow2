package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.RunnableWithCoordinator;
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;

public class IncrementalDomainVersionPropertiesDeltaGetter implements RunnableWithCoordinator {
  
  private final String domainName;
  private IncrementalDomainVersionProperties result;
  
  public IncrementalDomainVersionPropertiesDeltaGetter(String domainName) {
    this.domainName = domainName;
  }
  
  @Override
  public void run(Coordinator coordinator) throws IOException {
    Domain domain = DomainBuilderProperties.getDomain(coordinator, domainName);
    setResult(new IncrementalDomainVersionProperties.Delta(domain));
  }

  public IncrementalDomainVersionProperties getResult() {
    return result;
  }

  public void setResult(IncrementalDomainVersionProperties result) {
    this.result = result;
  }
}
