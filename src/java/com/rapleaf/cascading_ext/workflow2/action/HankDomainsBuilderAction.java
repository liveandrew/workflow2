package com.rapleaf.cascading_ext.workflow2.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.HankDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.hank.cascading.CascadingDomainBuilder;
import com.rapleaf.hank.config.CoordinatorConfigurator;
import com.rapleaf.hank.coordinator.RunWithCoordinator;
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;

public abstract class HankDomainsBuilderAction extends Action {
  
  protected final Map<Object, Object> properties;
  
  private final HankDataStore[] outputs;
  private HankVersionType versionType;
  private final CoordinatorConfigurator configurator;
  private Integer partitionToBuild = null;
  
  public HankDomainsBuilderAction(
      String checkpointToken,
      HankVersionType versionType,
      CoordinatorConfigurator configurator,
      HankDataStore... outputs) {
    this(checkpointToken, null, versionType, configurator, outputs);
  }
  
  public HankDomainsBuilderAction(String checkpointToken,
      String tmpRoot,
      HankVersionType versionType,
      CoordinatorConfigurator configurator,
      HankDataStore... outputs) {
    super(checkpointToken, tmpRoot);
    this.versionType = versionType;
    this.configurator = configurator;
    this.outputs = outputs;
    this.properties = new HashMap<Object, Object>();
  }
  
  public void execute() throws Exception {
    prepare();
    ArrayList<CascadingDomainBuilder> cdbs = new ArrayList<CascadingDomainBuilder>();
    
    for (HankDataStore output : outputs) {
      cdbs.add(makeDomainBuilder(output));
    }
    
    Flow flow = CascadingDomainBuilder.buildDomains(properties, getSources(), getSinks(), getTails(), cdbs.toArray(new CascadingDomainBuilder[cdbs.size()]));
    
    if (flow != null) {
      postProcessFlow(flow);
    }
  }
  
  protected void setVersionType(HankVersionType versionType) {
    this.versionType = versionType;
  }
  
  protected HankVersionType getVersionType() {
    return versionType;
  }
  
  protected void setPartitionToBuild(int partitionToBuild) {
    this.partitionToBuild = partitionToBuild;
  }
  
  protected Integer getPartitionToBuild() {
    return partitionToBuild;
  }
  
  protected CascadingDomainBuilder makeDomainBuilder(HankDataStore output) throws Exception {
    if (getVersionType() == null) {
      throw new IllegalStateException("Must set a version type before executing the domain builder!");
    }
    
    final DomainBuilderProperties domainBuilderProperties = new DomainBuilderProperties(
        output.getDomainName(), configurator, output.getPath());
    
    final IncrementalDomainVersionProperties domainVersionProperties;
    switch (versionType) {
      case BASE:
        domainVersionProperties = new IncrementalDomainVersionProperties.Base();
        break;
      case DELTA:
        IncrementalDomainVersionPropertiesDeltaGetter deltaGetter =
            new IncrementalDomainVersionPropertiesDeltaGetter(domainBuilderProperties.getDomainName());
        RunWithCoordinator.run(domainBuilderProperties.getConfigurator(), deltaGetter);
        domainVersionProperties = deltaGetter.getResult();
        break;
      default:
        throw new RuntimeException("Unknown version type: " + versionType);
    }
    
    CascadingDomainBuilder builder = new CascadingDomainBuilder(domainBuilderProperties,
        domainVersionProperties, getPipe(), getKeyFieldName(), getValueFieldName());
    
    if (partitionToBuild != null) {
      builder.setPartitionToBuild(partitionToBuild);
    }
    
    properties.putAll(CascadingHelper.DEFAULT_PROPERTIES);
    return builder;
  }
  
  protected abstract Pipe getPipe() throws Exception;
  
  protected abstract String getKeyFieldName();
  
  protected abstract String getValueFieldName();
  
  protected abstract Map<String, Tap> getSources();
  
  protected abstract Map<String, Tap> getSinks();
  
  protected abstract Pipe[] getTails();
  
  protected void postProcessFlow(Flow flow) {
    // Default is no-op
  }
  
  protected void prepare() {
    // Default is no-op
  }
}
