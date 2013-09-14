package com.rapleaf.cascading_ext.workflow2.action;

import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import com.google.common.collect.Maps;
import com.liveramp.hank.cascading.CascadingDomainBuilder;
import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.RunWithCoordinator;
import com.liveramp.hank.coordinator.RunnableWithCoordinator;
import com.liveramp.hank.hadoop.DomainBuilderProperties;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.HankDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

import java.io.IOException;
import java.util.Map;

public abstract class HankDomainBuilderAction extends Action {

  protected final Map<Object, Object> properties;

  private final HankDataStore output;
  protected HankVersionType versionType;
  private final CoordinatorConfigurator configurator;
  private Integer partitionToBuild = null;
  private Integer domainVersionNumber = null;

  public HankDomainBuilderAction(
      String checkpointToken,
      HankVersionType versionType,
      CoordinatorConfigurator configurator,
      HankDataStore output) {
    this(checkpointToken, null, versionType, configurator, output);
  }

  public HankDomainBuilderAction(
      String checkpointToken,
      HankVersionType versionType,
      CoordinatorConfigurator configurator,
      HankDataStore output,
      Map<Object, Object> properties) {
    this(checkpointToken, null, versionType, configurator, output, properties);
  }

  public HankDomainBuilderAction(String checkpointToken,
                                 String tmpRoot,
                                 HankVersionType versionType,
                                 CoordinatorConfigurator configurator,
                                 HankDataStore output) {
    this(checkpointToken, tmpRoot, versionType, configurator, output, Maps.newHashMap());
  }

  public HankDomainBuilderAction(String checkpointToken,
                                 String tmpRoot,
                                 HankVersionType versionType,
                                 CoordinatorConfigurator configurator,
                                 HankDataStore output,
                                 Map<Object, Object> properties) {
    super(checkpointToken, tmpRoot);
    this.versionType = versionType;
    this.configurator = configurator;
    this.output = output;
    this.properties = properties;
  }


  private static class IncrementalDomainVersionPropertiesDeltaGetter implements RunnableWithCoordinator {

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
  }

  @Override
  public void execute() throws Exception {
    prepare();

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
        domainVersionProperties = deltaGetter.result;
        break;
      default:
        throw new RuntimeException("Unknown version type: " + versionType);
    }

    CascadingDomainBuilder builder = new CascadingDomainBuilder(domainBuilderProperties,
        domainVersionProperties, getPipe(), getKeyFieldName(), getValueFieldName());

    if (partitionToBuild != null) {
      builder.setPartitionToBuild(partitionToBuild);
    }

    properties.putAll(CascadingHelper.get().getDefaultProperties());
    Flow flow = builder.build(CascadingHelper.get().getFlowConnectorFactory(properties), getSources());
    domainVersionNumber = builder.getDomainVersionNumber();

    if (flow != null) {
      postProcessFlow(flow);
    }
  }

  public Integer getDomainVersionNumber() {
    return domainVersionNumber;
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

  protected abstract Pipe getPipe() throws Exception;

  protected abstract String getKeyFieldName();

  protected abstract String getValueFieldName();

  protected abstract Map<String, Tap> getSources();

  protected void postProcessFlow(Flow flow) {
    // Default is no-op
  }

  protected void prepare() {
    // Default is no-op
  }
}
