package com.rapleaf.cascading_ext.workflow2.action;

import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.HankDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.liveramp.hank.cascading.CascadingDomainBuilder;
import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.coordinator.RunWithCoordinator;
import com.liveramp.hank.hadoop.DomainBuilderProperties;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class HankDomainsBuilderAction extends Action {

  protected final Map<Object, Object> properties;

  protected final HankDataStore[] outputs;
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

    Map<String, Pipe> pipes = getPipes();

    for (HankDataStore output : outputs) {
      Pipe pipe = pipes.get(output.getDomainName());
      if (pipe == null) {
        throw new RuntimeException("Pipe for domain " + output.getDomainName() + " was not provided by getPipes: " + pipes);
      }
      cdbs.add(makeDomainBuilder(pipe, output));
    }

    properties.putAll(CascadingHelper.get().getDefaultProperties());

    Flow flow = CascadingDomainBuilder.buildDomains(
        CascadingHelper.get().getFlowConnectorFactory(properties),
        asProperties(properties),
        getSources(),
        getOtherSinks(),
        getOtherTails(),
        cdbs.toArray(new CascadingDomainBuilder[cdbs.size()]));

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

  protected CascadingDomainBuilder makeDomainBuilder(Pipe pipe, HankDataStore output) throws Exception {
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
        domainVersionProperties, pipe, getKeyFieldName(), getValueFieldName());

    if (partitionToBuild != null) {
      builder.setPartitionToBuild(partitionToBuild);
    }

    return builder;
  }

  // Domain name to Pipe
  protected abstract Map<String, Pipe> getPipes() throws Exception;

  protected abstract String getKeyFieldName();

  protected abstract String getValueFieldName();

  protected abstract Map<String, Tap> getSources();

  protected Map<String, Tap> getOtherSinks() {
    // Default is empty
    return new HashMap<String, Tap>();
  }

  protected Pipe[] getOtherTails() {
    // Default is empty
    return new Pipe[0];
  }

  protected void postProcessFlow(Flow flow) {
    // Default is no-op
  }

  protected void prepare() {
    // Default is no-op
  }

  private static Properties asProperties(Map<Object, Object> properties) {
    Properties props = new Properties();
    props.putAll(properties);
    return props;
  }
}
