package com.rapleaf.cascading_ext.workflow2.action;

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
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;

public abstract class HankDomainBuilderAction extends Action {

  protected final Map<Object, Object> properties;

  private final HankDataStore output;
  private final HankVersionType versionType;
  private final CoordinatorConfigurator configurator;

  public HankDomainBuilderAction(String checkpointToken,
      HankVersionType versionType,
      CoordinatorConfigurator configurator,
      HankDataStore output) {
    super(checkpointToken);
    this.versionType = versionType;
    this.configurator = configurator;
    this.output = output;
    this.properties = new HashMap<Object, Object>();
  }

  public void execute() throws Exception {
    DomainBuilderProperties domainBuilderProperties = new DomainBuilderProperties(
      output.getDomainName(), configurator, output.getPath());
    IncrementalDomainVersionProperties domainVersionProperties;
    switch (versionType) {
      case BASE:
        domainVersionProperties = new IncrementalDomainVersionProperties.Base();
        break;
      case DELTA:
        domainVersionProperties = new IncrementalDomainVersionProperties.Delta(
          domainBuilderProperties.getDomain());
        break;
      default:
        throw new RuntimeException("Unknown version type: " + versionType);
    }
    CascadingDomainBuilder builder = new CascadingDomainBuilder(domainBuilderProperties,
      domainVersionProperties, getPipe(), getKeyFieldName(), getValueFieldName());
    properties.putAll(CascadingHelper.DEFAULT_PROPERTIES);
    Flow flow = builder.build(properties, getSources());
    if (flow != null) {
      postProcessFlow(flow);
    }
  }

  protected abstract Pipe getPipe() throws Exception;

  protected abstract String getKeyFieldName();

  protected abstract String getValueFieldName();

  protected abstract Map<String, Tap> getSources();

  protected void postProcessFlow(Flow flow) {
    // Default is no-op
  }
}
