package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.liveramp.hank.cascading.CascadingDomainBuilder;
import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.Domains;
import com.liveramp.hank.coordinator.RunWithCoordinator;
import com.liveramp.hank.coordinator.RunnableWithCoordinator;
import com.liveramp.hank.hadoop.DomainBuilderProperties;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.HankDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public abstract class HankDomainBuilderAction extends Action {

  protected final Map<Object, Object> properties;

  private final HankDataStore output;
  private final boolean allowSimultaneousDeltas;
  protected HankVersionType versionType;
  private boolean shouldPartitionAndSortInput = true;
  private final CoordinatorConfigurator configurator;
  private Integer partitionToBuild = null;
  private Integer domainVersionNumber = null;

  private DomainVersionStorage domainVersionStorage = null;

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
      boolean shouldPartitionAndSortInput,
      CoordinatorConfigurator configurator,
      HankDataStore output) {
    this(checkpointToken, null, versionType, shouldPartitionAndSortInput, false, configurator, output, Maps.newHashMap());
  }

  public HankDomainBuilderAction(
      String checkpointToken,
      HankVersionType versionType,
      CoordinatorConfigurator configurator,
      HankDataStore output,
      Map<Object, Object> properties) {
    this(checkpointToken, null, versionType, true, false, configurator, output, properties);
  }

  public HankDomainBuilderAction(String checkpointToken,
                                 String tmpRoot,
                                 HankVersionType versionType,
                                 CoordinatorConfigurator configurator,
                                 HankDataStore output) {
    this(checkpointToken, tmpRoot, versionType, true, false, configurator, output, Maps.newHashMap());
  }

  public HankDomainBuilderAction(String checkpointToken,
                                 String tmpRoot,
                                 HankVersionType versionType,
                                 boolean shouldPartitionAndSortInput,
                                 CoordinatorConfigurator configurator,
                                 HankDataStore output) {
    this(checkpointToken, tmpRoot, versionType, shouldPartitionAndSortInput, false, configurator, output, Maps.newHashMap());
  }

  public HankDomainBuilderAction(String checkpointToken,
                                 String tmpRoot,
                                 HankVersionType versionType,
                                 boolean shouldPartitionAndSortInput,
                                 CoordinatorConfigurator configurator,
                                 HankDataStore output,
                                 Map<Object, Object> properties) {
    this(checkpointToken, tmpRoot, versionType, shouldPartitionAndSortInput, false, configurator, output, properties);
  }

  public HankDomainBuilderAction(String checkpointToken,
                                 String tmpRoot,
                                 HankVersionType versionType,
                                 boolean shouldPartitionAndSortInput,
                                 boolean allowSimultaneousDeltas,
                                 CoordinatorConfigurator configurator,
                                 HankDataStore output,
                                 Map<Object, Object> properties) {
    super(checkpointToken, tmpRoot);
    this.versionType = versionType;
    this.shouldPartitionAndSortInput = shouldPartitionAndSortInput;
    this.allowSimultaneousDeltas = allowSimultaneousDeltas;
    this.configurator = configurator;
    this.output = output;
    this.properties = properties;
  }


  private static class IncrementalDomainVersionPropertiesDeltaGetter implements RunnableWithCoordinator {

    private final String domainName;
    private final boolean allowSimultaneousDeltas;
    private IncrementalDomainVersionProperties result;

    public IncrementalDomainVersionPropertiesDeltaGetter(String domainName, boolean allowSimultaneousDeltas) {
      this.domainName = domainName;
      this.allowSimultaneousDeltas = allowSimultaneousDeltas;
    }

    @Override
    public void run(Coordinator coordinator) throws IOException {
      Domain domain = DomainBuilderProperties.getDomain(coordinator, domainName);

      if (!allowSimultaneousDeltas) {
        if (Domains.hasOpenDelta(domain)) {
          throw new RuntimeException("Multiple deltas are open!.  Please mark defunct and delete any old deltas");
        }
      }

      result = new IncrementalDomainVersionProperties.Delta(domain);
    }
  }

  @Override
  public void execute() throws Exception {
    prepare();

    if (getVersionType() == null) {
      throw new IllegalStateException("Must set a version type before executing the domain builder!");
    }

    final DomainBuilderProperties domainBuilderProperties =
        new DomainBuilderProperties(
            output.getDomainName(),
            configurator)
            .setOutputPath(output.getPath())
            .setShouldPartitionAndSortInput(shouldPartitionAndSortInput);

    final IncrementalDomainVersionProperties domainVersionProperties;
    switch (versionType) {
      case BASE:
        domainVersionProperties = new IncrementalDomainVersionProperties.Base();
        break;
      case DELTA:
        IncrementalDomainVersionPropertiesDeltaGetter deltaGetter =
            new IncrementalDomainVersionPropertiesDeltaGetter(domainBuilderProperties.getDomainName(), allowSimultaneousDeltas);

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

      //  record stats
      runningFlow(flow);

      postProcessFlow(flow);

      if (domainVersionStorage != null) {
        domainVersionStorage.store(getDomainName(), getDomainVersionNumber());
      }
    }
  }

  public HankDomainBuilderAction setDomainVersionStorage(DomainVersionStorage domainVersionStorage) {
    this.domainVersionStorage = domainVersionStorage;
    return this;
  }

  public String getDomainName() {
    return output.getDomainName();
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

  protected abstract Map<String, Tap> getSources() throws IOException;

  protected void postProcessFlow(Flow flow) throws IOException {
    // Default is no-op
  }

  protected void prepare() {
    // Default is no-op
  }

  public static interface DomainVersionStorage {

    public void store(String domainName, Integer version);

    public Map<String, Integer> read();

  }

  public static class MockDomainStorage implements DomainVersionStorage {

    private final Map<String, Integer> memoryMap = Maps.newHashMap();

    @Override
    public void store(String domainName, Integer version) {
      memoryMap.put(domainName, version);
    }

    @Override
    public Map<String, Integer> read() {
      return memoryMap;
    }
  }

  public static class HdfsDomainVersionStorage implements DomainVersionStorage {

    private Path path;
    private FileSystem fs;

    public HdfsDomainVersionStorage(String path, FileSystem fs) {
      this.path = new Path(path);
      this.fs = fs;
    }

    @Override
    public void store(String domainName, Integer version) {
      try {
        Map<String, Integer> map;
        if (fs.exists(path)) {
          ObjectInputStream ois = new ObjectInputStream(fs.open(path));
          map = (Map<String, Integer>)ois.readObject();
          ois.close();
        } else {
          map = new HashMap<>();
        }
        map.put(domainName, version);

        ObjectOutputStream oos = new ObjectOutputStream(fs.create(path, true));
        oos.writeObject(map);
        oos.close();

      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }


    }

    @Override
    public Map<String, Integer> read() {
      try {
        Map<String, Integer> map;
        if (fs.exists(path)) {
          ObjectInputStream ois = new ObjectInputStream(fs.open(path));
          map = (Map<String, Integer>)ois.readObject();
          ois.close();
        } else {
          map = new HashMap<>();
        }
        return map;
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
