package com.rapleaf.cascading_ext.workflow2.state;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.importer.generated.AppType;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.support.Rap;

public class HdfsCheckpointPersistence implements WorkflowStatePersistence.Factory {
  private static final Logger LOG = Logger.getLogger(HdfsPersistenceContainer.class);

  private final String checkpointDir;
  private final boolean deleteOnSuccess;

  public HdfsCheckpointPersistence(String checkpointDir) {
    this(checkpointDir, true);
  }

  public HdfsCheckpointPersistence(String checkpointDir, boolean deleteOnSuccess) {
    this.checkpointDir = checkpointDir;
    this.deleteOnSuccess = deleteOnSuccess;
  }
  @Override
  public WorkflowStatePersistence prepare(DirectedGraph<Step, DefaultEdge> flatSteps,
                      String name,
                      String uniqueId,
                      AppType appType,
                      String host,
                      String username,
                      String pool,
                      String priority) {

    FileSystem fs = FileSystemHelper.getFS();

    Map<String, StepState> statuses = Maps.newHashMap();
    List<DataStoreInfo> datastores = Lists.newArrayList();

    try {
      LOG.info("Creating checkpoint dir " + checkpointDir);
      fs.mkdirs(new Path(checkpointDir));

      Map<DataStore, DataStoreInfo> dataStoreToRep = Maps.newHashMap();

      for (Step val : flatSteps.vertexSet()) {
        Action action = val.getAction();

        Set<String> dependencies = Sets.newHashSet();
        for (DefaultEdge edge : flatSteps.outgoingEdgesOf(val)) {
          dependencies.add(flatSteps.getEdgeTarget(edge).getCheckpointToken());
        }

        Multimap<Action.DSAction, DataStoreInfo> stepDsInfo = HashMultimap.create();

        for (Map.Entry<Action.DSAction, DataStore> entry : val.getAction().getAllDatastores().entries()) {
          DataStore dataStore = entry.getValue();

          if (!dataStoreToRep.containsKey(dataStore)) {

            DataStoreInfo info = new DataStoreInfo(
                dataStore.getName(),
                dataStore.getClass().getName(),
                dataStore.getPath(),
                dataStoreToRep.size()
            );

            dataStoreToRep.put(dataStore, info);
            datastores.add(info);

          }

          stepDsInfo.put(entry.getKey(), dataStoreToRep.get(dataStore));

        }

        statuses.put(val.getCheckpointToken(), new StepState(
            val.getCheckpointToken(),
            StepStatus.WAITING,
            action.getClass().getSimpleName(),
            dependencies,
            stepDsInfo
        ));

      }


      for (FileStatus status : FileSystemHelper.safeListStatus(fs, new Path(checkpointDir))) {
        String token = status.getPath().getName();
        if (statuses.containsKey(token)) {
          statuses.get(token).setStatus(StepStatus.SKIPPED);
        } else {
          LOG.info("Skipping obsolete token " + token);
        }
      }

      return new HdfsPersistenceContainer(
          checkpointDir,
          deleteOnSuccess,
          Hex.encodeHexString(Rap.uuidToBytes(UUID.randomUUID())),
          name,
          priority,
          pool,
          host,
          username,
          statuses,
          datastores
      );

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
