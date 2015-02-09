package com.rapleaf.cascading_ext.workflow2.registry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;

import com.liveramp.cascading_ext.zookeeper.ThriftMapCache;
import com.liveramp.java_support.constants.ZkConstants;
import com.liveramp.types.workflow.LiveWorkflowMeta;
import com.rapleaf.cascading_ext.queues.LiverampQueues;

public class ZkRegistry implements WorkflowRegistry {
  private static final Logger LOG = Logger.getLogger(ZkRegistry.class);

  private ThriftMapCache<LiveWorkflowMeta> liveWorkflowMap;
  private CuratorFramework framework;


  @Override
  public void register(String uuid, LiveWorkflowMeta meta) {
    try {

      framework = CuratorFrameworkFactory.newClient(ZkConstants.LIVERAMP_ZK_CONNECT_STRING,
          6 * LiverampQueues.TEN_SECONDS,
          LiverampQueues.TEN_SECONDS,
          new RetryNTimes(3, 100)
      );
      framework.start();

      liveWorkflowMap = new ThriftMapCache<LiveWorkflowMeta>(
          framework,
          ZkConstants.PRODUCTION_ZK_WORKFLOW_REGISTRY,
          new LiveWorkflowMeta(),
          true
      );

      liveWorkflowMap.put(uuid, meta);

    } catch (Exception e) {
      LOG.info("Failed to create live workflow node!", e);
    }

  }

  @Override
  public void deregister() {
    try {
      if (liveWorkflowMap != null) {
        liveWorkflowMap.shutdown();
      }
      if (framework != null) {
        framework.close();
      }
    } catch (Exception e) {
      LOG.info("Failed to shutdown map!", e);
    }
  }
}
