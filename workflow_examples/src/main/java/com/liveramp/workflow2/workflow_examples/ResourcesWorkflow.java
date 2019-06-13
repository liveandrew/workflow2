package com.liveramp.workflow2.workflow_examples;

import java.io.IOException;
import java.util.Collections;

import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.workflow2.workflow_examples.actions.ResourceReadAction;
import com.liveramp.workflow2.workflow_examples.actions.ResourceWriteAction;
import com.liveramp.workflow2.workflow_hadoop.ResourceManagers;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunners;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;

public class ResourcesWorkflow {

  public static void main(String[] args) throws IOException {

    WorkflowRunners.dbRun(
        ResourcesWorkflow.class.getName(),
        HadoopWorkflowOptions.test()
            .setResourceManager(ResourceManagers.defaultResourceManager()),

        dbHadoopWorkflow -> {

          Resource<String> emptyResource = dbHadoopWorkflow.getManager().emptyResource("value-to-pass");

          Step resourceWriter = new Step(new ResourceWriteAction(
              "write-resource",
              emptyResource
          ));

          Step resourceReader = new Step(new ResourceReadAction(
              "read-resource",
              emptyResource
          ), resourceWriter);

          return Collections.singleton(resourceReader);

        }
    );

  }

}
