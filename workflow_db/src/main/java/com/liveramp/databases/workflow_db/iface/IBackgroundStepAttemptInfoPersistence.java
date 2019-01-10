
/**
 * Autogenerated by Jack
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.liveramp.databases.workflow_db.iface;

import com.liveramp.databases.workflow_db.models.BackgroundStepAttemptInfo;
import com.liveramp.databases.workflow_db.query.BackgroundStepAttemptInfoQueryBuilder;
import com.liveramp.databases.workflow_db.query.BackgroundStepAttemptInfoDeleteBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.List;

import com.rapleaf.jack.IModelPersistence;

public interface IBackgroundStepAttemptInfoPersistence extends IModelPersistence<BackgroundStepAttemptInfo> {
  BackgroundStepAttemptInfo create(final long step_attempt_id, final byte[] serialized_context, final long next_execute_check, final int execute_check_cooldown_seconds, final String claimed_by_worker, final Integer background_workflow_executor_info_id) throws IOException;
  BackgroundStepAttemptInfo create(final long step_attempt_id, final byte[] serialized_context, final long next_execute_check, final int execute_check_cooldown_seconds) throws IOException;

  BackgroundStepAttemptInfo createDefaultInstance() throws IOException;
  List<BackgroundStepAttemptInfo> findByStepAttemptId(long value)  throws IOException;
  List<BackgroundStepAttemptInfo> findBySerializedContext(byte[] value)  throws IOException;
  List<BackgroundStepAttemptInfo> findByNextExecuteCheck(long value)  throws IOException;
  List<BackgroundStepAttemptInfo> findByExecuteCheckCooldownSeconds(int value)  throws IOException;
  List<BackgroundStepAttemptInfo> findByClaimedByWorker(String value)  throws IOException;
  List<BackgroundStepAttemptInfo> findByBackgroundWorkflowExecutorInfoId(Integer value)  throws IOException;

  BackgroundStepAttemptInfoQueryBuilder query();

  BackgroundStepAttemptInfoDeleteBuilder delete();
}