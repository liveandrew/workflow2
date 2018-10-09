package com.liveramp.workflow_db_state.kpi_utils;

import com.google.common.collect.Sets;
import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.databases.workflow_db.models.MapreduceJobTaskException;
import com.liveramp.databases.workflow_db.models.StepAttempt;
import com.rapleaf.jack.queries.Record;
import com.rapleaf.jack.queries.Records;

import java.io.IOException;
import java.util.Set;

/**
 * Created by lerickson on 8/2/16.
 */
public class InfrastructureFailureRates {

  public static InfrastructureFailureInfo getTaskFailureInfo(long startWindowMillis, IDatabases dbs) throws IOException {
    return getTaskFailureInfo(startWindowMillis, System.currentTimeMillis(), dbs);
  }

  public static InfrastructureFailureInfo getTaskFailureInfo(long startWindowMillis, long endWindowMillis, IDatabases dbs) throws IOException {

    long sampledTasks = 0L;
    long exception_count = 0L;
    long infra = 0L;


    Set<Integer> jobIds = Sets.newHashSet();

    for (Record job : dbs.getWorkflowDb().createQuery().from(StepAttempt.TBL)
        .where(StepAttempt.END_TIME.isNotNull())
        .where(StepAttempt.END_TIME.greaterThan(startWindowMillis))
        .where(StepAttempt.END_TIME.lessThanOrEqualTo(endWindowMillis))
        .innerJoin(MapreduceJob.TBL)
        .on(MapreduceJob.STEP_ATTEMPT_ID.equalTo(StepAttempt.ID))
        .select(MapreduceJob.TBL.getAllColumns()).fetch()) {
      if (job == null) {
        continue;
      }
      Integer sampled = job.get(MapreduceJob.TASKS_SAMPLED);
      if ( sampled==null ) {
        continue;
      }
      jobIds.add(job.get(MapreduceJob.ID).intValue());
      sampledTasks += sampled;
    }


    Records task_exceptions = dbs.getWorkflowDb().createQuery()
        .from(MapreduceJobTaskException.TBL)
        .where(MapreduceJobTaskException.MAPREDUCE_JOB_ID.in(jobIds))
        .fetch();

    for (Record task: task_exceptions) {
      if (ErrorMessageClassifier.classifyTaskFailure(task.get(MapreduceJobTaskException.EXCEPTION))) {
        infra++;
      }
      exception_count++;
    }

    return new InfrastructureFailureInfo(infra,exception_count,sampledTasks);
  }

  public static InfrastructureFailureInfo getAppFailureInfo(long startWindowMillis, IDatabases dbs) throws IOException {
    return getAppFailureInfo(startWindowMillis, System.currentTimeMillis(), dbs);
  }

  public static InfrastructureFailureInfo getAppFailureInfo(long startWindowMillis, long endWindowMillis, IDatabases dbs) throws IOException {
    long total = 0L;
    long infra = 0L;
    long failure_count = 0L;

    Records stepattempts = dbs.getWorkflowDb().createQuery()
        .from(StepAttempt.TBL)
        .where(StepAttempt.END_TIME.between(startWindowMillis,endWindowMillis))
        .select(StepAttempt.ID,StepAttempt.STEP_STATUS,StepAttempt.FAILURE_CAUSE)
        .fetch();

    for (Record attempt : stepattempts) {
      if (attempt == null) {
        continue;
      }
      Integer status = attempt.get(StepAttempt.STEP_STATUS);
      if (status == 2) {
        total++;
      }
      else if (status == 3) {
        total++;
        failure_count++;
        if (ErrorMessageClassifier.classifyFailedStepAttempt(attempt.get(StepAttempt.FAILURE_CAUSE),attempt.get(StepAttempt.ID),dbs.getWorkflowDb())) {
          infra++;
        }
      }
    }

    return new InfrastructureFailureInfo(infra,failure_count,total);
  }

  public static class InfrastructureFailureInfo {
    private final long numInfrastructureFailures;
    private final long sampleSize;
    private final long numTotalFailures;

    public InfrastructureFailureInfo(long numInfrastructureFailures,
                                     long numTotalFailures,
                                     long sampleSize) {
      this.numInfrastructureFailures = numInfrastructureFailures;
      this.numTotalFailures = numTotalFailures;
      this.sampleSize = sampleSize;
    }

    public long getNumInfrastructureFailures() {
      return numInfrastructureFailures;
    }

    public long getSampleSize() {
      return sampleSize;
    }

    public long getNumTotalFailures() {
      return numTotalFailures;
    }
  }

}

