package com.liveramp.workflow_state.kpi_utils;

import com.google.common.collect.Sets;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.models.MapreduceJobTaskException;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
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
    long infra = 0L;


    Set<Integer> jobIds = Sets.newHashSet();

    for (Record job : dbs.getRlDb().createQuery().from(StepAttempt.TBL)
        .where(StepAttempt.END_TIME.isNotNull())
        .where(StepAttempt.END_TIME.greaterThan(startWindowMillis))
        .where(StepAttempt.END_TIME.lessThanOrEqualTo(endWindowMillis))
        .innerJoin(MapreduceJob.TBL)
        .on(MapreduceJob.STEP_ATTEMPT_ID.equalTo(StepAttempt.ID.as(Integer.class)))
        .select(MapreduceJob.TBL.getAllColumns()).fetch()) {
      if (job == null) {
        continue;
      }
      jobIds.add(job.get(MapreduceJob.ID).intValue());
      sampledTasks+=job.get(MapreduceJob.TASKS_SAMPLED);
    }


    Records task_exceptions = dbs.getRlDb().createQuery()
        .from(MapreduceJobTaskException.TBL)
        .where(MapreduceJobTaskException.MAPREDUCE_JOB_ID.in(jobIds))
        .fetch();

    for (Record task: task_exceptions) {
      System.out.println("task.toString() = " + task.get(MapreduceJobTaskException.EXCEPTION));
      if (ErrorMessageClassifier.classifyTaskFailure(task.get(MapreduceJobTaskException.EXCEPTION))) {
        infra++;
      }
    }

    return new InfrastructureFailureInfo(infra,sampledTasks);
  }

  public static InfrastructureFailureInfo getAppFailureInfo(long startWindowMillis, IDatabases dbs) throws IOException {
    return getAppFailureInfo(startWindowMillis, System.currentTimeMillis(), dbs);
  }

  public static InfrastructureFailureInfo getAppFailureInfo(long startWindowMillis, long endWindowMillis, IDatabases dbs) throws IOException {
    long total = 0L;
    long infra = 0L;

    Records stepattempts = dbs.getRlDb().createQuery()
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
        if (ErrorMessageClassifier.classifyFailedStepAttempt(attempt.get(StepAttempt.FAILURE_CAUSE),attempt.get(StepAttempt.ID),dbs.getRlDb())) {
          infra++;
        }
      }
    }

    return new InfrastructureFailureInfo(infra,total);
  }

  public static class InfrastructureFailureInfo {
    private final long numInfrastructureFailures;
    private final long sampleSize;
    public InfrastructureFailureInfo(long numInfrastructureFailures,
                                     long sampleSize) {
      this.numInfrastructureFailures = numInfrastructureFailures;
      this.sampleSize = sampleSize;
    }

    public long getNumInfrastructureFailures() {
      return numInfrastructureFailures;
    }

    public long getSampleSize() {
      return sampleSize;
    }
  }

}

