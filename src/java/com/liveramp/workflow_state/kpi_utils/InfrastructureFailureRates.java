package com.liveramp.workflow_state.kpi_utils;

import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.models.MapreduceJobTaskException;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.jack.IDb;
import com.rapleaf.jack.queries.Record;
import com.rapleaf.jack.queries.Records;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by lerickson on 8/2/16.
 */
public class InfrastructureFailureRates {

  public double getTaskFailureRate(long startWindowMillis, IDb db) {
    return getTaskFailureRate(startWindowMillis, db);
  }

  public double getTaskFailureRate(long startWindowMillis, long endWindowMillis, IDb db) throws IOException {

    long total = 0L;
    long infra = 0L;

    List<Long> steps = db.createQuery()
        .from(StepAttempt.TBL)
        .where(StepAttempt.END_TIME.between(startWindowMillis, endWindowMillis))
        .where(StepAttempt.STEP_STATUS.between(1, 4))
        .select(StepAttempt.ID)
        .fetch()
        .getLongs(StepAttempt.ID);

    Set<Integer> stepIds = new HashSet<>();
    for (Long step : steps) {
      stepIds.add(step.intValue());
    }

    Records jobs = db.createQuery()
        .from(MapreduceJob.TBL)
        .where(MapreduceJob.STEP_ATTEMPT_ID.in(stepIds))
        .where(MapreduceJob.TASKS_SAMPLED.greaterThan(0))
        .select(MapreduceJob.TASKS_SAMPLED,
            MapreduceJob.ID)
        .fetch();

    Set<Integer> jobIds = new HashSet<>();
    for (Record job : jobs) {
      jobIds.add(job.get(MapreduceJob.ID).intValue());
      total += job.get(MapreduceJob.TASKS_SAMPLED);
    }

    Records task_exceptions = db.createQuery()
        .from(MapreduceJobTaskException.TBL)
        .where(MapreduceJobTaskException.MAPREDUCE_JOB_ID.in(jobIds))
        .fetch();

    for (Record task: task_exceptions) {
      if (ErrorMessageClassifier.classifyTaskFailure(task.get(MapreduceJobTaskException.EXCEPTION))) {
        infra++;
      }
    }
    return infra/(double)total;
  }


  public double getAppFailureRate(long startWindowMillis, IDb db) throws IOException {
    return getAppFailureRate(startWindowMillis, System.currentTimeMillis(), db);
  }

  public double getAppFailureRate(long startWindowMillis, long endWindowMillis, IDb db) throws IOException {
    long count=0;
    long infrastructure_failures=0;

    Records stepattempts = db.createQuery()
        .from(StepAttempt.TBL)
        .where(StepAttempt.END_TIME.between(startWindowMillis,endWindowMillis))
        .select(StepAttempt.ID,StepAttempt.STEP_STATUS,StepAttempt.FAILURE_CAUSE)
        .fetch();

    for (Record attempt : stepattempts) {
      if (attempt == null) {
        continue;
      }
      Integer status = attempt.get(StepAttempt.STEP_STATUS);
      if (status==2) {
        count++;
      }
      else if (status == 3) {
        count++;
        if (ErrorMessageClassifier.classifyFailedStepAttempt(attempt.get(StepAttempt.FAILURE_CAUSE),attempt.get(StepAttempt.ID),db)) {
          infrastructure_failures++;
        }
      }
    }

    return infrastructure_failures/(double)count;
  }
}

