package com.liveramp.workflow_ui.util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.liveramp.databases.workflow_db.IWorkflowDb;
import com.liveramp.databases.workflow_db.models.ApplicationCounterSummary;
import com.liveramp.databases.workflow_db.models.MapreduceCounter;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.databases.workflow_db.models.StepAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowExecution;
import com.liveramp.workflow_core.constants.YarnConstants;
import com.liveramp.workflow_ui.servlet.ClusterConstants;
import com.rapleaf.jack.queries.GenericQuery;
import com.rapleaf.jack.queries.Index;
import com.rapleaf.jack.queries.IndexHints;
import com.rapleaf.jack.queries.Record;
import com.rapleaf.jack.queries.where_operators.Between;
import com.rapleaf.jack.queries.where_operators.EqualTo;
import com.rapleaf.jack.queries.where_operators.IsNull;

import static com.rapleaf.jack.queries.AggregatedColumn.SUM;

public class Summarizer {
  private static final Logger LOG = LoggerFactory.getLogger(Summarizer.class);

  private static GenericQuery getSummarizationQuery(IWorkflowDb rldb, long start, long end, String group, String name) throws IOException {
    return rldb.createQuery().from(WorkflowExecution.TBL)
        .innerJoin(WorkflowAttempt.TBL)
        .on(WorkflowAttempt.WORKFLOW_EXECUTION_ID.equalTo(WorkflowExecution.ID.as(Integer.class)))
        .innerJoin(StepAttempt.TBL.with(IndexHints.force(Index.of("index_step_attempts_on_end_time"))))
        .on(StepAttempt.WORKFLOW_ATTEMPT_ID.equalTo(WorkflowAttempt.ID.as(Integer.class)))
        .innerJoin(MapreduceJob.TBL)
        .on(MapreduceJob.STEP_ATTEMPT_ID.equalTo(StepAttempt.ID))
        .innerJoin(MapreduceCounter.TBL)
        .on(MapreduceCounter.MAPREDUCE_JOB_ID.equalTo(MapreduceJob.ID.as(Integer.class)))

        .where(StepAttempt.END_TIME.between(start, end),
            MapreduceCounter.GROUP.in(group),
            MapreduceCounter.NAME.in(name))
        .groupBy(MapreduceCounter.NAME, MapreduceCounter.GROUP, WorkflowExecution.APPLICATION_ID)
        .select(WorkflowExecution.APPLICATION_ID, SUM(MapreduceCounter.VALUE));
  }


  public static void summarizeApplicationCounters(
      Multimap<String, String> countersToRecord,
      IWorkflowDb rldb,
      int dayWindow,
      LocalDate dateEnd) throws IOException, SQLException {

    LocalDate yesterday = new LocalDate().minusDays(1);

    //  never summarize earlier than yesterday, or we record incomplete summaries
    if(dateEnd.isAfter(yesterday)){
      dateEnd = yesterday;
    }

    LocalDate dateStart = dateEnd
        .minusDays(dayWindow);

    //  application_id is null for cluster sum

    for (Map.Entry<String, String> entry : countersToRecord.entries()) {

      String group = entry.getKey();
      String name = entry.getValue();

      LOG.info("\n");
      LOG.info("Looking at summaries for " + group + "." + name);

      List<ApplicationCounterSummary> summaries = rldb.applicationCounterSummaries().query()
          .whereDate(new Between<>(dateStart.toDate().getTime(), dateEnd.toDate().getTime()))
          .whereApplicationId(new IsNull<>())
          .whereGroup(new EqualTo<>(group))
          .whereName(new EqualTo<>(name))
          .find();

      Set<LocalDate> summarizedDates = Sets.newHashSet();
      for (ApplicationCounterSummary summary : summaries) {
        summarizedDates.add(new LocalDate(summary.getDate()));
      }

      for (int i = 0; i < dayWindow; i++) {
        LocalDate date = dateEnd.minusDays(i);
        LOG.info("Checking date: " + date);

        if (!summarizedDates.contains(date)) {
          LOG.info("Did not find a summary for date, fetching.");

          GenericQuery query = getSummarizationQuery(rldb, date.toDateTimeAtStartOfDay().toDate().getTime(),
              date.plusDays(1).toDateTimeAtStartOfDay().toDate().getTime(),
              group,
              name
          );

          long counterSum = 0L;
          int count = 0;

          for (Record record : query.fetch()) {
            count++;

            int appId = record.getInt(WorkflowExecution.APPLICATION_ID);
            long sum = record.getLong(SUM(MapreduceCounter.VALUE));
            counterSum += sum;

            LOG.info("Creating application counter summary:");
            LOG.info("  app: " + appId);
            LOG.info("  group: " + group);
            LOG.info("  name: " + name);
            LOG.info("  sum: " + sum);
            LOG.info("  date: " + date.toDate());

            rldb.applicationCounterSummaries().create(
                appId,
                group,
                name,
                sum,
                date.toDate().getTime()
            );

          }

          LOG.info("Recording records for " + count + " applications.");

          //  application_id is null for cluster sum
          ApplicationCounterSummary clusterSummary = rldb.applicationCounterSummaries().create(
              null,
              group,
              name,
              counterSum,
              date.toDate().getTime()
          );

          LOG.info("Recording cluster summary: " + clusterSummary);

        }
      }

    }

    LOG.info("Done recording summaries");
  }

  public static final Multimap<String, String> COUNTERS_TO_SUMMARIZE = new MultimapBuilder<String, String>()
      .put(ClusterConstants.MR2_GROUP, ClusterConstants.VCORE_MAP)
      .put(ClusterConstants.MR2_GROUP, ClusterConstants.VCORE_RED)
      .put(ClusterConstants.MR2_GROUP, ClusterConstants.MB_MAP)
      .put(ClusterConstants.MR2_GROUP, ClusterConstants.MB_RED)

      .put("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_READ_OPS")
      .put("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_WRITE_OPS")
      .put("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_LARGE_READ_OPS")
      .put("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_READ")
      .put("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_WRITTEN")

      .put("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_MAPS")
      .put("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_REDUCES")

      .put(YarnConstants.YARN_GROUP, YarnConstants.YARN_MB_SECONDS)
      .put(YarnConstants.YARN_GROUP, YarnConstants.YARN_VCORE_SECONDS)

      .get();


}