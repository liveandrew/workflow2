package com.liveramp.kpi_utils;

import com.liveramp.commons.collections.list.ListBuilder;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.models.MapreduceJobTaskException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by lerickson on 7/27/16.
 */
public class ErrorMessageClassifier {
  //TODO: migrate to workflow_db
  private static final IRlDb db = new DatabasesImpl().getRlDb();

  //regex patterns that classify an exception string as infrastructure-related
  private static final List<String> TASK_PATTERN_STRS = new ListBuilder<String>()
      .add("Not able to initialize .+ directories in any of the configured local directories for")
      .add("mkdir of .+ failed")
      .add("Error: java.io.IOException: All datanodes [.0-9:]+ are bad")
      .get();

  //substrings that classify an exception string as infrastructure-related
  private static final List<String> TASK_STRING_INCLUSIONS = new ListBuilder<String>()
      .add("Error: org.apache.hadoop.util.DiskChecker$DiskErrorException: Could not find")
      .add("Error: java.io.IOException: Failed to finalize bucket file")
      .add("Error: cascading.tap.TapException: exception closing:")
      .add("failed on connection exception: java.net.ConnectException: Connection refused; For more details see")
      .add("java.io.FileNotFoundException: File does not exist")
      .add("Error: java.io.IOException: Unable to close file because the last block does not have enough number of replicas.")
      .add("org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block")
      .add("Could not initialize log dir")
      .add("Error: java.io.EOFException: Premature EOF")
      .add("Cannot initialize without local dirs")
      .add("Error: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException): No lease on")
      .add("java.net.UnknownHostException")
      .add("Too many open files")
      .add("Error: java.io.IOException: Invalid LZO header")
      .add("No Route to Host from")
      .add("Exception from container-launch.")
      .add("Could not initialize local dir")
      .get();

  //exception strings that are infrastructure-related
  private static final List<String> TASK_STRINGS = new ListBuilder<String>()
      .add("/bin/ls")
      .get();

  //regex patterns that classify an exception string as infrastructure-related
  private static final List<String> FAILURE_CAUSE_PATTERNS_STRS = new ListBuilder<String>()
      .add("could not build flow from assembly:.*exception closing:|exception closing:.*could not build flow from assembly:")
      .add("Failed to determine whether resource .+ is stored")
      .add("^Couldn't persist data for request")
      .add("All datanodes .+ are bad. Aborting...")
      .add("MSJ Store .+ creation locked for > 24 hours")
      .add("Communications link failure\n*The last packet successfully received from the server was .+ milliseconds ago.")
      .add("java.net.SocketTimeoutException: .+ millis timeout while waiting for channel to be ready for read.")
      .add("could not build flow from assembly:.*Trying to read meta file from an empty stream|Trying to read meta file from an empty stream.*could not build flow from assembly:")
      .add("java.io.FileNotFoundException: .+ \\(Too many open files\\)")
      .add("Operation category .+ is not supported in state standby")
      .add("Exception connecting to .*Failed to connect to |Failed to connect to .*Exception connecting to ")
      .add("Unable to store resource .+ and value")
      .get();

  //substrings that classify an exception string as infrastructure-related
  private static final List<String> FAILURE_CAUSE_STRING_INCLUSIONS = new ListBuilder<String>()
      .add("java.io.IOException: Error sending email")
      .add("java.io.EOFException: Premature EOF: no length prefix available")
      .add("java.io.IOException: Unable to close file because the last block does not have enough number of replicas.")
      .add("unable to read from input identifier")
      .add("org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException")
      .add("Exception trying to reload contents of /hank/domains")
      .add("org.apache.hadoop.fs.FSError")
      .add("java.io.IOException: java.sql.SQLException: Lock wait timeout exceeded")
      .add("org.apache.thrift.transport.TTransportException")
      .add("Failed on local exception: java.net.SocketException: Too many open files")
      .add("java.io.IOException: Filesystem closed")
      .add("org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block")
      .add("Exception while loading paths.")
      .add("java.io.IOException: Failed on local exception: java.net.SocketException: No buffer space available")
      .add("failed on connection exception: java.net.ConnectException: Connection refused")
      .add("Abilitec Append Record Retriever Construction Error")
      .get();

  //exception strings that are infrastructure-related
  private static final List<String> FAILURE_CAUSE_STRINGS = new ListBuilder<String>()
      .get();

  /**
   * Failure causes that do not carry enough info are passed to TaskExceptions classifier
   */
  //regex patterns that classify an exception string as infrastructure-related
  private static final List<String> USE_TASKS_PATTERN_STRS = new ListBuilder<String>()
      .add("\n-+\nstep attempt failures: \n-+\n")
      .get();

  //substrings that classify an exception string as infrastructure-related
  private static final List<String> USE_TASKS_STRING_INCLUSIONS = new ListBuilder<String>()
      .get();

  //exception strings that are infrastructure-related
  private static final List<String> USE_TASKS_STRINGS = new ListBuilder<String>()
      .add("Giraph job failed!")
      .add("java.io.IOException: Job failed!")
      .get();

  private static final List<Pattern> TASK_PATTERNS = makePatterns(TASK_PATTERN_STRS);

  private static final List<Pattern> CAUSE_PATTERNS = makePatterns(FAILURE_CAUSE_PATTERNS_STRS);

  private static final List<Pattern> USE_TASKS_PATTERNS = makePatterns(USE_TASKS_PATTERN_STRS);

  private static List<Pattern> makePatterns (List<String> pattern_strings) {
    List<Pattern> patterns = new ArrayList<>();
    for (String s :
        TASK_PATTERN_STRS) {
      patterns.add(Pattern.compile(s));
    }
    return patterns;
  }

  private static boolean applyRegexes (String string, List<Pattern> patterns) {
    for (Pattern pattern:
        patterns) {
      if (pattern.matcher(string).matches()) {
        return true;
      }
    }
    return false;
  }

  private static boolean applySubstrings (String string, List<String> substrings) {
    for (String substring:
        substrings) {
      if (string.contains(substring)) {
        return true;
      }
    }
    return false;
  }

  private static boolean applyEqualities (String string, List<String> strings) {
    for (String string2:
        strings) {
      if (string2.equals(string)) {
        return true;
      }
    }
    return false;
  }

  /**
   * For establishing App failure rates due solely to infrastructure issues.
   *
   * @param failure_cause the failure_cause field from the StepAttempt table
   * @param step_attempt_id the step_attempt_id field from the StepAttempt table
   *                        null -> ignore task exceptions used for some classification patterns
   * @return true if an exception is determined to be infrastructural
   */
  public static boolean classifyFailedStepAttempt (String failure_cause, Long step_attempt_id) throws IOException {
    return applyRegexes(failure_cause, CAUSE_PATTERNS)
        || applySubstrings(failure_cause, FAILURE_CAUSE_STRING_INCLUSIONS)
        || applyEqualities(failure_cause, FAILURE_CAUSE_STRINGS)
        || (step_attempt_id != null && (applyRegexes(failure_cause, USE_TASKS_PATTERNS)
            || applySubstrings(failure_cause, USE_TASKS_STRING_INCLUSIONS)
            || applyEqualities(failure_cause, USE_TASKS_STRINGS)) &&
        classifyTaskFailure(step_attempt_id));
  }

  /**
   * For establishing Task failure rates due solely to infrastructure issues.
   *
   * @param exception the failure_cause field from the MapreduceJobTaskException table
   * @return true if an exception is determined to be infrastructural
   */
  public static boolean classifyTaskFailure (String exception) throws IOException {
    return applyRegexes(exception, TASK_PATTERNS)
        || applySubstrings(exception, TASK_STRING_INCLUSIONS)
        || applyEqualities(exception, TASK_STRINGS);
  }

  /**
   * For establishing App failure rates due solely to infrastructure issues.
   *
   * @param step_attempt_id the step_attempt_id of a failed step
   * @return true if any recorded task exceptions for the failed step are determined to be infrastructural
   */
  public static boolean classifyTaskFailure(Long step_attempt_id) throws IOException {
    List<Long> mr_ids = db.createQuery()
        .from(MapreduceJob.TBL)
        .where(MapreduceJob.STEP_ATTEMPT_ID.equalTo(step_attempt_id.intValue()))
        .where(MapreduceJob.TASKS_FAILED_IN_SAMPLE.greaterThan(0))
        .select(MapreduceJob.ID)
        .fetch()
        .gets(MapreduceJob.ID);
    List<Integer> mr_ints = new ArrayList<>();
    for(Long l : mr_ids) mr_ids.add(l);
    List<String> exceptions = new ArrayList<>();
    if (mr_ids.size() > 0) {
      exceptions = db.createQuery()
          .from(MapreduceJobTaskException.TBL)
          .where(MapreduceJobTaskException.MAPREDUCE_JOB_ID.in(mr_ints))
          .select(MapreduceJobTaskException.EXCEPTION)
          .fetch()
          .gets(MapreduceJobTaskException.EXCEPTION);
    }
    for (String exception:
        exceptions) {
      if (classifyTaskFailure(exception)) {
        return true;
      }
    }
    return false;
  }

}
