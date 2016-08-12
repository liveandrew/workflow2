package com.liveramp.workflow_db_state.kpi_utils;

import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.databases.workflow_db.models.MapreduceJobTaskException;
import com.rapleaf.jack.IDb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.liveramp.workflow_db_state.kpi_utils.ErrorMessagePatterns.*;
import static java.util.regex.Pattern.DOTALL;

/**
 * Created by lerickson on 7/27/16.
 */
public class ErrorMessageClassifier {

  private static final List<Pattern> TASK_PATTERNS = makePatterns(TASK_PATTERN_STRS);

  private static final List<Pattern> CAUSE_PATTERNS = makePatterns(FAILURE_CAUSE_PATTERNS_STRS);

  private static final List<Pattern> USE_TASKS_PATTERNS = makePatterns(USE_TASKS_PATTERN_STRS);

  private static List<Pattern> makePatterns (List<String> pattern_strings) {
    List<Pattern> patterns = new ArrayList<>();
    for (String s :
        pattern_strings) {
      patterns.add(Pattern.compile(s,DOTALL));
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
   * @param db link to the db
   * @return true if an exception is determined to be infrastructural
   */
  public static boolean classifyFailedStepAttempt (String failure_cause, Long step_attempt_id, IDb db) throws IOException {
    if (failure_cause == null) { return (classifyTaskFailure(step_attempt_id, db)); }
    return applyRegexes(failure_cause, CAUSE_PATTERNS)
        || applySubstrings(failure_cause, FAILURE_CAUSE_STRING_INCLUSIONS)
        || applyEqualities(failure_cause, FAILURE_CAUSE_STRINGS)
        || ((applyRegexes(failure_cause, USE_TASKS_PATTERNS)
            || applySubstrings(failure_cause, USE_TASKS_STRING_INCLUSIONS)
            || applyEqualities(failure_cause, USE_TASKS_STRINGS)) &&
        classifyTaskFailure(step_attempt_id, db));
  }

  /**
   * For establishing App failure rates due solely to infrastructure issues.
   *
   * @param failure_cause the failure_cause field from the StepAttempt table
   *
   * @return true if an exception is determined to be infrastructural
   */
  public static boolean classifyFailedStepAttempt (String failure_cause) throws IOException {
    return applyRegexes(failure_cause, CAUSE_PATTERNS)
        || applySubstrings(failure_cause, FAILURE_CAUSE_STRING_INCLUSIONS)
        || applyEqualities(failure_cause, FAILURE_CAUSE_STRINGS);
  }

  /**
   * For establishing Task failure rates due solely to infrastructure issues.
   *
   * @param exception the failure_cause field from the MapreduceJobTaskException table
   * @return true if an exception is determined to be infrastructural
   */
  public static boolean classifyTaskFailure (String exception) throws IOException {
    if (exception == null) { return false; }
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
  public static boolean classifyTaskFailure(Long step_attempt_id, IDb db) throws IOException {
    if (step_attempt_id == null) { return false; }
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
