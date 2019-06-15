package com.liveramp.spark_lib.workflow;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import com.liveramp.cascading_ext.flow.JobPersister;
import com.liveramp.cascading_tools.jobs.TrackedOperation;
import com.liveramp.spark_lib.SparkContextRunnable;

public class SparkTrackedOperation implements TrackedOperation {

  private static Logger LOG = LoggerFactory.getLogger(SparkTrackedOperation.class);

  private SparkContextRunnable sparkJob;
  private SparkConf conf;
  private static JavaSparkContext jvmContext;

  public SparkTrackedOperation(SparkConf conf, SparkContextRunnable sparkJob) {
    this.conf = conf;
    this.sparkJob = sparkJob;
  }

  @Override
  public void complete(JobPersister persister, boolean failOnCounterFetch) {
    LOG.info("Starting Tracked Spark Operation");
    resetContext();
    JavaSparkContext context = new JavaSparkContext(conf);
    jvmContext = context;
    List<WorkflowSparkListener> workflowListeners =
        toJava(context.sc().listenerBus().findListenersByClass(getClassTag(WorkflowSparkListener.class)));
    //there should really only ever be one, but this will be less confusing than the alternative in an error case
    for (WorkflowSparkListener workflowListener : workflowListeners) {
      workflowListener.setPersister(persister);
    }
    try {
      sparkJob.run(context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      context.close(); //This is idempotent
      for (WorkflowSparkListener workflowListener : workflowListeners) {
        workflowListener.onApplicationEnd();
      }
    }
  }

  public static <T, K extends T> ClassTag<K> getClassTag(Class<T> aClass) {
    return (ClassTag<K>)scala.reflect.ClassManifestFactory.fromClass(aClass);
  }

  public static <T> List<T> toJava(Seq<T> seq) {
    return (List<T>)JavaConverters.bufferAsJavaListConverter(seq.toBuffer()).asJava();
  }

  @Override
  public void stop() {
    //  not currently implemented
  }

  // Horrible hack to get around this: https://issues.apache.org/jira/browse/SPARK-2243.
  // Closing/stopping the context seems to reset the active context, but the stopped flag
  // is not reset, preventing any new context instances from being used.
  private static void resetContext() {
    if (jvmContext != null) {
      Class<SparkContext> contextClass = SparkContext.class;
      try {
        Field field = contextClass.getDeclaredField("stopped");
        field.setAccessible(true);
        AtomicBoolean stopped = (AtomicBoolean)field.get(jvmContext.sc());
        stopped.set(false);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }


}
