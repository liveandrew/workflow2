package com.liveramp.spark_lib;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;

public interface SparkContextRunnable extends Serializable {

  void run(JavaSparkContext context) throws Exception;

}
