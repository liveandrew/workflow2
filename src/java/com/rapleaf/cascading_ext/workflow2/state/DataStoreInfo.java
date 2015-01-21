package com.rapleaf.cascading_ext.workflow2.state;

public class DataStoreInfo {

  private final String name;
  private final String className;
  private final String path;
  private final Integer indexInFlow;

  public DataStoreInfo(String name, String className, String path, Integer indexInFlow) {
    this.name = name;
    this.className = className;
    this.path = path;
    this.indexInFlow = indexInFlow;
  }

  public String getName() {
    return name;
  }
  public String getClassName() {
    return className;
  }
  public String getPath() {
    return path;
  }
  public Integer getIndexInFlow() {
    return indexInFlow;
  }
}
