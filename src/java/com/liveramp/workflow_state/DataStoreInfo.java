package com.liveramp.workflow_state;

//  TODO remove this class, replace usages with WorkflowAttemptDatastore
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
  public Integer getId() {
    return indexInFlow;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataStoreInfo)) {
      return false;
    }

    DataStoreInfo that = (DataStoreInfo)o;

    if (className != null ? !className.equals(that.className) : that.className != null) {
      return false;
    }
    if (indexInFlow != null ? !indexInFlow.equals(that.indexInFlow) : that.indexInFlow != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (path != null ? !path.equals(that.path) : that.path != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (className != null ? className.hashCode() : 0);
    result = 31 * result + (path != null ? path.hashCode() : 0);
    result = 31 * result + (indexInFlow != null ? indexInFlow.hashCode() : 0);
    return result;
  }
}
