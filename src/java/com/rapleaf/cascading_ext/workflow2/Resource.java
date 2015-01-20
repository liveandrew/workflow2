package com.rapleaf.cascading_ext.workflow2;

public class Resource<T> {

  private final String relativeId;
  private final ActionId parent;

  protected Resource(String relativeId, ActionId parent) {
    this.relativeId = relativeId;
    this.parent = parent;
  }

  public String getRelativeId() {
    return relativeId;
  }

  public ActionId getParent() {
    return parent;
  }

  @Override
  public String toString() {
    return "Resource{" +
        "relativeId='" + relativeId + '\'' +
        ", parent=" + parent +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Resource)) {
      return false;
    }

    Resource resource = (Resource)o;

    if (parent != null ? !parent.equals(resource.parent) : resource.parent != null) {
      return false;
    }

    if (relativeId != null ? !relativeId.equals(resource.relativeId) : resource.relativeId != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = relativeId != null ? relativeId.hashCode() : 0;
    result = 31 * result + (parent != null ? parent.hashCode() : 0);
    return result;
  }
}
