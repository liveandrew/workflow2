package com.rapleaf.cascading_ext.workflow2;

public class Resource<T> {

  private final String id;

  protected Resource(String id) {
    this.id = id;
  }
  
  public String getId() {
    return id;
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

    if (id != null ? !id.equals(resource.id) : resource.id != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }
}
