package com.rapleaf.support.workflow2.action;

public enum HankVersionType {
  BASE,
  DELTA;

  public static HankVersionType fromString(String s) {
    if (s.equals("BASE")) {
      return BASE;
    } else if (s.equals("DELTA")) {
      return DELTA;
    } else {
      throw new RuntimeException("Unknown version type string: " + s);
    }
  }
}
