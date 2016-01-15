package com.rapleaf.cascading_ext.workflow2.util;

import org.apache.commons.lang3.tuple.Triple;

import com.liveramp.java_support.functional.Fn;
import com.rapleaf.cascading_ext.workflow2.Action;

public interface IdActionFactory<ID, OUTPUT> extends Fn<Triple<ID, String, OUTPUT>, Action> {
}
