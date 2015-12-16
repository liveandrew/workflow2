package com.rapleaf.cascading_ext.workflow2.util;

import com.liveramp.java_support.functional.Fn;
import com.rapleaf.cascading_ext.workflow2.Action;

public interface IdActionFactory<Id> extends Fn<Id, Action> {
}
