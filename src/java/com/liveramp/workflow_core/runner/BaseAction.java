package com.liveramp.workflow_core.runner;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.liveramp.java_support.workflow.ActionId;

public abstract class BaseAction {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAction.class);

  private final ActionId actionId;

  private OverridableProperties stepProperties;
  private OverridableProperties combinedProperties;


  public BaseAction(String checkpointToken, Map<Object, Object> properties){
    this.actionId = new ActionId(checkpointToken);
    this.stepProperties = new NestedProperties(properties, false);

  }

  public ActionId getActionId() {
    return actionId;
  }

  public String fullId() {
    return actionId.resolve();
  }


  // Don't call this method directly!
  protected abstract void execute() throws Exception;

  protected void preExecute() throws Exception {
    //  no op
  }

  //  either fail or succeed
  protected void onFinish(){

  }

  protected OverridableProperties getCombinedProperties(){
    return combinedProperties;
  }

  protected OverridableProperties getStepProperties(){
    return stepProperties;
  }

  //  not really public : /
  public final void internalExecute(OverridableProperties parentProperties) {

    try {

      combinedProperties = stepProperties.override(parentProperties);

      preExecute();

      execute();

    } catch (Throwable t) {
      LOG.error("Action " + fullId() + " failed due to Throwable", t);
      throw wrapRuntimeException(t);
    } finally {

      onFinish();

    }
  }

  private static RuntimeException wrapRuntimeException(Throwable t) {
    return (t instanceof RuntimeException) ? (RuntimeException)t : new RuntimeException(t);
  }


  @Override
  public String toString() {
    return getClass().getSimpleName() + " [checkpointToken=" + getActionId().getRelativeName() + "]";
  }

}
