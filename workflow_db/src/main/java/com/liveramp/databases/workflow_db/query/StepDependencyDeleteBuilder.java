/**
 * Autogenerated by Jack
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.liveramp.databases.workflow_db.query;

import java.util.Collection;

import com.rapleaf.jack.queries.AbstractDeleteBuilder;
import com.rapleaf.jack.queries.where_operators.IWhereOperator;
import com.rapleaf.jack.queries.where_operators.JackMatchers;
import com.rapleaf.jack.queries.WhereConstraint;
import com.liveramp.databases.workflow_db.iface.IStepDependencyPersistence;
import com.liveramp.databases.workflow_db.models.StepDependency;


public class StepDependencyDeleteBuilder extends AbstractDeleteBuilder<StepDependency> {

  public StepDependencyDeleteBuilder(IStepDependencyPersistence caller) {
    super(caller);
  }

  public StepDependencyDeleteBuilder id(Long value) {
    addId(value);
    return this;
  }

  public StepDependencyDeleteBuilder idIn(Collection<Long> values) {
    addIds(values);
    return this;
  }

  public StepDependencyDeleteBuilder stepAttemptId(Long value) {
    addWhereConstraint(new WhereConstraint<Long>(StepDependency._Fields.step_attempt_id, JackMatchers.equalTo(value)));
    return this;
  }

  public StepDependencyDeleteBuilder whereStepAttemptId(IWhereOperator<Long> operator) {
    addWhereConstraint(new WhereConstraint<Long>(StepDependency._Fields.step_attempt_id, operator));
    return this;
  }

  public StepDependencyDeleteBuilder dependencyAttemptId(Long value) {
    addWhereConstraint(new WhereConstraint<Long>(StepDependency._Fields.dependency_attempt_id, JackMatchers.equalTo(value)));
    return this;
  }

  public StepDependencyDeleteBuilder whereDependencyAttemptId(IWhereOperator<Long> operator) {
    addWhereConstraint(new WhereConstraint<Long>(StepDependency._Fields.dependency_attempt_id, operator));
    return this;
  }
}