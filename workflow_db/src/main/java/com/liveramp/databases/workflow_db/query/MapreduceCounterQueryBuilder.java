/**
 * Autogenerated by Jack
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.liveramp.databases.workflow_db.query;

import java.util.Collection;

import com.rapleaf.jack.queries.AbstractQueryBuilder;
import com.rapleaf.jack.queries.Column;
import com.rapleaf.jack.queries.FieldSelector;
import com.rapleaf.jack.queries.where_operators.IWhereOperator;
import com.rapleaf.jack.queries.where_operators.JackMatchers;
import com.rapleaf.jack.queries.WhereConstraint;
import com.rapleaf.jack.queries.QueryOrder;
import com.rapleaf.jack.queries.OrderCriterion;
import com.rapleaf.jack.queries.LimitCriterion;
import com.liveramp.databases.workflow_db.iface.IMapreduceCounterPersistence;
import com.liveramp.databases.workflow_db.models.MapreduceCounter;


public class MapreduceCounterQueryBuilder extends AbstractQueryBuilder<MapreduceCounter> {

  public MapreduceCounterQueryBuilder(IMapreduceCounterPersistence caller) {
    super(caller);
  }

  public MapreduceCounterQueryBuilder select(MapreduceCounter._Fields... fields) {
    for (MapreduceCounter._Fields field : fields){
      addSelectedField(new FieldSelector(field));
    }
    return this;
  }

  public MapreduceCounterQueryBuilder selectAgg(FieldSelector... aggregatedFields) {
    addSelectedFields(aggregatedFields);
    return this;
  }

  public MapreduceCounterQueryBuilder id(Long value) {
    addId(value);
    return this;
  }

  public MapreduceCounterQueryBuilder idIn(Collection<Long> values) {
    addIds(values);
    return this;
  }

  public MapreduceCounterQueryBuilder whereId(IWhereOperator<Long> operator) {
    addWhereConstraint(new WhereConstraint<>(Column.fromId(null), operator, null));
    return this;
  }

  public MapreduceCounterQueryBuilder limit(int offset, int nResults) {
    setLimit(new LimitCriterion(offset, nResults));
    return this;
  }

  public MapreduceCounterQueryBuilder limit(int nResults) {
    setLimit(new LimitCriterion(nResults));
    return this;
  }

  public MapreduceCounterQueryBuilder groupBy(MapreduceCounter._Fields... fields) {
    addGroupByFields(fields);
    return this;
  }

  public MapreduceCounterQueryBuilder order() {
    this.addOrder(new OrderCriterion(QueryOrder.ASC));
    return this;
  }

  public MapreduceCounterQueryBuilder order(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(queryOrder));
    return this;
  }

  public MapreduceCounterQueryBuilder orderById() {
    this.addOrder(new OrderCriterion(QueryOrder.ASC));
    return this;
  }

  public MapreduceCounterQueryBuilder orderById(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(queryOrder));
    return this;
  }

  public MapreduceCounterQueryBuilder mapreduceJobId(Integer value) {
    addWhereConstraint(new WhereConstraint<>(MapreduceCounter._Fields.mapreduce_job_id, JackMatchers.equalTo(value)));
    return this;
  }

  public MapreduceCounterQueryBuilder whereMapreduceJobId(IWhereOperator<Integer> operator) {
    addWhereConstraint(new WhereConstraint<>(MapreduceCounter._Fields.mapreduce_job_id, operator));
    return this;
  }

  public MapreduceCounterQueryBuilder orderByMapreduceJobId() {
    this.addOrder(new OrderCriterion(MapreduceCounter._Fields.mapreduce_job_id, QueryOrder.ASC));
    return this;
  }

  public MapreduceCounterQueryBuilder orderByMapreduceJobId(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(MapreduceCounter._Fields.mapreduce_job_id, queryOrder));
    return this;
  }

  public MapreduceCounterQueryBuilder group(String value) {
    addWhereConstraint(new WhereConstraint<>(MapreduceCounter._Fields.group, JackMatchers.equalTo(value)));
    return this;
  }

  public MapreduceCounterQueryBuilder whereGroup(IWhereOperator<String> operator) {
    addWhereConstraint(new WhereConstraint<>(MapreduceCounter._Fields.group, operator));
    return this;
  }

  public MapreduceCounterQueryBuilder orderByGroup() {
    this.addOrder(new OrderCriterion(MapreduceCounter._Fields.group, QueryOrder.ASC));
    return this;
  }

  public MapreduceCounterQueryBuilder orderByGroup(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(MapreduceCounter._Fields.group, queryOrder));
    return this;
  }

  public MapreduceCounterQueryBuilder name(String value) {
    addWhereConstraint(new WhereConstraint<>(MapreduceCounter._Fields.name, JackMatchers.equalTo(value)));
    return this;
  }

  public MapreduceCounterQueryBuilder whereName(IWhereOperator<String> operator) {
    addWhereConstraint(new WhereConstraint<>(MapreduceCounter._Fields.name, operator));
    return this;
  }

  public MapreduceCounterQueryBuilder orderByName() {
    this.addOrder(new OrderCriterion(MapreduceCounter._Fields.name, QueryOrder.ASC));
    return this;
  }

  public MapreduceCounterQueryBuilder orderByName(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(MapreduceCounter._Fields.name, queryOrder));
    return this;
  }

  public MapreduceCounterQueryBuilder value(Long value) {
    addWhereConstraint(new WhereConstraint<>(MapreduceCounter._Fields.value, JackMatchers.equalTo(value)));
    return this;
  }

  public MapreduceCounterQueryBuilder whereValue(IWhereOperator<Long> operator) {
    addWhereConstraint(new WhereConstraint<>(MapreduceCounter._Fields.value, operator));
    return this;
  }

  public MapreduceCounterQueryBuilder orderByValue() {
    this.addOrder(new OrderCriterion(MapreduceCounter._Fields.value, QueryOrder.ASC));
    return this;
  }

  public MapreduceCounterQueryBuilder orderByValue(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(MapreduceCounter._Fields.value, queryOrder));
    return this;
  }
}