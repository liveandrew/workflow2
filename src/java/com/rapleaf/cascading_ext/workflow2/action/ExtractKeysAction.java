package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.relevance.Relevance;
import com.rapleaf.cascading_ext.relevance.Relevance.ExtractKeysStats;
import com.rapleaf.cascading_ext.relevance.Relevance.RelevanceFunction;


public class ExtractKeysAction extends RelevanceAction {
  private final DataStore source;
  private final BucketDataStore output;
  private final String outField;
  private final RelevanceFunction relevanceFunction;
  private final Relevance relevance;
  private final Map<Object, Object> userProperties;

  public ExtractKeysAction(String checkpointToken,
      DataStore source,
      BucketDataStore output,
      String outField,
      RelevanceFunction func,
      Class type) {
    this(checkpointToken, source, output, outField, func, type, Collections.emptyMap());
  }

  public ExtractKeysAction(String checkpointToken,
      DataStore source,
      BucketDataStore output,
      String outField,
      RelevanceFunction func) {
    this(checkpointToken, source, output, outField, func, byte[].class, Collections.emptyMap());
  }

  public ExtractKeysAction(String checkpointToken,
      DataStore source,
      BucketDataStore output,
      String outField,
      RelevanceFunction func,
      Map<Object, Object> userProperties) {
    this(checkpointToken, source, output, outField, func, byte[].class, userProperties);
  }

  public ExtractKeysAction(String checkpointToken,
      DataStore source,
      BucketDataStore output,
      String outField,
      RelevanceFunction func,
      Class type,
      Map<Object, Object> userProperties) {
    super(checkpointToken);
    this.source = source;
    this.output = output;
    this.outField = outField;
    this.relevanceFunction = func;
    this.userProperties = userProperties;
    this.relevance = getRelevance(type);

    readsFrom(source);
    creates(output);
  }

  public ExtractKeysStats getExtractKeysStats() {
    return relevance.setParent(this).getExtractKeysStats();
  }

  @Override
  protected void execute() throws IOException {
    relevance.setParent(this).extract_keys(source.getTap(), output.getTap(), outField,
      relevanceFunction, userProperties);
  }
}
