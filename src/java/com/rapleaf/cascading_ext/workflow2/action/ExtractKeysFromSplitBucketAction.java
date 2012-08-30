package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.SplitBucketDataStore;
import com.rapleaf.cascading_ext.relevance.Relevance;
import com.rapleaf.cascading_ext.relevance.function.RelevanceFunction;

import java.io.IOException;
import java.util.EnumSet;

public class ExtractKeysFromSplitBucketAction extends RelevanceAction{

  private final SplitBucketDataStore source;
  private final EnumSet selectedFields;
  private final BucketDataStore output;
  private final String outField;
  private final RelevanceFunction relevanceFunction;
  private final Relevance relevance;
  
  public ExtractKeysFromSplitBucketAction(String checkpointToken,
      SplitBucketDataStore source,
      EnumSet selectedFields,
      BucketDataStore output,
      String outField,
      RelevanceFunction func,
      Class type) {
    super(checkpointToken);
    
    this.source = source;
    this.selectedFields = selectedFields;
    this.output = output;
    this.outField = outField;
    this.relevanceFunction = func;
    this.relevance = getRelevance(type);
    
    readsFrom(source);
    creates(output);
  }

  @Override
  protected void execute() throws IOException {
    relevance.setParent(this).extract_keys(source.getTap(selectedFields), output.getTap(), outField, relevanceFunction);
  }
}
