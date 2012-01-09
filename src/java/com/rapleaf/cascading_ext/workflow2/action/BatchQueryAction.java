package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.relevance.Relevance;
import com.rapleaf.cascading_ext.relevance.Relevance.RelevanceFunction;
import com.rapleaf.support.datastore.DataStore;

public class BatchQueryAction extends RelevanceAction {

  private DataStore source;
  private DataStore output;
  private RelevanceFunction func;
  private DataStore keys;
  private Relevance relevance;
  private boolean useBloom;
  private boolean exact;

  public BatchQueryAction(String checkPointToken,
      Class relevanceClass,
      DataStore source,
      DataStore output,
      RelevanceFunction func,
      DataStore keys) {
    this(checkPointToken, relevanceClass, source, output, func, keys, true, true);
  }

  public BatchQueryAction(String checkPointToken,
      Class relevanceClass,
      DataStore source,
      DataStore output,
      RelevanceFunction func,
      DataStore keys,
      boolean useBloom,
      boolean exact) {
    super(checkPointToken);
    this.relevance = getRelevance(relevanceClass).setParent(this);
    this.source = source;
    this.output = output;
    this.func = func;
    this.keys = keys;
    this.useBloom = useBloom;
    this.exact = exact;

    readsFrom(source);
    readsFrom(keys);
    creates(output);
  }

  @Override
  protected void execute() throws Exception {
    relevance.batch_query(source.getTap(), output.getTap(), func, keys.getPath(), useBloom, exact);
  }
}
