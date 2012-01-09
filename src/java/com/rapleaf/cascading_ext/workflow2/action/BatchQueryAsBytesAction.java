package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.relevance.Relevance.RelevanceFunction;


public class BatchQueryAsBytesAction extends RelevanceAction {

  private final BucketDataStore source;
  private final BucketDataStore output;
  private final RelevanceFunction func;
  private final DataStore keys;
  private final boolean useBloom;
  private final boolean exact;
  private final String field;

  public BatchQueryAsBytesAction(String checkpointToken,
      BucketDataStore source,
      BucketDataStore output,
      RelevanceFunction func,
      DataStore keys) {
    this(checkpointToken, source, output, func, keys, true, true);
  }

  public BatchQueryAsBytesAction(String checkpointToken,
      BucketDataStore source,
      BucketDataStore output,
      RelevanceFunction func,
      DataStore keys,
      boolean useBloom,
      boolean exact) {
    this(checkpointToken, source, output, func, keys, useBloom, exact, "bytes");
  }

  public BatchQueryAsBytesAction(String checkpointToken,
      BucketDataStore source,
      BucketDataStore output,
      RelevanceFunction func,
      DataStore keys,
      boolean useBloom,
      boolean exact,
      String field) {
    super(checkpointToken);
    this.source = source;
    this.output = output;
    this.func = func;
    this.keys = keys;
    this.useBloom = useBloom;
    this.exact = exact;
    this.field = field;

    readsFrom(source);
    readsFrom(keys);
    creates(output);
  }

  @Override
  protected void execute() throws Exception {
    getRelevance(byte[].class).setParent(this).batch_query(source.getTapAsBytes(field),
      output.getTapAsBytes(field), func, keys.getPath(), useBloom, exact);
  }

}
