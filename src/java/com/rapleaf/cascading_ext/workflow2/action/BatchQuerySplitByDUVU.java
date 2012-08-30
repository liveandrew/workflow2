package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.SplitBucketDataStore;
import com.rapleaf.cascading_ext.relevance.Relevance;
import com.rapleaf.cascading_ext.relevance.function.RelevanceFunction;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.types.new_person_data.DataUnitValueUnion;

import java.util.EnumSet;
import java.util.Set;

public class BatchQuerySplitByDUVU extends Action {

  private final SplitBucketDataStore<DataUnitValueUnion._Fields> splitStore;
  private final BucketDataStore eids;
  private final BucketDataStore relevantStore;
  private final Set<DataUnitValueUnion._Fields> fields;

  private final RelevanceFunction relevanceFunction;
  private final Relevance relevance;

  public BatchQuerySplitByDUVU(String checkpointToken, Relevance relevance, RelevanceFunction relevanceFunction,
                               SplitBucketDataStore splitStore, BucketDataStore eids, BucketDataStore relevantSplitStore) {
    this(checkpointToken, relevance, relevanceFunction, splitStore, EnumSet.allOf(DataUnitValueUnion._Fields.class),  eids, relevantSplitStore);
  }

  public BatchQuerySplitByDUVU(String checkpointToken, Relevance relevance, RelevanceFunction relevanceFunction, SplitBucketDataStore splitStore, Set<DataUnitValueUnion._Fields> fields, BucketDataStore eids, BucketDataStore relevantSplitStore) {
    super(checkpointToken);

    this.relevanceFunction = relevanceFunction;
    this.splitStore = splitStore;
    this.eids = eids;
    this.fields = fields;
    this.relevantStore = relevantSplitStore;
    this.relevance = relevance;

    readsFrom(eids);
    readsFrom(splitStore);
    creates(relevantSplitStore);
  }

  @Override
  protected void execute() throws Exception {
    relevance.setParent(this).batch_query(splitStore.getTap(fields), relevantStore.getTap(), relevanceFunction, eids.getPath(), true, true);
  }
}
