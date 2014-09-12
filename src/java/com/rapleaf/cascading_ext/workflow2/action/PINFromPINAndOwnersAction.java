package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;

import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.extractors.TBinaryExtractor;
import com.rapleaf.cascading_ext.msj_tap.merger.MSJGroup;
import com.rapleaf.cascading_ext.msj_tap.operation.MSJFunction;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;
import com.rapleaf.types.new_person_data.PIN;
import com.rapleaf.types.new_person_data.PINAndOwners;

public class PINFromPINAndOwnersAction extends MSJTapAction<BytesWritable> {

  public PINFromPINAndOwnersAction(
      String checkpointToken,
      String tmpRoot,
      MapSideJoinableDataStore pinAndOwnerSets,
      BucketDataStore pins) {
    super(checkpointToken, tmpRoot,
        new ExtractorsList<BytesWritable>()
            .add(pinAndOwnerSets, new TBinaryExtractor<PIN>(PIN.class, PINAndOwners._Fields.PIN)),
        new PINExtractorJoiner(),
        pins
    );
  }

  private static class PINExtractorJoiner extends MSJFunction<BytesWritable> {

    private PINExtractorJoiner() {
      super(new Fields("pin"));
    }

    @Override
    public void operate(FunctionCall functionCall, MSJGroup<BytesWritable> group) {
      Iterator<PINAndOwners> itr = group.getThriftIterator(0, new PINAndOwners());
      while (itr.hasNext()) {
        functionCall.getOutputCollector().add(new Tuple(itr.next().get_pin()));
      }
    }
  }
}

