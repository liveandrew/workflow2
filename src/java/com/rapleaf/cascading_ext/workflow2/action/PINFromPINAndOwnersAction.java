package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.map_side_join.Joiner;
import com.rapleaf.cascading_ext.map_side_join.MapSideJoin;
import com.rapleaf.cascading_ext.map_side_join.extractors.TBinaryExtractor;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.types.new_person_data.PIN;
import com.rapleaf.types.new_person_data.PINAndOwners;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class PINFromPINAndOwnersAction extends Action {
  BucketDataStore pinAndOwnerSets;
  BucketDataStore pins;

  public PINFromPINAndOwnersAction(String checkpointToken,
                                   String tmpRoot,
                                   BucketDataStore pinAndOwnerSets,
                                   BucketDataStore pins) {
    super(checkpointToken, tmpRoot);
    this.pinAndOwnerSets = pinAndOwnerSets;
    this.pins = pins;
  }

  @Override
  protected void execute() throws Exception {

    MapSideJoin<BytesWritable> joiner = new MapSideJoin<BytesWritable>(
        Arrays.<Extractor<BytesWritable>>asList(new TBinaryExtractor(PIN.class, PINAndOwners._Fields.PIN)),
        new PINExtractorJoiner(),
        Arrays.asList(pinAndOwnerSets),
        pins);
    joiner.run();

    pins.getBucket().markAsMutable();
  }

  private static class PINExtractorJoiner extends Joiner<BytesWritable> {

    public void join() throws IOException {
      Iterator<PINAndOwners> itr = getThriftIterator(0, new PINAndOwners());
      while (itr.hasNext()) {
        emit(itr.next().get_pin());
      }
    }
  }
}

