package com.liveramp.cascading_ext.action;

import cascading.pipe.Pipe;
import com.rapleaf.cascading_ext.assembly.SortByThriftField;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.workflow2.CascadingAction2;
import com.rapleaf.types.new_person_data.PIN;

import java.util.Map;

public class SortPINs extends CascadingAction2 {

  public SortPINs(String checkpointToken, String tmpRoot,
                  BucketDataStore<PIN> pins,
                  BucketDataStore<PIN> pinsSorted,
                  Map<Object, Object> properties) {
    super(checkpointToken, tmpRoot, properties);

    Pipe pinsPipe = bindSource("pins", pins);
    pinsPipe = new SortByThriftField(pinsPipe, "pin");
    complete("sort", pinsPipe, pinsSorted);
  }
}
