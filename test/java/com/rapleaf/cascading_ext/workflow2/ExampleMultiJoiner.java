package com.rapleaf.cascading_ext.workflow2;

import com.google.common.collect.Sets;
import com.rapleaf.cascading_ext.msj_tap.joiner.TOutputMultiJoiner;
import com.rapleaf.cascading_ext.msj_tap.merger.MSJGroup;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.IdentitySumm;
import com.rapleaf.types.new_person_data.PIN;
import com.rapleaf.types.new_person_data.PINAndOwners;
import org.apache.hadoop.io.BytesWritable;

import java.util.Iterator;
import java.util.Set;

//  add pins to summ
public class ExampleMultiJoiner implements TOutputMultiJoiner<BytesWritable, IdentitySumm> {

  @Override
  public Object join(MSJGroup<BytesWritable> group) {

    Iterator<DustinInternalEquiv> thriftIterator = group.getThriftIterator(0, new DustinInternalEquiv());
    Iterator<IdentitySumm> summIterator = group.getThriftIterator(1, new IdentitySumm());

    Set<PIN> toAdd = Sets.newHashSet();
    while (thriftIterator.hasNext()) {
      toAdd.add(thriftIterator.next().get_pin());
    }

    IdentitySumm summ = summIterator.next();
    for (PIN pin : toAdd) {
      summ.add_to_pin_and_owners(new PINAndOwners(pin));
    }

    return summ;
  }

  @Override
  public Class<IdentitySumm> getOutputType() {
    return IdentitySumm.class;
  }
}
