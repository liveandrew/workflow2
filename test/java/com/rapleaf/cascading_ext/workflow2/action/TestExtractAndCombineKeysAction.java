package com.rapleaf.support.workflow2.action;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.rapleaf.cascading_ext.relevance.Relevance.RelevanceFunction;
import com.rapleaf.cascading_ext.relevance.RelevanceConstants;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.formats.test.TupleDataStoreHelper;
import com.rapleaf.java_support.JavaSupportTestCase;
import com.rapleaf.support.Rap;
import com.rapleaf.support.datastore.BucketDataStore;
import com.rapleaf.support.datastore.BucketDataStoreImpl;
import com.rapleaf.support.datastore.TupleDataStore;
import com.rapleaf.support.datastore.TupleDataStoreImpl;
import com.rapleaf.support.workflow2.Step;
import com.rapleaf.support.workflow2.WorkflowRunner;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.PIN;

public class TestExtractAndCombineKeysAction extends JavaSupportTestCase{

  private BucketDataStore store1;
  private BucketDataStore store2;
  private TupleDataStore store3;
  private BucketDataStore output;
  
  @Override
  public void setUp() throws Exception{
    super.setUp();
    
    store1 = new BucketDataStoreImpl(getFS(), "store1", getTestRoot(), "/store1", DustinInternalEquiv.class);
    store2 = new BucketDataStoreImpl(getFS(), "store2", getTestRoot(), "/store2", DustinInternalEquiv.class);
    store3 = new TupleDataStoreImpl("store3", getTestRoot(), "/store3", new Fields("die"));
    
    ThriftBucketHelper.writeToBucket(store1.getBucket(), 
        new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN.email("dude1@gmail.com"), 1),
        new DustinInternalEquiv(ByteBuffer.wrap("2".getBytes()), PIN.email("dude2@gmail.com"), 1)
    );

    ThriftBucketHelper.writeToBucket(store2.getBucket(), 
        new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN.email("dude1@gmail.com"), 1),
        new DustinInternalEquiv(ByteBuffer.wrap("2".getBytes()), PIN.email("dude3@gmail.com"), 1)
    );

    TupleDataStoreHelper.writeToStore(store3, 
        new Tuple(new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN.email("dude1@gmail.com"), 1)),
        new Tuple(new DustinInternalEquiv(ByteBuffer.wrap("1".getBytes()), PIN.email("dude1@gmail.com"), 1))
    );
    
    output = new BucketDataStoreImpl(getFS(), "output", getTestRoot(), "/output", PIN.class);
  }
  
  public void testIt() throws IOException{
    
    new WorkflowRunner("combine and distinct", getTestRoot()+"/tmp", 2, 32432, 
        new Step(new ExtractAndCombineKeysAction("extract-combine-keys", getTestRoot(), Arrays.asList(store1, store2, store3), 
            output, "pin", new PINFromInternalEquiv(), PIN.class))).run();
    
    List<PIN> pins = Rap.getValuesFromBucket(output);
    
    assertEquals(3, pins.size());
    assertCollectionContains(pins, PIN.email("dude1@gmail.com"));
    assertCollectionContains(pins, PIN.email("dude2@gmail.com"));
    assertCollectionContains(pins, PIN.email("dude3@gmail.com"));
  }
  
  //  alas, dustin common can't be referenced here :( 
  public static class PINFromInternalEquiv extends RelevanceFunction<PIN> {

    protected int getAverageMatchSize() {
      return RelevanceConstants.AVERAGE_PIN_SIZE;
    }
    
    protected Fields getRequiredFields(){
      return new Fields("die");
    }

    public Set<PIN> extractPotentialMatches(TupleEntry tuple) {
      DustinInternalEquiv die = (DustinInternalEquiv) tuple.get("die");
      return Collections.singleton(die.get_pin());
    }

    @Override
    protected int getAverageRequiredFieldsSize() {
      return RelevanceConstants.AVERAGE_DIE_SIZE;
    }
  }
}
