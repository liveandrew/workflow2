package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.identity_resolver.generated.DirectedPinPair;
import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.types.new_person_data.DustinInternalEquiv;
import com.rapleaf.types.new_person_data.PIN;
import com.rapleaf.types.new_person_data.PinToPinList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestSortStoreByThriftField extends CascadingExtTestCase {
  private static DustinInternalEquiv die1 = new DustinInternalEquiv(ByteBuffer.wrap("01".getBytes()), PIN.email("email1@gmail.com"), 12);
  private static DustinInternalEquiv die2 = new DustinInternalEquiv(ByteBuffer.wrap("02".getBytes()), PIN.email("email0@gmail.com"), 11);
  private static DustinInternalEquiv die3 = new DustinInternalEquiv(ByteBuffer.wrap("03".getBytes()), PIN.email("email4@gmail.com"), 10);

  private BucketDataStore<DustinInternalEquiv> dies;
  private Properties properties;

  @Before
  public void prepare() throws IOException, TException {
    dies = bucket("dies", die1, die2, die3);

    properties = new Properties();
    properties.put("mapred.reduce.tasks", 1599);
  }

  @Test
  public void testSortByPin() throws IOException, TException, InstantiationException, IllegalAccessException {
    BucketDataStore<DustinInternalEquiv> sortedDies = builder().getDIEDataStore("sorted_dies");
    runSortDiesWorkflow(dies, DustinInternalEquiv._Fields.PIN, sortedDies);

    List<DustinInternalEquiv> sortedDiesList = HRap.getValuesFromBucket(sortedDies);

    assertEquals(sortedDiesList.size(), 3);
    assertEquals(sortedDiesList.get(0), die2);
    assertEquals(sortedDiesList.get(1), die1);
    assertEquals(sortedDiesList.get(2), die3);
  }

  @Test
  public void testSortByCreationTime() throws IOException, TException, InstantiationException, IllegalAccessException {
    BucketDataStore<DustinInternalEquiv> sortedDies = builder().getDIEDataStore("sorted_dies");
    runSortDiesWorkflow(dies, DustinInternalEquiv._Fields.CREATION_TIME, sortedDies);

    List<DustinInternalEquiv> sortedDiesList = HRap.getValuesFromBucket(sortedDies);

    assertEquals(sortedDiesList.size(), 3);
    assertEquals(sortedDiesList.get(0), die3);
    assertEquals(sortedDiesList.get(1), die2);
    assertEquals(sortedDiesList.get(2), die1);
  }

  @Test
  public void testSortByEid() throws IOException, TException, InstantiationException, IllegalAccessException {
    BucketDataStore<DustinInternalEquiv> sortedDies = builder().getDIEDataStore("sorted_dies");
    runSortDiesWorkflow(dies, DustinInternalEquiv._Fields.EID, sortedDies);

    List<DustinInternalEquiv> sortedDiesList = HRap.getValuesFromBucket(sortedDies);

    assertEquals(sortedDiesList.size(), 3);
    assertEquals(sortedDiesList.get(0), die1);
    assertEquals(sortedDiesList.get(1), die2);
    assertEquals(sortedDiesList.get(2), die3);
  }

  @Test
  public void testFailOnContainer() throws IOException, TException, InstantiationException, IllegalAccessException {
    BucketDataStore<PinToPinList> pinListStore = builder().getPinToPinListDataStore("pin_list_store");
    BucketDataStore<PinToPinList> pinListSortedStore = builder().getPinToPinListDataStore("pin_list_sorted_store");

    try {
      Step sortStoreByThriftField = new Step(new SortThriftStoreByField<PinToPinList>(
          "sort-store-by-thrift-field",
          getTestRoot(),
          pinListStore,
          pinListSortedStore,
          PinToPinList.class,
          PinToPinList._Fields.VALUES,
          properties
      ));

      execute(sortStoreByThriftField);

      fail("Workflow should not have completed.");
    } catch (RuntimeException re) {
      if (!re.getMessage().equals("Cannot sort by a container.")) {
        fail("Wrong exception.");
      }
    }
  }

  @Test
  public void testFailOnIncorrectEnum() throws IOException, TException, InstantiationException, IllegalAccessException {
    BucketDataStore<DustinInternalEquiv> sortedDies = builder().getDIEDataStore("sorted_dies");

    try {
      Step sortStoreByThriftField = new Step(new SortThriftStoreByField<DustinInternalEquiv>(
          "sort-store-by-thrift-field",
          getTestRoot(),
          dies,
          sortedDies,
          DustinInternalEquiv.class,
          DirectedPinPair._Fields.SOURCE,
          properties
      ));

      execute(sortStoreByThriftField);

      fail("Workflow should not have completed.");
    } catch (RuntimeException re) {
      if (!re.getMessage().equals("Field source is not valid for com.rapleaf.types.new_person_data.DustinInternalEquiv.")) {
        fail("Wrong exception.");
      }
    }
  }

  private void runSortDiesWorkflow(BucketDataStore<DustinInternalEquiv> inputDies, DustinInternalEquiv._Fields field, BucketDataStore<DustinInternalEquiv> outputDies) throws InstantiationException, IllegalAccessException, IOException {
    Step sortStoreByThriftField = new Step(new SortThriftStoreByField<DustinInternalEquiv>(
        "sort-store-by-thrift-field",
        getTestRoot(),
        inputDies,
        outputDies,
        DustinInternalEquiv.class,
        field,
        properties
    ));

    execute(sortStoreByThriftField);
  }
}
