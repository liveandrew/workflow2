package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import org.apache.thrift.TException;
import org.junit.Test;

import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.liveramp.cascading_ext.util.FieldHelper;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.PartitionedDataStore;
import com.rapleaf.cascading_ext.function.DirectFunction;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionedBucketScheme;
import com.rapleaf.cascading_ext.workflow2.sink_bindings.RoutedSinkBinding;
import com.rapleaf.cascading_ext.workflow2.sink_bindings.TypedSink;
import com.rapleaf.formats.test.ThriftBucketHelper;
import com.rapleaf.types.new_person_data.AbiliTecId;
import com.rapleaf.types.new_person_data.DataPartnerPIN;
import com.rapleaf.types.new_person_data.GroupPIN;
import com.rapleaf.types.new_person_data.Household;
import com.rapleaf.types.new_person_data.NAPkin;
import com.rapleaf.types.new_person_data.NameAndCity;
import com.rapleaf.types.new_person_data.PIN;
import com.rapleaf.types.person_data.DataPartner;

public class TestRoutedSinkBinding extends WorkflowTestCase {
  @Test
  public void testRoutedSink() throws IOException, TException {
    final BucketDataStore<PIN> pins = builder().getBucketDataStore("pins", PIN.class);
    final NAPkin inNap = new NAPkin().set_first_name("Roshan");
    final AbiliTecId inAb = new AbiliTecId("val");
    final DataPartnerPIN inDp = new DataPartnerPIN(DataPartner.DATRAN, "1");
    final NameAndCity inNac = new NameAndCity("roshan", "george", "san francisco", "ca");
    final Household inHh = new Household().set_zip("94105");
    ThriftBucketHelper.writeToBucket(pins.getBucket(), PIN.abilitec(inAb), PIN.napkin(inNap),
        PIN.data_partner_pin(inDp),
        PIN.group(GroupPIN.household(inHh)), PIN.group(GroupPIN.name_and_city(inNac)));

    final BucketDataStore<NAPkin> nap = builder().getBucketDataStore("nap", NAPkin.class);
    final BucketDataStore<AbiliTecId> abilitec = builder().getBucketDataStore("abilitec", AbiliTecId.class);
    final BucketDataStore<DataPartnerPIN> dapar = builder().getBucketDataStore("dapar", DataPartnerPIN.class);
    final PartitionedDataStore<GroupPIN> partGroup = builder().getPartitionedDataStore("part-group", GroupPIN.class);

    final SetMultimap<String, String> structureMap = HashMultimap.create();
    final String nacSplit = "split" + GroupPIN._Fields.NAME_AND_CITY;
    final String hhSplit = "split" + GroupPIN._Fields.HOUSEHOLD;
    final String nacPart = "part" + GroupPIN._Fields.NAME_AND_CITY;
    final String hhPart = "part" + GroupPIN._Fields.HOUSEHOLD;
    structureMap.put(nacSplit, nacPart);
    structureMap.put(hhSplit, hhPart);

    // Twice to trigger enforcement of creates
    execute(new TypedRoutedSinkAction("test-routed", getTestRoot(), pins, nap, abilitec, dapar, partGroup, new PartitionStructure.FromMap(structureMap)));
    execute(new TypedRoutedSinkAction("test-routed", getTestRoot(), pins, nap, abilitec, dapar, partGroup, new PartitionStructure.FromMap(structureMap)));

    final List<NAPkin> naps = ThriftBucketHelper.readBucket(nap.getBucket(), NAPkin.class);
    final List<AbiliTecId> abiliTecIds = ThriftBucketHelper.readBucket(abilitec.getBucket(), AbiliTecId.class);
    final List<DataPartnerPIN> dataPartnerPINs = ThriftBucketHelper.readBucket(dapar.getBucket(), DataPartnerPIN.class);
    final List<NameAndCity> nameAndCities = ThriftBucketHelper.readBucket(fs, partGroup.getRoot() + "/" + nacSplit, NameAndCity.class);
    final List<Household> households = ThriftBucketHelper.readBucket(fs, partGroup.getRoot() + "/" + hhSplit, Household.class);
    assertCollectionEquivalent(Lists.newArrayList(inNap), naps);
    assertCollectionEquivalent(Lists.newArrayList(inAb), abiliTecIds);
    assertCollectionEquivalent(Lists.newArrayList(inDp), dataPartnerPINs);
    assertCollectionEquivalent(Lists.newArrayList(inNac), nameAndCities);
    assertCollectionEquivalent(Lists.newArrayList(inHh), households);
  }

  private static class TypedRoutedSinkAction extends CascadingAction2 {

    public static final String TYPE = "type";
    public static final String VALUE = "value";

    public TypedRoutedSinkAction(String checkpointToken, String tmpRoot,
                                 BucketDataStore<PIN> in,
                                 BucketDataStore<NAPkin> nap,
                                 BucketDataStore<AbiliTecId> abilitec,
                                 BucketDataStore<DataPartnerPIN> dapar,
                                 PartitionedDataStore<GroupPIN> partGroup,
                                 PartitionStructure.FromMap groupPartitioner) {
      super(checkpointToken, tmpRoot);

      Pipe pipe = bindSource("in-pins", in);
      pipe = new Each(pipe, FieldHelper.fieldOf(PIN.class), new Split(new Fields(TYPE, VALUE)));

      complete("name", Lists.newArrayList(new RoutedSinkBinding(
          pipe,
          TYPE,
          VALUE)
          .add(PIN._Fields.GROUP, FieldHelper.fieldNameOf(GroupPIN.class), partGroup, groupPartitioner)
          .add(PIN._Fields.DATA_PARTNER_PIN, "data_partner_pin", dapar, dapar.getTap())
          .add(PIN._Fields.ABILITEC, FieldHelper.fieldNameOf(AbiliTecId.class), abilitec)
          .add(PIN._Fields.NAPKIN, TypedSink.of(nap, NAPkin.class))));
    }

    private static class Split extends DirectFunction<PIN> {

      public Split(Fields outputFields) {
        super(outputFields.append(new Fields(PartitionedBucketScheme.SPLIT_FIELD)).append(new Fields(PartitionedBucketScheme.VIRTUAL_PARTITION_FIELD)));
      }

      @Override
      public void operate(PIN value, FunctionCall call) {
        if (value.getSetField() == PIN._Fields.GROUP) {
          emit(call, value.getSetField(), value.get_group().getFieldValue(), "split"+value.get_group().getSetField(), "part"+value.get_group().getSetField());
        } else {
          emit(call, value.getSetField(), value.getFieldValue());
        }
      }
    }
  }

}
