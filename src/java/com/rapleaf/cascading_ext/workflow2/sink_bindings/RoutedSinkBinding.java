package com.rapleaf.cascading_ext.workflow2.sink_bindings;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.thrift.TBase;

import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.liveramp.cascading_ext.fields.SingleField;
import com.liveramp.cascading_ext.util.FieldHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.UnitDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.PartitionableDataStore;
import com.rapleaf.cascading_ext.tap.FieldRemap;
import com.rapleaf.cascading_ext.tap.PartitionedSinkTapFactory;
import com.rapleaf.cascading_ext.tap.RoutingSinkTap;
import com.rapleaf.cascading_ext.tap.TapFactory;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.SinkBinding;

public class RoutedSinkBinding implements SinkBinding {

  private final Pipe pipe;
  private final String routeField;
  private final String valueField;
  private final Map<Object, TapFactory> routeToTapFactories;
  private final Map<Object, String> routeToSinkFields;
  private Set<DataStore> dataStores;

  public RoutedSinkBinding(Pipe pipe, String routeField, String valueField) {
    this.pipe = pipe;
    this.routeField = routeField;
    this.valueField = valueField;
    this.routeToTapFactories = new HashMap<>();
    this.routeToSinkFields = new HashMap<>();
    this.dataStores = new HashSet<>();
  }

  public RoutedSinkBinding(Pipe pipe, SingleField<String> routeField, SingleField<String> valueField) {
    this(pipe, routeField.getName(), valueField.getName());
  }

  public RoutedSinkBinding add(Object route, String sinkField, DataStore store, TapFactory tapFactory) {
    if (routeToTapFactories.containsKey(route)) {
      throw new IllegalArgumentException("Route " + route + " is already mapped to tap " + routeToTapFactories.get(route));
    }

    if (routeToSinkFields.containsKey(route)) {
      throw new IllegalArgumentException("Route " + route + " is already mapped to sink field " + routeToTapFactories.get(route));
    }

    if (dataStores.contains(store)) {
      throw new IllegalArgumentException("Store " + store + " is already going to be locked.");
    }

    this.routeToTapFactories.put(route, tapFactory);
    this.routeToSinkFields.put(route, sinkField);
    this.dataStores.add(store);

    return this;
  }

  public RoutedSinkBinding add(Object route, String sinkField, DataStore store, Tap tap) {
    return this.add(route, sinkField, store, new TapFactory.Wrapping(tap));
  }

  public <T> RoutedSinkBinding add(Object route, Class<T> klass, UnitDataStore<T> store) {
    return this.add(route, FieldHelper.fieldNameOf(klass), store);
  }

  public RoutedSinkBinding add(Object route, String sinkField, DataStore store) {
    return this.add(route, sinkField, store, new TapFactory.SimpleFactory(store));
  }

  public RoutedSinkBinding add(Object route, String sinkField, PartitionableDataStore<? extends TBase> pds, PartitionStructure pStructure) {
    return this.add(route, sinkField, pds, new PartitionedSinkTapFactory<>(pds, pStructure));
  }

  public List<DataStore> getOutputStores() {
    return Lists.newArrayList(dataStores);
  }

  @Override
  public Pipe getPipe() {
    return pipe;
  }

  @Override
  public TapFactory getTapFactory() {
    return new RoutingSinkTapFactory(routeField,
        valueField,
        routeToTapFactories,
        routeToSinkFields);
  }

  private static class RoutingSinkTapFactory extends TapFactory {

    private final String routeField;
    private final String valueField;
    private final Map<Object, TapFactory> routeToTapFactory;
    private final Map<Object, String> routeToSinkField;

    public RoutingSinkTapFactory(String routeField, String valueField, Map<Object, TapFactory> routeToTapFactory, Map<Object, String> routeToSinkField) {
      this.routeField = routeField;
      this.valueField = valueField;
      this.routeToTapFactory = routeToTapFactory;
      this.routeToSinkField = routeToSinkField;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tap createTap() throws IOException {
      return new RoutingSinkTap<>(
          routeField,
          Maps.newHashMap(Maps.transformEntries(this.routeToTapFactory, new Maps.EntryTransformer<Object, TapFactory, Tap>() {
            @Override
            public Tap transformEntry(Object o, TapFactory tapFactory) {
              try {
                return tapFactory.createTap();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          })),
          new FieldRemap.FieldRouteRemap(
              valueField,
              routeToSinkField
          )
      );
    }
  }

}
