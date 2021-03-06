
/**
 * Autogenerated by Jack
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.liveramp.databases.workflow_db.models;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.rapleaf.jack.AssociationType;
import com.rapleaf.jack.AttributesWithId;
import com.rapleaf.jack.BelongsToAssociation;
import com.rapleaf.jack.DefaultAssociationMetadata;
import com.rapleaf.jack.HasManyAssociation;
import com.rapleaf.jack.HasOneAssociation;
import com.rapleaf.jack.IAssociationMetadata;
import com.rapleaf.jack.IModelAssociationMetadata;
import com.rapleaf.jack.ModelIdWrapper;
import com.rapleaf.jack.ModelWithId;
import com.rapleaf.jack.queries.AbstractTable;
import com.rapleaf.jack.queries.Column;

import com.liveramp.databases.workflow_db.IDatabases;
import com.rapleaf.jack.util.JackUtility;

public class BackgroundWorkflowExecutorInfo extends ModelWithId<BackgroundWorkflowExecutorInfo, IDatabases> implements Comparable<BackgroundWorkflowExecutorInfo>{
  
  public static final long serialVersionUID = 6214690017522203866L;

  public static class Tbl extends AbstractTable<BackgroundWorkflowExecutorInfo.Attributes, BackgroundWorkflowExecutorInfo> {
    public final Column<Long> ID;
    public final Column<String> HOST;
    public final Column<Integer> STATUS;
    public final Column<Long> LAST_HEARTBEAT;
    public final Column<Long> LAST_HEARTBEAT_EPOCH;

    private Tbl(String alias) {
      super("background_workflow_executor_infos", alias, BackgroundWorkflowExecutorInfo.Attributes.class, BackgroundWorkflowExecutorInfo.class);
      this.ID = Column.fromId(alias);
      this.HOST = Column.fromField(alias, _Fields.host, String.class);
      this.STATUS = Column.fromField(alias, _Fields.status, Integer.class);
      this.LAST_HEARTBEAT = Column.fromTimestamp(alias, _Fields.last_heartbeat);
      this.LAST_HEARTBEAT_EPOCH = Column.fromField(alias, _Fields.last_heartbeat_epoch, Long.class);
      Collections.addAll(this.allColumns, ID, HOST, STATUS, LAST_HEARTBEAT, LAST_HEARTBEAT_EPOCH);
    }

    public static Tbl as(String alias) {
      return new Tbl(alias);
    }
  }

  public static final Tbl TBL = new Tbl("background_workflow_executor_infos");
  public static final Column<Long> ID = TBL.ID;
  public static final Column<String> HOST = TBL.HOST;
  public static final Column<Integer> STATUS = TBL.STATUS;
  public static final Column<Long> LAST_HEARTBEAT = TBL.LAST_HEARTBEAT;
  public static final Column<Long> LAST_HEARTBEAT_EPOCH = TBL.LAST_HEARTBEAT_EPOCH;

  private final Attributes attributes;

  private transient BackgroundWorkflowExecutorInfo.Id cachedTypedId;

  // Associations
  private HasManyAssociation<BackgroundStepAttemptInfo> __assoc_background_step_attempt_info;

  public enum _Fields {
    host,
    status,
    last_heartbeat,
    last_heartbeat_epoch,
  }

  @Override
  public BackgroundWorkflowExecutorInfo.Id getTypedId() {
    if (cachedTypedId == null) {
      cachedTypedId = new BackgroundWorkflowExecutorInfo.Id(this.getId());
    }
    return cachedTypedId;
  }

  public BackgroundWorkflowExecutorInfo(long id, final String host, final int status, final long last_heartbeat, final Long last_heartbeat_epoch, IDatabases databases) {
    super(databases);
    attributes = new Attributes(id, host, status, last_heartbeat, last_heartbeat_epoch);
    this.__assoc_background_step_attempt_info = new HasManyAssociation<>(databases.getWorkflowDb().backgroundStepAttemptInfos(), "background_workflow_executor_info_id", getId());
  }

  public BackgroundWorkflowExecutorInfo(long id, final String host, final int status, final long last_heartbeat, final Long last_heartbeat_epoch) {
    super(null);
    attributes = new Attributes(id, host, status, last_heartbeat, last_heartbeat_epoch);
  }
  
  public BackgroundWorkflowExecutorInfo(long id, final String host, final int status, final long last_heartbeat, IDatabases databases) {
    super(databases);
    attributes = new Attributes(id, host, status, last_heartbeat);
    this.__assoc_background_step_attempt_info = new HasManyAssociation<>(databases.getWorkflowDb().backgroundStepAttemptInfos(), "background_workflow_executor_info_id", getId());
  }

  public BackgroundWorkflowExecutorInfo(long id, final String host, final int status, final long last_heartbeat) {
    super(null);
    attributes = new Attributes(id, host, status, last_heartbeat);
  }

  public static BackgroundWorkflowExecutorInfo newDefaultInstance(long id) {
    return new BackgroundWorkflowExecutorInfo(id, "", 0, 0L);
  }

  public BackgroundWorkflowExecutorInfo(Attributes attributes, IDatabases databases) {
    super(databases);
    this.attributes = attributes;

    if (databases != null) {
      this.__assoc_background_step_attempt_info = new HasManyAssociation<>(databases.getWorkflowDb().backgroundStepAttemptInfos(), "background_workflow_executor_info_id", getId());
    }
  }

  public BackgroundWorkflowExecutorInfo(Attributes attributes) {
    this(attributes, (IDatabases) null);
  }

  public BackgroundWorkflowExecutorInfo(long id, Map<Enum, Object> fieldsMap) {
    super(null);
    attributes = new Attributes(id, fieldsMap);
  }

  public BackgroundWorkflowExecutorInfo (BackgroundWorkflowExecutorInfo other) {
    this(other, (IDatabases)null);
  }

  public BackgroundWorkflowExecutorInfo (BackgroundWorkflowExecutorInfo other, IDatabases databases) {
    super(databases);
    attributes = new Attributes(other.getAttributes());

    if (databases != null) {
      this.__assoc_background_step_attempt_info = new HasManyAssociation<>(databases.getWorkflowDb().backgroundStepAttemptInfos(), "background_workflow_executor_info_id", getId());
    }
  }

  public Attributes getAttributes() {
    return attributes;
  }

  public String getHost() {
    return attributes.getHost();
  }

  public BackgroundWorkflowExecutorInfo setHost(String newval) {
    attributes.setHost(newval);
    cachedHashCode = 0;
    return this;
  }

  public int getStatus() {
    return attributes.getStatus();
  }

  public BackgroundWorkflowExecutorInfo setStatus(int newval) {
    attributes.setStatus(newval);
    cachedHashCode = 0;
    return this;
  }

  public long getLastHeartbeat() {
    return attributes.getLastHeartbeat();
  }

  public BackgroundWorkflowExecutorInfo setLastHeartbeat(long newval) {
    attributes.setLastHeartbeat(newval);
    cachedHashCode = 0;
    return this;
  }

  public Long getLastHeartbeatEpoch() {
    return attributes.getLastHeartbeatEpoch();
  }

  public BackgroundWorkflowExecutorInfo setLastHeartbeatEpoch(Long newval) {
    attributes.setLastHeartbeatEpoch(newval);
    cachedHashCode = 0;
    return this;
  }

  public void setField(_Fields field, Object value) {
    switch (field) {
      case host:
        setHost((String)value);
        break;
      case status:
        setStatus((Integer)value);
        break;
      case last_heartbeat:
        setLastHeartbeat((Long)value);
        break;
      case last_heartbeat_epoch:
        setLastHeartbeatEpoch((Long)value);
        break;
      default:
        throw new IllegalStateException("Invalid field: " + field);
    }
  }
  
  public void setField(String fieldName, Object value) {
    if (fieldName.equals("host")) {
      setHost((String)  value);
      return;
    }
    if (fieldName.equals("status")) {
      setStatus((Integer)  value);
      return;
    }
    if (fieldName.equals("last_heartbeat")) {
      setLastHeartbeat((Long)  value);
      return;
    }
    if (fieldName.equals("last_heartbeat_epoch")) {
      setLastHeartbeatEpoch((Long)  value);
      return;
    }
    throw new IllegalStateException("Invalid field: " + fieldName);
  }

  public static Class getFieldType(_Fields field) {
    switch (field) {
      case host:
        return String.class;
      case status:
        return int.class;
      case last_heartbeat:
        return long.class;
      case last_heartbeat_epoch:
        return Long.class;
      default:
        throw new IllegalStateException("Invalid field: " + field);
    }    
  }

  public static Class getFieldType(String fieldName) {    
    if (fieldName.equals("host")) {
      return String.class;
    }
    if (fieldName.equals("status")) {
      return int.class;
    }
    if (fieldName.equals("last_heartbeat")) {
      return long.class;
    }
    if (fieldName.equals("last_heartbeat_epoch")) {
      return Long.class;
    }
    throw new IllegalStateException("Invalid field name: " + fieldName);
  }

  public List<BackgroundStepAttemptInfo> getBackgroundStepAttemptInfo() throws IOException {
    return __assoc_background_step_attempt_info.get();
  }

  @Override
  public Object getField(String fieldName) {
    if (fieldName.equals("id")) {
      return getId();
    }
    if (fieldName.equals("host")) {
      return getHost();
    }
    if (fieldName.equals("status")) {
      return getStatus();
    }
    if (fieldName.equals("last_heartbeat")) {
      return getLastHeartbeat();
    }
    if (fieldName.equals("last_heartbeat_epoch")) {
      return getLastHeartbeatEpoch();
    }
    throw new IllegalStateException("Invalid field name: " + fieldName);
  }

  public Object getField(_Fields field) {
    switch (field) {
      case host:
        return getHost();
      case status:
        return getStatus();
      case last_heartbeat:
        return getLastHeartbeat();
      case last_heartbeat_epoch:
        return getLastHeartbeatEpoch();
    }
    throw new IllegalStateException("Invalid field: " + field);
  }

  public boolean hasField(String fieldName) {
    if (fieldName.equals("id")) {
      return true;
    }
    if (fieldName.equals("host")) {
      return true;
    }
    if (fieldName.equals("status")) {
      return true;
    }
    if (fieldName.equals("last_heartbeat")) {
      return true;
    }
    if (fieldName.equals("last_heartbeat_epoch")) {
      return true;
    }
    return false;
  }

  public static Object getDefaultValue(_Fields field) {
    switch (field) {
      case host:
        return null;
      case status:
        return null;
      case last_heartbeat:
        return null;
      case last_heartbeat_epoch:
        return null;
    }
    throw new IllegalStateException("Invalid field: " + field);
  }

  @Override
  public Set<Enum> getFieldSet() {
    Set set = EnumSet.allOf(_Fields.class);
    return set;
  }

  @Override
  public BackgroundWorkflowExecutorInfo getCopy() {
    return getCopy(databases);
  }

  @Override
  public BackgroundWorkflowExecutorInfo getCopy(IDatabases databases) {
    return new BackgroundWorkflowExecutorInfo(this, databases);
  }

  @Override
  public boolean save() throws IOException {
    return databases.getWorkflowDb().backgroundWorkflowExecutorInfos().save(this);
  }

  public String toString() {
    return "<BackgroundWorkflowExecutorInfo"
        + " id: " + this.getId()
        + " host: " + getHost()
        + " status: " + getStatus()
        + " last_heartbeat: " + getLastHeartbeat()
        + " last_heartbeat_epoch: " + getLastHeartbeatEpoch()
        + ">";
  }

  public void unsetAssociations() {
    unsetDatabaseReference();
    __assoc_background_step_attempt_info = null;
  }

  public int compareTo(BackgroundWorkflowExecutorInfo that) {
    return Long.valueOf(this.getId()).compareTo(that.getId());
  }
  
  
  public static class Attributes extends AttributesWithId {
    
    public static final long serialVersionUID = -6062746735172100773L;

    // Fields
    private String __host;
    private int __status;
    private long __last_heartbeat;
    private Long __last_heartbeat_epoch;

    public Attributes(long id) {
      super(id);
    }

    public Attributes(long id, final String host, final int status, final long last_heartbeat, final Long last_heartbeat_epoch) {
      super(id);
      this.__host = host;
      this.__status = status;
      this.__last_heartbeat = last_heartbeat;
      this.__last_heartbeat_epoch = last_heartbeat_epoch;
    }
    
    public Attributes(long id, final String host, final int status, final long last_heartbeat) {
      super(id);
      this.__host = host;
      this.__status = status;
      this.__last_heartbeat = last_heartbeat;
    }

    public static Attributes newDefaultInstance(long id) {
      return new Attributes(id, "", 0, 0L);
    }

    public Attributes(long id, Map<Enum, Object> fieldsMap) {
      super(id);
      String host = (String)fieldsMap.get(BackgroundWorkflowExecutorInfo._Fields.host);
      int status = (Integer)fieldsMap.get(BackgroundWorkflowExecutorInfo._Fields.status);
      long last_heartbeat = (Long)fieldsMap.get(BackgroundWorkflowExecutorInfo._Fields.last_heartbeat);
      Long last_heartbeat_epoch = (Long)fieldsMap.get(BackgroundWorkflowExecutorInfo._Fields.last_heartbeat_epoch);
      this.__host = host;
      this.__status = status;
      this.__last_heartbeat = last_heartbeat;
      this.__last_heartbeat_epoch = last_heartbeat_epoch;
    }

    public Attributes(Attributes other) {
      super(other.getId());
      this.__host = other.getHost();
      this.__status = other.getStatus();
      this.__last_heartbeat = other.getLastHeartbeat();
      this.__last_heartbeat_epoch = other.getLastHeartbeatEpoch();
    }

    public String getHost() {
      return __host;
    }

    public Attributes setHost(String newval) {
      this.__host = newval;
      cachedHashCode = 0;
      return this;
    }

    public int getStatus() {
      return __status;
    }

    public Attributes setStatus(int newval) {
      this.__status = newval;
      cachedHashCode = 0;
      return this;
    }

    public long getLastHeartbeat() {
      return __last_heartbeat;
    }

    public Attributes setLastHeartbeat(long newval) {
      this.__last_heartbeat = newval;
      cachedHashCode = 0;
      return this;
    }

    public Long getLastHeartbeatEpoch() {
      return __last_heartbeat_epoch;
    }

    public Attributes setLastHeartbeatEpoch(Long newval) {
      this.__last_heartbeat_epoch = newval;
      cachedHashCode = 0;
      return this;
    }

    public void setField(_Fields field, Object value) {
      switch (field) {
        case host:
          setHost((String)value);
          break;
        case status:
          setStatus((Integer)value);
          break;
        case last_heartbeat:
          setLastHeartbeat((Long)value);
          break;
        case last_heartbeat_epoch:
          setLastHeartbeatEpoch((Long)value);
          break;
        default:
          throw new IllegalStateException("Invalid field: " + field);
      }
    }

    public void setField(String fieldName, Object value) {
      if (fieldName.equals("host")) {
        setHost((String)value);
        return;
      }
      if (fieldName.equals("status")) {
        setStatus((Integer)value);
        return;
      }
      if (fieldName.equals("last_heartbeat")) {
        setLastHeartbeat((Long)value);
        return;
      }
      if (fieldName.equals("last_heartbeat_epoch")) {
        setLastHeartbeatEpoch((Long)value);
        return;
      }
      throw new IllegalStateException("Invalid field: " + fieldName);
    }

    public static Class getFieldType(_Fields field) {
      switch (field) {
        case host:
          return String.class;
        case status:
          return int.class;
        case last_heartbeat:
          return long.class;
        case last_heartbeat_epoch:
          return Long.class;
        default:
          throw new IllegalStateException("Invalid field: " + field);
      }    
    }

    public static Class getFieldType(String fieldName) {    
      if (fieldName.equals("host")) {
        return String.class;
      }
      if (fieldName.equals("status")) {
        return int.class;
      }
      if (fieldName.equals("last_heartbeat")) {
        return long.class;
      }
      if (fieldName.equals("last_heartbeat_epoch")) {
        return Long.class;
      }
      throw new IllegalStateException("Invalid field name: " + fieldName);
    }

    @Override
    public Object getField(String fieldName) {
      if (fieldName.equals("id")) {
        return getId();
      }
      if (fieldName.equals("host")) {
        return getHost();
      }
      if (fieldName.equals("status")) {
        return getStatus();
      }
      if (fieldName.equals("last_heartbeat")) {
        return getLastHeartbeat();
      }
      if (fieldName.equals("last_heartbeat_epoch")) {
        return getLastHeartbeatEpoch();
      }
      throw new IllegalStateException("Invalid field name: " + fieldName);
    }

    public Object getField(_Fields field) {
      switch (field) {
        case host:
          return getHost();
        case status:
          return getStatus();
        case last_heartbeat:
          return getLastHeartbeat();
        case last_heartbeat_epoch:
          return getLastHeartbeatEpoch();
      }
      throw new IllegalStateException("Invalid field: " + field);
    }

    public boolean hasField(String fieldName) {
      if (fieldName.equals("id")) {
        return true;
      }
      if (fieldName.equals("host")) {
        return true;
      }
      if (fieldName.equals("status")) {
        return true;
      }
      if (fieldName.equals("last_heartbeat")) {
        return true;
      }
      if (fieldName.equals("last_heartbeat_epoch")) {
        return true;
      }
      return false;
    }

    public static Object getDefaultValue(_Fields field) {
      switch (field) {
        case host:
          return null;
        case status:
          return null;
        case last_heartbeat:
          return null;
        case last_heartbeat_epoch:
          return null;
      }
      throw new IllegalStateException("Invalid field: " + field);
    }
    
    @Override
    public Set<Enum> getFieldSet() {
      Set set = EnumSet.allOf(_Fields.class);
      return set;
    }
    
    public String toString() {
      return "<BackgroundWorkflowExecutorInfo.Attributes"
          + " host: " + getHost()
          + " status: " + getStatus()
          + " last_heartbeat: " + getLastHeartbeat()
          + " last_heartbeat_epoch: " + getLastHeartbeatEpoch()
          + ">";
    }
  }

  public static class Id implements ModelIdWrapper<BackgroundWorkflowExecutorInfo.Id> {
    public static final long serialVersionUID = 1L;

    private final long id;

    public Id(Long id) {
      this.id = id;
    }

    @Override
    public Long getId() {
      return id;
    }

    @Override
    public int compareTo(Id other) {
      return this.getId().compareTo(other.getId());
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof Id) {
        return this.getId().equals(((Id)other).getId());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return this.getId().hashCode();
    }

    @Override
    public String toString() {
      return "<BackgroundWorkflowExecutorInfo.Id: " + this.getId() + ">";
    }
  }

  public static Set<Attributes> convertToAttributesSet(Collection<BackgroundWorkflowExecutorInfo> models) {
    return models.stream()
        .map(BackgroundWorkflowExecutorInfo::getAttributes)
        .collect(Collectors.toSet());
  }

  public static class AssociationMetadata implements IModelAssociationMetadata {

    private List<IAssociationMetadata> meta = new ArrayList<>();

    public AssociationMetadata(){
      meta.add(new DefaultAssociationMetadata(AssociationType.HAS_MANY, BackgroundWorkflowExecutorInfo.class, BackgroundStepAttemptInfo.class, "background_workflow_executor_info_id"));
    }

    @Override
    public List<IAssociationMetadata> getAssociationMetadata() {
      return meta;
    }
  }

}
