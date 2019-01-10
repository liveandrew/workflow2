
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

public class WorkflowExecutionConfiguredNotification extends ModelWithId<WorkflowExecutionConfiguredNotification, IDatabases> implements Comparable<WorkflowExecutionConfiguredNotification>{
  
  public static final long serialVersionUID = 2403628880853412806L;

  public static class Tbl extends AbstractTable<WorkflowExecutionConfiguredNotification.Attributes, WorkflowExecutionConfiguredNotification> {
    public final Column<Long> ID;
    public final Column<Long> WORKFLOW_EXECUTION_ID;
    public final Column<Long> CONFIGURED_NOTIFICATION_ID;

    private Tbl(String alias) {
      super("workflow_execution_configured_notifications", alias, WorkflowExecutionConfiguredNotification.Attributes.class, WorkflowExecutionConfiguredNotification.class);
      this.ID = Column.fromId(alias);
      this.WORKFLOW_EXECUTION_ID = Column.fromField(alias, _Fields.workflow_execution_id, Long.class);
      this.CONFIGURED_NOTIFICATION_ID = Column.fromField(alias, _Fields.configured_notification_id, Long.class);
      Collections.addAll(this.allColumns, ID, WORKFLOW_EXECUTION_ID, CONFIGURED_NOTIFICATION_ID);
    }

    public static Tbl as(String alias) {
      return new Tbl(alias);
    }
  }

  public static final Tbl TBL = new Tbl("workflow_execution_configured_notifications");
  public static final Column<Long> ID = TBL.ID;
  public static final Column<Long> WORKFLOW_EXECUTION_ID = TBL.WORKFLOW_EXECUTION_ID;
  public static final Column<Long> CONFIGURED_NOTIFICATION_ID = TBL.CONFIGURED_NOTIFICATION_ID;

  private final Attributes attributes;

  private transient WorkflowExecutionConfiguredNotification.Id cachedTypedId;

  // Associations
  private BelongsToAssociation<ConfiguredNotification> __assoc_configured_notification;
  private BelongsToAssociation<WorkflowExecution> __assoc_workflow_execution;

  public enum _Fields {
    workflow_execution_id,
    configured_notification_id,
  }

  @Override
  public WorkflowExecutionConfiguredNotification.Id getTypedId() {
    if (cachedTypedId == null) {
      cachedTypedId = new WorkflowExecutionConfiguredNotification.Id(this.getId());
    }
    return cachedTypedId;
  }

  public WorkflowExecutionConfiguredNotification(long id, final long workflow_execution_id, final long configured_notification_id, IDatabases databases) {
    super(databases);
    attributes = new Attributes(id, workflow_execution_id, configured_notification_id);
    this.__assoc_configured_notification = new BelongsToAssociation<>(databases.getWorkflowDb().configuredNotifications(), getConfiguredNotificationId());
    this.__assoc_workflow_execution = new BelongsToAssociation<>(databases.getWorkflowDb().workflowExecutions(), getWorkflowExecutionId());
  }

  public WorkflowExecutionConfiguredNotification(long id, final long workflow_execution_id, final long configured_notification_id) {
    super(null);
    attributes = new Attributes(id, workflow_execution_id, configured_notification_id);
  }

  public static WorkflowExecutionConfiguredNotification newDefaultInstance(long id) {
    return new WorkflowExecutionConfiguredNotification(id, 0L, 0L);
  }

  public WorkflowExecutionConfiguredNotification(Attributes attributes, IDatabases databases) {
    super(databases);
    this.attributes = attributes;

    if (databases != null) {
      this.__assoc_configured_notification = new BelongsToAssociation<>(databases.getWorkflowDb().configuredNotifications(), getConfiguredNotificationId());
      this.__assoc_workflow_execution = new BelongsToAssociation<>(databases.getWorkflowDb().workflowExecutions(), getWorkflowExecutionId());
    }
  }

  public WorkflowExecutionConfiguredNotification(Attributes attributes) {
    this(attributes, (IDatabases) null);
  }

  public WorkflowExecutionConfiguredNotification(long id, Map<Enum, Object> fieldsMap) {
    super(null);
    attributes = new Attributes(id, fieldsMap);
  }

  public WorkflowExecutionConfiguredNotification (WorkflowExecutionConfiguredNotification other) {
    this(other, (IDatabases)null);
  }

  public WorkflowExecutionConfiguredNotification (WorkflowExecutionConfiguredNotification other, IDatabases databases) {
    super(databases);
    attributes = new Attributes(other.getAttributes());

    if (databases != null) {
      this.__assoc_configured_notification = new BelongsToAssociation<>(databases.getWorkflowDb().configuredNotifications(), getConfiguredNotificationId());
      this.__assoc_workflow_execution = new BelongsToAssociation<>(databases.getWorkflowDb().workflowExecutions(), getWorkflowExecutionId());
    }
  }

  public Attributes getAttributes() {
    return attributes;
  }

  public long getWorkflowExecutionId() {
    return attributes.getWorkflowExecutionId();
  }

  public WorkflowExecutionConfiguredNotification setWorkflowExecutionId(long newval) {
    attributes.setWorkflowExecutionId(newval);
    if(__assoc_workflow_execution != null){
      this.__assoc_workflow_execution.setOwnerId(newval);
    }
    cachedHashCode = 0;
    return this;
  }

  public long getConfiguredNotificationId() {
    return attributes.getConfiguredNotificationId();
  }

  public WorkflowExecutionConfiguredNotification setConfiguredNotificationId(long newval) {
    attributes.setConfiguredNotificationId(newval);
    if(__assoc_configured_notification != null){
      this.__assoc_configured_notification.setOwnerId(newval);
    }
    cachedHashCode = 0;
    return this;
  }

  public void setField(_Fields field, Object value) {
    switch (field) {
      case workflow_execution_id:
        setWorkflowExecutionId((Long)value);
        break;
      case configured_notification_id:
        setConfiguredNotificationId((Long)value);
        break;
      default:
        throw new IllegalStateException("Invalid field: " + field);
    }
  }
  
  public void setField(String fieldName, Object value) {
    if (fieldName.equals("workflow_execution_id")) {
      setWorkflowExecutionId((Long)  value);
      return;
    }
    if (fieldName.equals("configured_notification_id")) {
      setConfiguredNotificationId((Long)  value);
      return;
    }
    throw new IllegalStateException("Invalid field: " + fieldName);
  }

  public static Class getFieldType(_Fields field) {
    switch (field) {
      case workflow_execution_id:
        return long.class;
      case configured_notification_id:
        return long.class;
      default:
        throw new IllegalStateException("Invalid field: " + field);
    }    
  }

  public static Class getFieldType(String fieldName) {    
    if (fieldName.equals("workflow_execution_id")) {
      return long.class;
    }
    if (fieldName.equals("configured_notification_id")) {
      return long.class;
    }
    throw new IllegalStateException("Invalid field name: " + fieldName);
  }

  public ConfiguredNotification getConfiguredNotification() throws IOException {
    return __assoc_configured_notification.get();
  }

  public WorkflowExecution getWorkflowExecution() throws IOException {
    return __assoc_workflow_execution.get();
  }

  @Override
  public Object getField(String fieldName) {
    if (fieldName.equals("id")) {
      return getId();
    }
    if (fieldName.equals("workflow_execution_id")) {
      return getWorkflowExecutionId();
    }
    if (fieldName.equals("configured_notification_id")) {
      return getConfiguredNotificationId();
    }
    throw new IllegalStateException("Invalid field name: " + fieldName);
  }

  public Object getField(_Fields field) {
    switch (field) {
      case workflow_execution_id:
        return getWorkflowExecutionId();
      case configured_notification_id:
        return getConfiguredNotificationId();
    }
    throw new IllegalStateException("Invalid field: " + field);
  }

  public boolean hasField(String fieldName) {
    if (fieldName.equals("id")) {
      return true;
    }
    if (fieldName.equals("workflow_execution_id")) {
      return true;
    }
    if (fieldName.equals("configured_notification_id")) {
      return true;
    }
    return false;
  }

  public static Object getDefaultValue(_Fields field) {
    switch (field) {
      case workflow_execution_id:
        return null;
      case configured_notification_id:
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
  public WorkflowExecutionConfiguredNotification getCopy() {
    return getCopy(databases);
  }

  @Override
  public WorkflowExecutionConfiguredNotification getCopy(IDatabases databases) {
    return new WorkflowExecutionConfiguredNotification(this, databases);
  }

  @Override
  public boolean save() throws IOException {
    return databases.getWorkflowDb().workflowExecutionConfiguredNotifications().save(this);
  }

  public ConfiguredNotification createConfiguredNotification(final int workflow_runner_notification) throws IOException {
 
    ConfiguredNotification newConfiguredNotification = databases.getWorkflowDb().configuredNotifications().create(workflow_runner_notification);
    setConfiguredNotificationId(newConfiguredNotification.getId());
    save();
    __assoc_configured_notification.clearCache();
    return newConfiguredNotification;
  }

  public ConfiguredNotification createConfiguredNotification(final int workflow_runner_notification, final String email, final Boolean provided_alerts_handler) throws IOException {
 
    ConfiguredNotification newConfiguredNotification = databases.getWorkflowDb().configuredNotifications().create(workflow_runner_notification, email, provided_alerts_handler);
    setConfiguredNotificationId(newConfiguredNotification.getId());
    save();
    __assoc_configured_notification.clearCache();
    return newConfiguredNotification;
  }

  public ConfiguredNotification createConfiguredNotification() throws IOException {
 
    ConfiguredNotification newConfiguredNotification = databases.getWorkflowDb().configuredNotifications().create(0);
    setConfiguredNotificationId(newConfiguredNotification.getId());
    save();
    __assoc_configured_notification.clearCache();
    return newConfiguredNotification;
  }

  public WorkflowExecution createWorkflowExecution(final String name, final int status) throws IOException {
 
    WorkflowExecution newWorkflowExecution = databases.getWorkflowDb().workflowExecutions().create(name, status);
    setWorkflowExecutionId(newWorkflowExecution.getId());
    save();
    __assoc_workflow_execution.clearCache();
    return newWorkflowExecution;
  }

  public WorkflowExecution createWorkflowExecution(final Integer app_type, final String name, final String scope_identifier, final int status, final Long start_time, final Long end_time, final Integer application_id, final String pool_override) throws IOException {
 
    WorkflowExecution newWorkflowExecution = databases.getWorkflowDb().workflowExecutions().create(app_type, name, scope_identifier, status, start_time, end_time, application_id, pool_override);
    setWorkflowExecutionId(newWorkflowExecution.getId());
    save();
    __assoc_workflow_execution.clearCache();
    return newWorkflowExecution;
  }

  public WorkflowExecution createWorkflowExecution() throws IOException {
 
    WorkflowExecution newWorkflowExecution = databases.getWorkflowDb().workflowExecutions().create("", 0);
    setWorkflowExecutionId(newWorkflowExecution.getId());
    save();
    __assoc_workflow_execution.clearCache();
    return newWorkflowExecution;
  }

  public String toString() {
    return "<WorkflowExecutionConfiguredNotification"
        + " id: " + this.getId()
        + " workflow_execution_id: " + getWorkflowExecutionId()
        + " configured_notification_id: " + getConfiguredNotificationId()
        + ">";
  }

  public void unsetAssociations() {
    unsetDatabaseReference();
    __assoc_configured_notification = null;
    __assoc_workflow_execution = null;
  }

  public int compareTo(WorkflowExecutionConfiguredNotification that) {
    return Long.valueOf(this.getId()).compareTo(that.getId());
  }
  
  
  public static class Attributes extends AttributesWithId {
    
    public static final long serialVersionUID = 5526705265741082250L;

    // Fields
    private long __workflow_execution_id;
    private long __configured_notification_id;

    public Attributes(long id) {
      super(id);
    }

    public Attributes(long id, final long workflow_execution_id, final long configured_notification_id) {
      super(id);
      this.__workflow_execution_id = workflow_execution_id;
      this.__configured_notification_id = configured_notification_id;
    }

    public static Attributes newDefaultInstance(long id) {
      return new Attributes(id, 0L, 0L);
    }

    public Attributes(long id, Map<Enum, Object> fieldsMap) {
      super(id);
      long workflow_execution_id = (Long)fieldsMap.get(WorkflowExecutionConfiguredNotification._Fields.workflow_execution_id);
      long configured_notification_id = (Long)fieldsMap.get(WorkflowExecutionConfiguredNotification._Fields.configured_notification_id);
      this.__workflow_execution_id = workflow_execution_id;
      this.__configured_notification_id = configured_notification_id;
    }

    public Attributes(Attributes other) {
      super(other.getId());
      this.__workflow_execution_id = other.getWorkflowExecutionId();
      this.__configured_notification_id = other.getConfiguredNotificationId();
    }

    public long getWorkflowExecutionId() {
      return __workflow_execution_id;
    }

    public Attributes setWorkflowExecutionId(long newval) {
      this.__workflow_execution_id = newval;
      cachedHashCode = 0;
      return this;
    }

    public long getConfiguredNotificationId() {
      return __configured_notification_id;
    }

    public Attributes setConfiguredNotificationId(long newval) {
      this.__configured_notification_id = newval;
      cachedHashCode = 0;
      return this;
    }

    public void setField(_Fields field, Object value) {
      switch (field) {
        case workflow_execution_id:
          setWorkflowExecutionId((Long)value);
          break;
        case configured_notification_id:
          setConfiguredNotificationId((Long)value);
          break;
        default:
          throw new IllegalStateException("Invalid field: " + field);
      }
    }

    public void setField(String fieldName, Object value) {
      if (fieldName.equals("workflow_execution_id")) {
        setWorkflowExecutionId((Long)value);
        return;
      }
      if (fieldName.equals("configured_notification_id")) {
        setConfiguredNotificationId((Long)value);
        return;
      }
      throw new IllegalStateException("Invalid field: " + fieldName);
    }

    public static Class getFieldType(_Fields field) {
      switch (field) {
        case workflow_execution_id:
          return long.class;
        case configured_notification_id:
          return long.class;
        default:
          throw new IllegalStateException("Invalid field: " + field);
      }    
    }

    public static Class getFieldType(String fieldName) {    
      if (fieldName.equals("workflow_execution_id")) {
        return long.class;
      }
      if (fieldName.equals("configured_notification_id")) {
        return long.class;
      }
      throw new IllegalStateException("Invalid field name: " + fieldName);
    }

    @Override
    public Object getField(String fieldName) {
      if (fieldName.equals("id")) {
        return getId();
      }
      if (fieldName.equals("workflow_execution_id")) {
        return getWorkflowExecutionId();
      }
      if (fieldName.equals("configured_notification_id")) {
        return getConfiguredNotificationId();
      }
      throw new IllegalStateException("Invalid field name: " + fieldName);
    }

    public Object getField(_Fields field) {
      switch (field) {
        case workflow_execution_id:
          return getWorkflowExecutionId();
        case configured_notification_id:
          return getConfiguredNotificationId();
      }
      throw new IllegalStateException("Invalid field: " + field);
    }

    public boolean hasField(String fieldName) {
      if (fieldName.equals("id")) {
        return true;
      }
      if (fieldName.equals("workflow_execution_id")) {
        return true;
      }
      if (fieldName.equals("configured_notification_id")) {
        return true;
      }
      return false;
    }

    public static Object getDefaultValue(_Fields field) {
      switch (field) {
        case workflow_execution_id:
          return null;
        case configured_notification_id:
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
      return "<WorkflowExecutionConfiguredNotification.Attributes"
          + " workflow_execution_id: " + getWorkflowExecutionId()
          + " configured_notification_id: " + getConfiguredNotificationId()
          + ">";
    }
  }

  public static class Id implements ModelIdWrapper<WorkflowExecutionConfiguredNotification.Id> {
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
      return "<WorkflowExecutionConfiguredNotification.Id: " + this.getId() + ">";
    }
  }

  public static Set<Attributes> convertToAttributesSet(Collection<WorkflowExecutionConfiguredNotification> models) {
    return models.stream()
        .map(WorkflowExecutionConfiguredNotification::getAttributes)
        .collect(Collectors.toSet());
  }

  public static class AssociationMetadata implements IModelAssociationMetadata {

    private List<IAssociationMetadata> meta = new ArrayList<>();

    public AssociationMetadata(){
      meta.add(new DefaultAssociationMetadata(AssociationType.BELONGS_TO, WorkflowExecutionConfiguredNotification.class, ConfiguredNotification.class, "configured_notification_id"));
      meta.add(new DefaultAssociationMetadata(AssociationType.BELONGS_TO, WorkflowExecutionConfiguredNotification.class, WorkflowExecution.class, "workflow_execution_id"));
    }

    @Override
    public List<IAssociationMetadata> getAssociationMetadata() {
      return meta;
    }
  }

}