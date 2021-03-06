
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

public class WorkflowAttemptConfiguredNotification extends ModelWithId<WorkflowAttemptConfiguredNotification, IDatabases> implements Comparable<WorkflowAttemptConfiguredNotification>{
  
  public static final long serialVersionUID = 7285080083867928196L;

  public static class Tbl extends AbstractTable<WorkflowAttemptConfiguredNotification.Attributes, WorkflowAttemptConfiguredNotification> {
    public final Column<Long> ID;
    public final Column<Long> WORKFLOW_ATTEMPT_ID;
    public final Column<Long> CONFIGURED_NOTIFICATION_ID;

    private Tbl(String alias) {
      super("workflow_attempt_configured_notifications", alias, WorkflowAttemptConfiguredNotification.Attributes.class, WorkflowAttemptConfiguredNotification.class);
      this.ID = Column.fromId(alias);
      this.WORKFLOW_ATTEMPT_ID = Column.fromField(alias, _Fields.workflow_attempt_id, Long.class);
      this.CONFIGURED_NOTIFICATION_ID = Column.fromField(alias, _Fields.configured_notification_id, Long.class);
      Collections.addAll(this.allColumns, ID, WORKFLOW_ATTEMPT_ID, CONFIGURED_NOTIFICATION_ID);
    }

    public static Tbl as(String alias) {
      return new Tbl(alias);
    }
  }

  public static final Tbl TBL = new Tbl("workflow_attempt_configured_notifications");
  public static final Column<Long> ID = TBL.ID;
  public static final Column<Long> WORKFLOW_ATTEMPT_ID = TBL.WORKFLOW_ATTEMPT_ID;
  public static final Column<Long> CONFIGURED_NOTIFICATION_ID = TBL.CONFIGURED_NOTIFICATION_ID;

  private final Attributes attributes;

  private transient WorkflowAttemptConfiguredNotification.Id cachedTypedId;

  // Associations
  private BelongsToAssociation<ConfiguredNotification> __assoc_configured_notification;
  private BelongsToAssociation<WorkflowAttempt> __assoc_workflow_attempt;

  public enum _Fields {
    workflow_attempt_id,
    configured_notification_id,
  }

  @Override
  public WorkflowAttemptConfiguredNotification.Id getTypedId() {
    if (cachedTypedId == null) {
      cachedTypedId = new WorkflowAttemptConfiguredNotification.Id(this.getId());
    }
    return cachedTypedId;
  }

  public WorkflowAttemptConfiguredNotification(long id, final long workflow_attempt_id, final long configured_notification_id, IDatabases databases) {
    super(databases);
    attributes = new Attributes(id, workflow_attempt_id, configured_notification_id);
    this.__assoc_configured_notification = new BelongsToAssociation<>(databases.getWorkflowDb().configuredNotifications(), getConfiguredNotificationId());
    this.__assoc_workflow_attempt = new BelongsToAssociation<>(databases.getWorkflowDb().workflowAttempts(), getWorkflowAttemptId());
  }

  public WorkflowAttemptConfiguredNotification(long id, final long workflow_attempt_id, final long configured_notification_id) {
    super(null);
    attributes = new Attributes(id, workflow_attempt_id, configured_notification_id);
  }

  public static WorkflowAttemptConfiguredNotification newDefaultInstance(long id) {
    return new WorkflowAttemptConfiguredNotification(id, 0L, 0L);
  }

  public WorkflowAttemptConfiguredNotification(Attributes attributes, IDatabases databases) {
    super(databases);
    this.attributes = attributes;

    if (databases != null) {
      this.__assoc_configured_notification = new BelongsToAssociation<>(databases.getWorkflowDb().configuredNotifications(), getConfiguredNotificationId());
      this.__assoc_workflow_attempt = new BelongsToAssociation<>(databases.getWorkflowDb().workflowAttempts(), getWorkflowAttemptId());
    }
  }

  public WorkflowAttemptConfiguredNotification(Attributes attributes) {
    this(attributes, (IDatabases) null);
  }

  public WorkflowAttemptConfiguredNotification(long id, Map<Enum, Object> fieldsMap) {
    super(null);
    attributes = new Attributes(id, fieldsMap);
  }

  public WorkflowAttemptConfiguredNotification (WorkflowAttemptConfiguredNotification other) {
    this(other, (IDatabases)null);
  }

  public WorkflowAttemptConfiguredNotification (WorkflowAttemptConfiguredNotification other, IDatabases databases) {
    super(databases);
    attributes = new Attributes(other.getAttributes());

    if (databases != null) {
      this.__assoc_configured_notification = new BelongsToAssociation<>(databases.getWorkflowDb().configuredNotifications(), getConfiguredNotificationId());
      this.__assoc_workflow_attempt = new BelongsToAssociation<>(databases.getWorkflowDb().workflowAttempts(), getWorkflowAttemptId());
    }
  }

  public Attributes getAttributes() {
    return attributes;
  }

  public long getWorkflowAttemptId() {
    return attributes.getWorkflowAttemptId();
  }

  public WorkflowAttemptConfiguredNotification setWorkflowAttemptId(long newval) {
    attributes.setWorkflowAttemptId(newval);
    if(__assoc_workflow_attempt != null){
      this.__assoc_workflow_attempt.setOwnerId(newval);
    }
    cachedHashCode = 0;
    return this;
  }

  public long getConfiguredNotificationId() {
    return attributes.getConfiguredNotificationId();
  }

  public WorkflowAttemptConfiguredNotification setConfiguredNotificationId(long newval) {
    attributes.setConfiguredNotificationId(newval);
    if(__assoc_configured_notification != null){
      this.__assoc_configured_notification.setOwnerId(newval);
    }
    cachedHashCode = 0;
    return this;
  }

  public void setField(_Fields field, Object value) {
    switch (field) {
      case workflow_attempt_id:
        setWorkflowAttemptId((Long)value);
        break;
      case configured_notification_id:
        setConfiguredNotificationId((Long)value);
        break;
      default:
        throw new IllegalStateException("Invalid field: " + field);
    }
  }
  
  public void setField(String fieldName, Object value) {
    if (fieldName.equals("workflow_attempt_id")) {
      setWorkflowAttemptId((Long)  value);
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
      case workflow_attempt_id:
        return long.class;
      case configured_notification_id:
        return long.class;
      default:
        throw new IllegalStateException("Invalid field: " + field);
    }    
  }

  public static Class getFieldType(String fieldName) {    
    if (fieldName.equals("workflow_attempt_id")) {
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

  public WorkflowAttempt getWorkflowAttempt() throws IOException {
    return __assoc_workflow_attempt.get();
  }

  @Override
  public Object getField(String fieldName) {
    if (fieldName.equals("id")) {
      return getId();
    }
    if (fieldName.equals("workflow_attempt_id")) {
      return getWorkflowAttemptId();
    }
    if (fieldName.equals("configured_notification_id")) {
      return getConfiguredNotificationId();
    }
    throw new IllegalStateException("Invalid field name: " + fieldName);
  }

  public Object getField(_Fields field) {
    switch (field) {
      case workflow_attempt_id:
        return getWorkflowAttemptId();
      case configured_notification_id:
        return getConfiguredNotificationId();
    }
    throw new IllegalStateException("Invalid field: " + field);
  }

  public boolean hasField(String fieldName) {
    if (fieldName.equals("id")) {
      return true;
    }
    if (fieldName.equals("workflow_attempt_id")) {
      return true;
    }
    if (fieldName.equals("configured_notification_id")) {
      return true;
    }
    return false;
  }

  public static Object getDefaultValue(_Fields field) {
    switch (field) {
      case workflow_attempt_id:
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
  public WorkflowAttemptConfiguredNotification getCopy() {
    return getCopy(databases);
  }

  @Override
  public WorkflowAttemptConfiguredNotification getCopy(IDatabases databases) {
    return new WorkflowAttemptConfiguredNotification(this, databases);
  }

  @Override
  public boolean save() throws IOException {
    return databases.getWorkflowDb().workflowAttemptConfiguredNotifications().save(this);
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

  public WorkflowAttempt createWorkflowAttempt(final int workflow_execution_id, final String system_user, final String priority, final String pool, final String host) throws IOException {
 
    WorkflowAttempt newWorkflowAttempt = databases.getWorkflowDb().workflowAttempts().create(workflow_execution_id, system_user, priority, pool, host);
    setWorkflowAttemptId(newWorkflowAttempt.getId());
    save();
    __assoc_workflow_attempt.clearCache();
    return newWorkflowAttempt;
  }

  public WorkflowAttempt createWorkflowAttempt(final int workflow_execution_id, final String system_user, final String shutdown_reason, final String priority, final String pool, final String host, final Long start_time, final Long end_time, final Integer status, final Long last_heartbeat, final String launch_dir, final String launch_jar, final String error_email, final String info_email, final String scm_remote, final String commit_revision, final String description, final Long last_heartbeat_epoch) throws IOException {
 
    WorkflowAttempt newWorkflowAttempt = databases.getWorkflowDb().workflowAttempts().create(workflow_execution_id, system_user, shutdown_reason, priority, pool, host, start_time, end_time, status, last_heartbeat, launch_dir, launch_jar, error_email, info_email, scm_remote, commit_revision, description, last_heartbeat_epoch);
    setWorkflowAttemptId(newWorkflowAttempt.getId());
    save();
    __assoc_workflow_attempt.clearCache();
    return newWorkflowAttempt;
  }

  public WorkflowAttempt createWorkflowAttempt() throws IOException {
 
    WorkflowAttempt newWorkflowAttempt = databases.getWorkflowDb().workflowAttempts().create(0, "", "", "", "");
    setWorkflowAttemptId(newWorkflowAttempt.getId());
    save();
    __assoc_workflow_attempt.clearCache();
    return newWorkflowAttempt;
  }

  public String toString() {
    return "<WorkflowAttemptConfiguredNotification"
        + " id: " + this.getId()
        + " workflow_attempt_id: " + getWorkflowAttemptId()
        + " configured_notification_id: " + getConfiguredNotificationId()
        + ">";
  }

  public void unsetAssociations() {
    unsetDatabaseReference();
    __assoc_configured_notification = null;
    __assoc_workflow_attempt = null;
  }

  public int compareTo(WorkflowAttemptConfiguredNotification that) {
    return Long.valueOf(this.getId()).compareTo(that.getId());
  }
  
  
  public static class Attributes extends AttributesWithId {
    
    public static final long serialVersionUID = 7943318576946503525L;

    // Fields
    private long __workflow_attempt_id;
    private long __configured_notification_id;

    public Attributes(long id) {
      super(id);
    }

    public Attributes(long id, final long workflow_attempt_id, final long configured_notification_id) {
      super(id);
      this.__workflow_attempt_id = workflow_attempt_id;
      this.__configured_notification_id = configured_notification_id;
    }

    public static Attributes newDefaultInstance(long id) {
      return new Attributes(id, 0L, 0L);
    }

    public Attributes(long id, Map<Enum, Object> fieldsMap) {
      super(id);
      long workflow_attempt_id = (Long)fieldsMap.get(WorkflowAttemptConfiguredNotification._Fields.workflow_attempt_id);
      long configured_notification_id = (Long)fieldsMap.get(WorkflowAttemptConfiguredNotification._Fields.configured_notification_id);
      this.__workflow_attempt_id = workflow_attempt_id;
      this.__configured_notification_id = configured_notification_id;
    }

    public Attributes(Attributes other) {
      super(other.getId());
      this.__workflow_attempt_id = other.getWorkflowAttemptId();
      this.__configured_notification_id = other.getConfiguredNotificationId();
    }

    public long getWorkflowAttemptId() {
      return __workflow_attempt_id;
    }

    public Attributes setWorkflowAttemptId(long newval) {
      this.__workflow_attempt_id = newval;
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
        case workflow_attempt_id:
          setWorkflowAttemptId((Long)value);
          break;
        case configured_notification_id:
          setConfiguredNotificationId((Long)value);
          break;
        default:
          throw new IllegalStateException("Invalid field: " + field);
      }
    }

    public void setField(String fieldName, Object value) {
      if (fieldName.equals("workflow_attempt_id")) {
        setWorkflowAttemptId((Long)value);
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
        case workflow_attempt_id:
          return long.class;
        case configured_notification_id:
          return long.class;
        default:
          throw new IllegalStateException("Invalid field: " + field);
      }    
    }

    public static Class getFieldType(String fieldName) {    
      if (fieldName.equals("workflow_attempt_id")) {
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
      if (fieldName.equals("workflow_attempt_id")) {
        return getWorkflowAttemptId();
      }
      if (fieldName.equals("configured_notification_id")) {
        return getConfiguredNotificationId();
      }
      throw new IllegalStateException("Invalid field name: " + fieldName);
    }

    public Object getField(_Fields field) {
      switch (field) {
        case workflow_attempt_id:
          return getWorkflowAttemptId();
        case configured_notification_id:
          return getConfiguredNotificationId();
      }
      throw new IllegalStateException("Invalid field: " + field);
    }

    public boolean hasField(String fieldName) {
      if (fieldName.equals("id")) {
        return true;
      }
      if (fieldName.equals("workflow_attempt_id")) {
        return true;
      }
      if (fieldName.equals("configured_notification_id")) {
        return true;
      }
      return false;
    }

    public static Object getDefaultValue(_Fields field) {
      switch (field) {
        case workflow_attempt_id:
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
      return "<WorkflowAttemptConfiguredNotification.Attributes"
          + " workflow_attempt_id: " + getWorkflowAttemptId()
          + " configured_notification_id: " + getConfiguredNotificationId()
          + ">";
    }
  }

  public static class Id implements ModelIdWrapper<WorkflowAttemptConfiguredNotification.Id> {
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
      return "<WorkflowAttemptConfiguredNotification.Id: " + this.getId() + ">";
    }
  }

  public static Set<Attributes> convertToAttributesSet(Collection<WorkflowAttemptConfiguredNotification> models) {
    return models.stream()
        .map(WorkflowAttemptConfiguredNotification::getAttributes)
        .collect(Collectors.toSet());
  }

  public static class AssociationMetadata implements IModelAssociationMetadata {

    private List<IAssociationMetadata> meta = new ArrayList<>();

    public AssociationMetadata(){
      meta.add(new DefaultAssociationMetadata(AssociationType.BELONGS_TO, WorkflowAttemptConfiguredNotification.class, ConfiguredNotification.class, "configured_notification_id"));
      meta.add(new DefaultAssociationMetadata(AssociationType.BELONGS_TO, WorkflowAttemptConfiguredNotification.class, WorkflowAttempt.class, "workflow_attempt_id"));
    }

    @Override
    public List<IAssociationMetadata> getAssociationMetadata() {
      return meta;
    }
  }

}
