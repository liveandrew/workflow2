
/**
 * Autogenerated by Jack
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.liveramp.databases.workflow_db.impl;

import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Timestamp;

import com.rapleaf.jack.AbstractDatabaseModel;
import com.rapleaf.jack.BaseDatabaseConnection;
import com.rapleaf.jack.queries.WhereConstraint;
import com.rapleaf.jack.queries.WhereClause;
import com.rapleaf.jack.util.JackUtility;
import com.liveramp.databases.workflow_db.iface.IConfiguredNotificationPersistence;
import com.liveramp.databases.workflow_db.models.ConfiguredNotification;
import com.liveramp.databases.workflow_db.query.ConfiguredNotificationQueryBuilder;
import com.liveramp.databases.workflow_db.query.ConfiguredNotificationDeleteBuilder;

import com.liveramp.databases.workflow_db.IDatabases;

public class BaseConfiguredNotificationPersistenceImpl extends AbstractDatabaseModel<ConfiguredNotification> implements IConfiguredNotificationPersistence {
  private final IDatabases databases;

  public BaseConfiguredNotificationPersistenceImpl(BaseDatabaseConnection conn, IDatabases databases) {
    super(conn, "configured_notifications", Arrays.<String>asList("workflow_runner_notification", "email", "provided_alerts_handler"));
    this.databases = databases;
  }

  @Override
  public ConfiguredNotification create(Map<Enum, Object> fieldsMap) throws IOException {
    int workflow_runner_notification = (Integer) fieldsMap.get(ConfiguredNotification._Fields.workflow_runner_notification);
    String email = (String) fieldsMap.get(ConfiguredNotification._Fields.email);
    Boolean provided_alerts_handler = (Boolean) fieldsMap.get(ConfiguredNotification._Fields.provided_alerts_handler);
    return create(workflow_runner_notification, email, provided_alerts_handler);
  }

  public ConfiguredNotification create(final int workflow_runner_notification, final String email, final Boolean provided_alerts_handler) throws IOException {
    StatementCreator statementCreator = new StatementCreator() {
      private final List<String> nonNullFields = new ArrayList<>();
      private final List<AttrSetter> statementSetters = new ArrayList<>();

      {
        int index = 1;

        nonNullFields.add("workflow_runner_notification");
        int fieldIndex0 = index++;
        statementSetters.add(stmt -> stmt.setInt(fieldIndex0, workflow_runner_notification));

        if (email != null) {
          nonNullFields.add("email");
          int fieldIndex1 = index++;
          statementSetters.add(stmt -> stmt.setString(fieldIndex1, email));
        }

        if (provided_alerts_handler != null) {
          nonNullFields.add("provided_alerts_handler");
          int fieldIndex2 = index++;
          statementSetters.add(stmt -> stmt.setBoolean(fieldIndex2, provided_alerts_handler));
        }
      }

      @Override
      public String getStatement() {
        return getInsertStatement(nonNullFields);
      }

      @Override
      public void setStatement(PreparedStatement statement) throws SQLException {
        for (AttrSetter setter : statementSetters) {
          setter.set(statement);
        }
      }
    };

    long __id = realCreate(statementCreator);
    ConfiguredNotification newInst = new ConfiguredNotification(__id, workflow_runner_notification, email, provided_alerts_handler, databases);
    newInst.setCreated(true);
    cachedById.put(__id, newInst);
    clearForeignKeyCache();
    return newInst;
  }

  public ConfiguredNotification create(final int workflow_runner_notification) throws IOException {
    StatementCreator statementCreator = new StatementCreator() {
      private final List<String> nonNullFields = new ArrayList<>();
      private final List<AttrSetter> statementSetters = new ArrayList<>();

      {
        int index = 1;

        nonNullFields.add("workflow_runner_notification");
        int fieldIndex0 = index++;
        statementSetters.add(stmt -> stmt.setInt(fieldIndex0, workflow_runner_notification));
      }

      @Override
      public String getStatement() {
        return getInsertStatement(nonNullFields);
      }

      @Override
      public void setStatement(PreparedStatement statement) throws SQLException {
        for (AttrSetter setter : statementSetters) {
          setter.set(statement);
        }
      }
    };

    long __id = realCreate(statementCreator);
    ConfiguredNotification newInst = new ConfiguredNotification(__id, workflow_runner_notification, null, null, databases);
    newInst.setCreated(true);
    cachedById.put(__id, newInst);
    clearForeignKeyCache();
    return newInst;
  }

  public ConfiguredNotification createDefaultInstance() throws IOException {
    return create(0);
  }

  public List<ConfiguredNotification> find(Map<Enum, Object> fieldsMap) throws IOException {
    return find(null, fieldsMap);
  }

  public List<ConfiguredNotification> find(Collection<Long> ids, Map<Enum, Object> fieldsMap) throws IOException {
    List<ConfiguredNotification> foundList = new ArrayList<>();

    if (fieldsMap == null || fieldsMap.isEmpty()) {
      return foundList;
    }

    StringBuilder statementString = new StringBuilder();
    statementString.append("SELECT * FROM configured_notifications WHERE (");
    List<Object> nonNullValues = new ArrayList<>();
    List<ConfiguredNotification._Fields> nonNullValueFields = new ArrayList<>();

    Iterator<Map.Entry<Enum, Object>> iter = fieldsMap.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<Enum, Object> entry = iter.next();
      Enum field = entry.getKey();
      Object value = entry.getValue();

      String queryValue = value != null ? " = ? " : " IS NULL";
      if (value != null) {
        nonNullValueFields.add((ConfiguredNotification._Fields) field);
        nonNullValues.add(value);
      }

      statementString.append(field).append(queryValue);
      if (iter.hasNext()) {
        statementString.append(" AND ");
      }
    }
    if (ids != null) statementString.append(" AND ").append(getIdSetCondition(ids));
    statementString.append(")");

    int retryCount = 0;
    PreparedStatement preparedStatement;

    while (true) {
      preparedStatement = getPreparedStatement(statementString.toString());

      for (int i = 0; i < nonNullValues.size(); i++) {
        ConfiguredNotification._Fields field = nonNullValueFields.get(i);
        try {
          switch (field) {
            case workflow_runner_notification:
              preparedStatement.setInt(i+1, (Integer) nonNullValues.get(i));
              break;
            case email:
              preparedStatement.setString(i+1, (String) nonNullValues.get(i));
              break;
            case provided_alerts_handler:
              preparedStatement.setBoolean(i+1, (Boolean) nonNullValues.get(i));
              break;
          }
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }

      try {
        executeQuery(foundList, preparedStatement);
        return foundList;
      } catch (SQLRecoverableException e) {
        if (++retryCount > AbstractDatabaseModel.MAX_CONNECTION_RETRIES) {
          throw new IOException(e);
        }
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  protected void setStatementParameters(PreparedStatement preparedStatement, WhereClause whereClause) throws IOException {
    int index = 0;
    for (WhereConstraint constraint : whereClause.getWhereConstraints()) {
      for (Object parameter : constraint.getParameters()) {
        if (parameter == null) {
          continue;
        }
        try {
          if (constraint.isId()) {
            preparedStatement.setLong(++index, (Long)parameter);
          } else {
            ConfiguredNotification._Fields field = (ConfiguredNotification._Fields)constraint.getField();
            switch (field) {
              case workflow_runner_notification:
                preparedStatement.setInt(++index, (Integer) parameter);
                break;
              case email:
                preparedStatement.setString(++index, (String) parameter);
                break;
              case provided_alerts_handler:
                preparedStatement.setBoolean(++index, (Boolean) parameter);
                break;
            }
          }
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }
    }
  }

  @Override
  protected void setAttrs(ConfiguredNotification model, PreparedStatement stmt, boolean setNull) throws SQLException {
    int index = 1;
    {
      stmt.setInt(index++, model.getWorkflowRunnerNotification());
    }
    if (setNull && model.getEmail() == null) {
      stmt.setNull(index++, java.sql.Types.CHAR);
    } else if (model.getEmail() != null) {
      stmt.setString(index++, model.getEmail());
    }
    if (setNull && model.isProvidedAlertsHandler() == null) {
      stmt.setNull(index++, java.sql.Types.BOOLEAN);
    } else if (model.isProvidedAlertsHandler() != null) {
      stmt.setBoolean(index++, model.isProvidedAlertsHandler());
    }
    stmt.setLong(index, model.getId());
  }

  @Override
  protected ConfiguredNotification instanceFromResultSet(ResultSet rs, Collection<Enum> selectedFields) throws SQLException {
    boolean allFields = selectedFields == null || selectedFields.isEmpty();
    long id = rs.getLong("id");
    return new ConfiguredNotification(id,
      allFields || selectedFields.contains(ConfiguredNotification._Fields.workflow_runner_notification) ? getIntOrNull(rs, "workflow_runner_notification") : 0,
      allFields || selectedFields.contains(ConfiguredNotification._Fields.email) ? rs.getString("email") : null,
      allFields || selectedFields.contains(ConfiguredNotification._Fields.provided_alerts_handler) ? getBooleanOrNull(rs, "provided_alerts_handler") : null,
      databases
    );
  }

  public List<ConfiguredNotification> findByWorkflowRunnerNotification(final int value) throws IOException {
    return find(Collections.<Enum, Object>singletonMap(ConfiguredNotification._Fields.workflow_runner_notification, value));
  }

  public List<ConfiguredNotification> findByEmail(final String value) throws IOException {
    return find(Collections.<Enum, Object>singletonMap(ConfiguredNotification._Fields.email, value));
  }

  public List<ConfiguredNotification> findByProvidedAlertsHandler(final Boolean value) throws IOException {
    return find(Collections.<Enum, Object>singletonMap(ConfiguredNotification._Fields.provided_alerts_handler, value));
  }

  public ConfiguredNotificationQueryBuilder query() {
    return new ConfiguredNotificationQueryBuilder(this);
  }

  public ConfiguredNotificationDeleteBuilder delete() {
    return new ConfiguredNotificationDeleteBuilder(this);
  }
}