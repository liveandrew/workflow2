
/**
 * Autogenerated by Jack
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.liveramp.databases.workflow_db.iface;

import com.liveramp.databases.workflow_db.models.Application;
import com.liveramp.databases.workflow_db.query.ApplicationQueryBuilder;
import com.liveramp.databases.workflow_db.query.ApplicationDeleteBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.List;

import com.rapleaf.jack.IModelPersistence;

public interface IApplicationPersistence extends IModelPersistence<Application> {
  Application create(final String name, final Integer app_type) throws IOException;
  Application create(final String name) throws IOException;

  Application createDefaultInstance() throws IOException;
  List<Application> findByName(String value)  throws IOException;
  List<Application> findByAppType(Integer value)  throws IOException;

  ApplicationQueryBuilder query();

  ApplicationDeleteBuilder delete();
}
