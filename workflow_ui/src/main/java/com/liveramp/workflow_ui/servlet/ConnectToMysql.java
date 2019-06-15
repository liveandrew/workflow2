package com.liveramp.workflow_ui.servlet;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.liveramp.databases.workflow_db.IWorkflowDb;
import com.rapleaf.jack.queries.QueryOrder;

public class ConnectToMysql {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectToMysql.class);

  public static void main(String[] args) throws Exception {
    try {
      int n = Integer.parseInt(args[0]);
      List<IWorkflowDb> conns = Lists.newArrayList();
      System.out.println("Creating " + n + " connections");
      LOG
      for (int i = 0; i < n; i++) {
        IWorkflowDb db = new DatabasesImpl().getWorkflowDb();
        db.workflowAttempts()
            .query()
            .orderById(QueryOrder.DESC)
            .limit(1)
            .find();
        conns.add(db);
      }
      System.out.println("Finished connecting. Sleeping");
      Thread.sleep(60 * 60 * 3600);
    } catch (Exception e) {
      System.out.println("Error " + ExceptionUtils.getFullStackTrace(e));
      Thread.sleep(5 * 60 * 1000);
      throw e;
    }
  }
}
