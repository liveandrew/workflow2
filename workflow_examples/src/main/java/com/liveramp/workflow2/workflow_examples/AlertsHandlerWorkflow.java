package com.liveramp.workflow2.workflow_examples;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.MailBuffer;
import com.liveramp.java_support.alerts_handler.configs.DefaultAlertMessageConfig;
import com.liveramp.java_support.alerts_handler.configs.GenericAlertsHandlerConfig;
import com.liveramp.java_support.alerts_handler.recipients.StaticEmailRecipient;
import com.liveramp.mail_utils.MailerHelperAlertsHandler;
import com.liveramp.mail_utils.SmtpConfig;
import com.liveramp.workflow2.workflow_examples.actions.WaitAction;
import com.liveramp.workflow_core.alerting.AlertsHandlerFactory;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunners;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;

public class AlertsHandlerWorkflow {

  public static void main(String[] args) throws IOException {

    Step step1 = new Step(new NoOpAction("step1"));
    Step step2 = new Step(new NoOpAction("step2"));
    Step step3 = new Step(new WaitAction("step3", 180_000), step1, step2);

    WorkflowRunners.dbRun(
        AlertsHandlerWorkflow.class.getName(),
        HadoopWorkflowOptions.test()
            .setAlertsHandlerFactory(new MailerHelperHandlerFactory())
            .setScope(args[0]),
        dbHadoopWorkflow -> Sets.newHashSet(step3)
    );

  }

  public static class MailerHelperHandlerFactory implements AlertsHandlerFactory {
    @Override
    public AlertsHandler buildHandler(Set<String> emails, MailBuffer buffer) {
      return new MailerHelperAlertsHandler(new GenericAlertsHandlerConfig(
          new DefaultAlertMessageConfig(false, Lists.newArrayList()),
          "noreply",
          "example.com",
          new StaticEmailRecipient("target@example.com")
      ), new SmtpConfig("mailserver.example.com", Duration.ofMinutes(5l)));
    }
  }

}
