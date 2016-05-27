package com.rapleaf.cascading_ext.workflow2.state;

import java.util.LinkedList;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiShutdownHook extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(MultiShutdownHook.class);

  public MultiShutdownHook(String name) {
    super("Shutdown Hook for " + name);
  }

  public interface Hook {
    public void onShutdown() throws Exception;
  }

  private final LinkedList<Hook> hooks = Lists.newLinkedList();

  //  execute in reverse order added
  public void add(Hook hook){
    this.hooks.addFirst(hook);
  }

  @Override
  public void run() {
    try {
      LOG.info("Process killed, updating persistence.");

      for (Hook hook : hooks) {
        hook.onShutdown();
      }

      LOG.info("Finished cleaning up status");
    } catch (Exception e) {
      LOG.info("Failed to cleanly shutdown", e);
    }
  }
}
