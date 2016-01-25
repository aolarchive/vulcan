package com.aol.advertising.vulcan.writer;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ConsumerThreadFactory implements ThreadFactory {

  private static final ThreadFactory backingThreadFactory = Executors.defaultThreadFactory();

  @Override
  public Thread newThread(Runnable runnable) {
    Thread thread = backingThreadFactory.newThread(runnable);
    thread.setName("vulcan-avro-writer");
    return thread;
  }
}
