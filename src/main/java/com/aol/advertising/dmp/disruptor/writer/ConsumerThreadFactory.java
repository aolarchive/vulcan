package com.aol.advertising.dmp.disruptor.writer;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ConsumerThreadFactory implements ThreadFactory {

  private static final ThreadFactory backingThreadFactory = Executors.defaultThreadFactory();

  @Override
  public Thread newThread(final Runnable runnable) {
    final Thread thread = backingThreadFactory.newThread(runnable);
    thread.setName("disruptor-avro-writer");
    return thread;
  }
}
