package org.slf4j.impl;

import org.slf4j.ILoggerFactory;
import org.slf4j.helpers.NOPLoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

/**
 * When placed in the test classpath, this class removes the error output Slf4j generates when it
 * cannot find an implementation.
 * <p>
 * If it creates problems in the future, simply remove this class. It's not actually part of the
 * tests
 * 
 */
public class StaticLoggerBinder implements LoggerFactoryBinder {

  public static final StaticLoggerBinder getSingleton() {
    return new StaticLoggerBinder();
  }

  @Override
  public ILoggerFactory getLoggerFactory() {
    return new NOPLoggerFactory();
  }

  @Override
  public String getLoggerFactoryClassStr() {
    return NOPLoggerFactory.class.getName();
  }
}
