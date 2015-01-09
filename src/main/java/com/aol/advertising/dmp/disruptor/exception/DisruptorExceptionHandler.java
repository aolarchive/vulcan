package com.aol.advertising.dmp.disruptor.exception;

import com.lmax.disruptor.ExceptionHandler;

public class DisruptorExceptionHandler implements ExceptionHandler {

  @Override
  public void handleEventException(Throwable ex, long sequence, Object event) {
    // TODO Auto-generated method stub

  }

  @Override
  public void handleOnStartException(Throwable ex) {
    // TODO Auto-generated method stub

  }

  @Override
  public void handleOnShutdownException(Throwable ex) {
    // TODO Auto-generated method stub

  }

}
