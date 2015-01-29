package com.aol.advertising.dmp.disruptor.exception;

public class FileRollingException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public FileRollingException(final String message) {
    super(message);
  }

}
