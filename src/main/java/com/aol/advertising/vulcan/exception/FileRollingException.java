package com.aol.advertising.vulcan.exception;

import java.io.IOException;

public class FileRollingException extends IOException {

  private static final long serialVersionUID = 1L;

  public FileRollingException(String message) {
    super(message);
  }
}
