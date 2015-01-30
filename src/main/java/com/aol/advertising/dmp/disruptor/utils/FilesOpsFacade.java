package com.aol.advertising.dmp.disruptor.utils;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

public class FilesOpsFacade {
  
  public static FilesOpsFacade facadeInstance = new FilesOpsFacade();

  public boolean exists(final Path path, final LinkOption... options) {
    return Files.exists(path, options);
  }

  public void move(final Path source, final Path target, final CopyOption... options) throws IOException {
    Files.move(source, target, options);
  }

}
