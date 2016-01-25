package com.aol.advertising.vulcan.rolling;

import java.nio.file.Path;

interface RollingCondition {

  boolean shouldRollover();

  void signalRollover();

  interface FileAwareRollingCondition extends RollingCondition {

    void registerAvroFileName(Path avroFilename);

  }
}
