package com.aol.advertising.dmp.disruptor.rolling;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Accessing the disk to check the file size is a costly operation. This class uses an <a
 * href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a> to try to
 * predict rolling points while minimizing the number of reads from disk
 */
class SizeBasedRollingCondition {

  private static final int ONE_MB_IN_BYTES = 1_048_576;
  private static final double DECAY_RATE = 0.2;
  private static final double DECAY_RATE_COMPLEMENT = 1.0 - DECAY_RATE;

  private final Path avroFileName;
  private final int rolloverTriggeringSizeInBytes;

  private int recordsInCurrentFile;
  private RolloverShouldHappen delegateImplementation;
  private double writeRate;
  
  SizeBasedRollingCondition(final Path avroFileName, int rolloverTriggeringSizeInMB) {
    this.rolloverTriggeringSizeInBytes = rolloverTriggeringSizeInMB * ONE_MB_IN_BYTES;
    this.avroFileName = avroFileName;

    this.recordsInCurrentFile = 0;
    this.delegateImplementation = getWarmupDelegateImplementation();
    this.writeRate = 0.0;
  }

  private RolloverShouldHappen getWarmupDelegateImplementation() {
    if (Files.exists(avroFileName)) {
      try {
        return new EMAWarmupPeriod(Files.size(avroFileName));
      } catch (IOException ioe) {
        throw new IllegalArgumentException(ioe);
      }
    } else {
      return new EMAWarmupPeriod(0);
    }
  }

  boolean sizeThresholdHasBeenHit() {
    try {
      return delegateImplementation.sizeThresholdHasBeenHit();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  void signalRollover() {
    try {
      delegateImplementation.signalRollover();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private interface RolloverShouldHappen {
    boolean sizeThresholdHasBeenHit() throws IOException;
    void signalRollover() throws IOException;
  }

  private class EMAWarmupPeriod implements RolloverShouldHappen {

    private static final int FIXED_SIZE_CHECK_RATE = 150;
    private static final int SCHEMA_SIZE_EPSILON = 7000;
    
    private long initialFileSize;

    private EMAWarmupPeriod(long initialFileSize) {
      this.initialFileSize = initialFileSize;
    }

    @Override
    public boolean sizeThresholdHasBeenHit() throws IOException {
      if ((++recordsInCurrentFile % FIXED_SIZE_CHECK_RATE) == 0) {
        final long currentFileSize = Files.size(avroFileName);
        if (recordsHaveBeenWrittenToDisk(currentFileSize)) {
          calculateInitialInstantRate(currentFileSize);
          estimateTotalNumberOfRecordsInFile(currentFileSize);
          switchToEMAReady();
          return currentFileSize >= rolloverTriggeringSizeInBytes;
        }
      }
      return false;
    }

    @Override
    public void signalRollover() {
      initialFileSize = 0;
      recordsInCurrentFile = 0;
    }

    private boolean recordsHaveBeenWrittenToDisk(long currentFileSize) {
      return currentFileSize > initialFileSize + SCHEMA_SIZE_EPSILON;
    }

    // Base case of EMA
    private void calculateInitialInstantRate(long currentFileSize) {
      writeRate = (currentFileSize - initialFileSize) / (double) recordsInCurrentFile;
    }

    private void estimateTotalNumberOfRecordsInFile(long currentFileSize) {
      recordsInCurrentFile = (int) (currentFileSize / writeRate);
    }

    private void switchToEMAReady() {
      delegateImplementation = new EMAReady();
    }
  }

  private class EMAReady implements RolloverShouldHappen {

    @Override
    public boolean sizeThresholdHasBeenHit() throws IOException {
      if (predictedRollReached()) {
        return Files.size(avroFileName) >= rolloverTriggeringSizeInBytes;
      }
      return false;
    }

    @Override
    public void signalRollover() throws IOException {
      updateWriteRate(Files.size(avroFileName));
      recordsInCurrentFile = 0;
    }

    private boolean predictedRollReached() {
      return ++recordsInCurrentFile * writeRate >= rolloverTriggeringSizeInBytes;
    }

    // Recursive case of EMA
    private void updateWriteRate(long currentFileSize) {
      final double instantWriteRate = currentFileSize / (double) recordsInCurrentFile;
      writeRate = DECAY_RATE * instantWriteRate + DECAY_RATE_COMPLEMENT * writeRate;
    }
  }
}
