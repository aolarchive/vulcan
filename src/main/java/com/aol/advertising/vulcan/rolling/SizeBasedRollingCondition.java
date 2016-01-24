package com.aol.advertising.vulcan.rolling;

import static java.lang.Math.max;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.aol.advertising.vulcan.rolling.RollingCondition.FileAwareRollingCondition;

/**
 * Accessing the disk to check the file size is a costly operation. This class uses a simple pseudo-
 * <a href="https://en.wikipedia.org/wiki/PID_controller">PID controller</a> to try to predict
 * rolling points while minimizing the number of reads from disk
 * 
 * @author Jaime Nuche
 *
 */
class SizeBasedRollingCondition implements FileAwareRollingCondition {

  private static final int ONE_MB_IN_BYTES = 1_048_576;

  private final long rolloverTriggeringSizeInBytes;

  private Path avroFilename;
  private RollingCondition loadedImplementation;

  SizeBasedRollingCondition(int rolloverTriggeringSizeInMb) {
    this.rolloverTriggeringSizeInBytes = rolloverTriggeringSizeInMb * ONE_MB_IN_BYTES;
  }

  @Override
  public void registerAvroFileName(Path avroFilename) {
    this.avroFilename = avroFilename;
    this.loadedImplementation = new Warmup();
  }

  @Override
  public boolean shouldRollover() {
    return loadedImplementation.shouldRollover();
  }

  @Override
  public void signalRollover() {
    loadedImplementation.signalRollover();
  }

  private long getAvroFileSize() {
    try {
      return Files.size(avroFilename);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private class Warmup implements RollingCondition {

    private static final int WARMUP_FIXED_SAMPLING_RATE = 150;
    private static final int SCHEMA_SIZE_EPSILON = 7000;

    private long initialFileSize;
    private long currentFileSize;
    private int recordsWritten;

    private Warmup() {
      initialFileSize = getInitialFileSize();
      currentFileSize = 0;
      recordsWritten = 0;
    }

    private long getInitialFileSize() {
      if (Files.exists(avroFilename)) {
        return getAvroFileSize();
      } else {
        return 0;
      }
    }

    @Override
    public boolean shouldRollover() {
      if (checkpointReached()) {
        currentFileSize = getAvroFileSize();
        if (recordsHaveBeenWrittenToDisk()) {
          switchToPseudoPidController();
          return currentFileSize >= rolloverTriggeringSizeInBytes;
        }
      }
      return false;
    }

    @Override
    public void signalRollover() {}

    private boolean checkpointReached() {
      return (++recordsWritten % WARMUP_FIXED_SAMPLING_RATE) == 0;
    }

    private boolean recordsHaveBeenWrittenToDisk() {
      return currentFileSize > initialFileSize + SCHEMA_SIZE_EPSILON;
    }

    private void switchToPseudoPidController() {
      loadedImplementation = new PseudoPidController(getInitialWriteRate(), currentFileSize);
    }

    private double getInitialWriteRate() {
      return (currentFileSize - initialFileSize) / (double) recordsWritten;
    }
  }

  private class PseudoPidController implements RollingCondition {

    private static final int RELATIVE_SAMPLING_RATE = 32;

    private final long targetCheckpointDistance;
    private final long rollingPointEpsilon;

    private double predictedWriteRate;
    private double previousPredictedWriteRate;
    private double previousWriteRate;
    private long currentFileSize;
    private long previousFileSize;
    private int recordsWrittenInCurrentPeriod;
    private long distanceToNextCheckpoint;

    private PseudoPidController(double previousWriteRate, long previousFileSize) {
      // The controller will aim at sampling/checking RELATIVE_SAMPLING_RATE times between rolling points
      this.targetCheckpointDistance = rolloverTriggeringSizeInBytes / RELATIVE_SAMPLING_RATE;
      // Tries to minimize how much the controller will over/undershoot the rolling point
      this.rollingPointEpsilon = targetCheckpointDistance / 2;

      this.predictedWriteRate = previousWriteRate;
      this.previousPredictedWriteRate = previousWriteRate;
      this.previousWriteRate = 0.0;
      this.currentFileSize = 0;
      this.previousFileSize = previousFileSize;
      this.recordsWrittenInCurrentPeriod = 0;
      this.distanceToNextCheckpoint = targetCheckpointDistance;
    }

    @Override
    public boolean shouldRollover() {
      if (checkpointReached()) {
        currentFileSize = getAvroFileSize();
        predictNewWriteRate();
        calculateDistanceToNextCheckpoint();
        prepareNextIteration();
        return currentFileSize >= rolloverTriggeringSizeInBytes - rollingPointEpsilon;
      }
      return false;
    }

    @Override
    public void signalRollover() {
      previousFileSize = 0;
      recordsWrittenInCurrentPeriod = 0;
      distanceToNextCheckpoint = targetCheckpointDistance;
    }

    private boolean checkpointReached() {
      return ++recordsWrittenInCurrentPeriod * predictedWriteRate >= distanceToNextCheckpoint;
    }

    private void predictNewWriteRate() {
      calculatePredictionBasedOnRecentRates();
      limitVariationFloorTo70Pc();
    }

    private void calculatePredictionBasedOnRecentRates() {
      predictedWriteRate = getCurrentWriteRate() + getLocalRateVariation();
    }

    // If the current iteration could not gather any new info (i.e. there was no growth in the file due to buffering), 
    // keep previous write rate
    private double getCurrentWriteRate() {
      double currentWriteRate = getFileSizeIncrement() / (double) recordsWrittenInCurrentPeriod;
      return currentWriteRate > 0 ? currentWriteRate : previousWriteRate;
    }

    private long getFileSizeIncrement() {
      return currentFileSize - previousFileSize;
    }

    // This variation is sort of a "poor man's second derivative". It's based on the assumption that there will be some
    // degree of dependency in the data received. For example, if the size of the data has grown recently, it may continue
    // to grow in the close future at a similar rate
    private double getLocalRateVariation() {
      return getCurrentWriteRate() - previousWriteRate;
    }

    // Limit big prediction drops (more than 30%) to try to achieve two things:
    // * Smoothen out possible drops induced by buffering (counted events may not be in disk yet)
    // * Avoid negative or zero rates
    private void limitVariationFloorTo70Pc() {
      predictedWriteRate = max(predictedWriteRate, 0.7 * previousPredictedWriteRate);
    }

    private void calculateDistanceToNextCheckpoint() {
      distanceToNextCheckpoint = targetCheckpointDistance + getPreviousPredictionError();
      if (overshootingSkippedCheckpoints()) {
        findNextAvailableCheckpointNotSkipped();
      }
    }

    private long getPreviousPredictionError() {
      return distanceToNextCheckpoint - getFileSizeIncrement();
    }

    private boolean overshootingSkippedCheckpoints() {
      return distanceToNextCheckpoint < 0;
    }

    private void findNextAvailableCheckpointNotSkipped() {
      while (distanceToNextCheckpoint < 0) {
        distanceToNextCheckpoint += targetCheckpointDistance;
      }
    }

    private void prepareNextIteration() {
      previousWriteRate = getCurrentWriteRate();
      previousPredictedWriteRate = predictedWriteRate;
      previousFileSize = currentFileSize;
      recordsWrittenInCurrentPeriod = 0;
    }
  }
}
