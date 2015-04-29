package com.aol.advertising.dmp.disruptor.rolling;

public class TimeAndSizeBasedRollingPolicyConfig {

  private int rollingSizeInMb;

  public TimeAndSizeBasedRollingPolicyConfig() {
    rollingSizeInMb = 50;
  }

  public TimeAndSizeBasedRollingPolicyConfig withFileRollingSizeOf(final int rollingSizeInMb) {
    if (rollingSizeInMb <= 0) {
      throw new IllegalArgumentException("File rolling size must be at least 1 MB");
    }
    this.rollingSizeInMb = rollingSizeInMb;
    return this;
  }

  public int getRollingSizeInMb() {
    return rollingSizeInMb;
  }
}
