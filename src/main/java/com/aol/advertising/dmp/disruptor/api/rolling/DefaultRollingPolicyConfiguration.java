package com.aol.advertising.dmp.disruptor.api.rolling;

public class DefaultRollingPolicyConfiguration {

  private int rollingSizeInMb;

  public DefaultRollingPolicyConfiguration() {
    rollingSizeInMb = 50;
  }

  public DefaultRollingPolicyConfiguration withFileRollingSizeOf(final int rollingSizeInMb) {
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
