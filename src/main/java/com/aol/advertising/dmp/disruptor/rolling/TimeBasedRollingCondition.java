package com.aol.advertising.dmp.disruptor.rolling;

import org.joda.time.DateTime;

class TimeBasedRollingCondition {

  private DateTime lastRolloverDate;

  TimeBasedRollingCondition() {
    lastRolloverDate = DateTime.now();
  }

  boolean rolloverShouldHappen() {
    return lastRolloverHappenedBeforeToday();
  }

  void signalRollover() {
    lastRolloverDate = DateTime.now();
  }

  private boolean lastRolloverHappenedBeforeToday() {
    return lastRolloverDate.isBefore(new DateTime().withTimeAtStartOfDay());
  }

}
