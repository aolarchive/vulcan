package com.aol.advertising.dmp.disruptor.rolling;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

class UTCDateTime {

  static DateTime now() {
    return DateTime.now(DateTimeZone.UTC);
  }

  static DateTime beginningOfToday() {
    return new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay();
  }

}
