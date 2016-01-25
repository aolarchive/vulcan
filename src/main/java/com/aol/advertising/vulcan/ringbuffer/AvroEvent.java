package com.aol.advertising.vulcan.ringbuffer;

import org.apache.avro.specific.SpecificRecord;

public class AvroEvent {

  private SpecificRecord avroRecord;

  public SpecificRecord getAvroRecord() {
    return avroRecord;
  }

  public void setAvroRecord(SpecificRecord avroRecord) {
    this.avroRecord = avroRecord;
  }
}
