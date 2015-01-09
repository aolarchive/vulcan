package com.aol.advertising.dmp.disruptor.ringbuffer;

import org.apache.avro.specific.SpecificRecord;

public class AvroEvent {

  private SpecificRecord avroRecord;

  public SpecificRecord getAvroRecord() {
    return avroRecord;
  }

  public void setAvroRecord(final SpecificRecord avroRecord) {
    this.avroRecord = avroRecord;
  }

}
