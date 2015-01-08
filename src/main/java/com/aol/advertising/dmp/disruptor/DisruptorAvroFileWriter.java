package com.aol.advertising.dmp.disruptor;

import org.apache.avro.specific.SpecificRecord;

public interface DisruptorAvroFileWriter extends AutoCloseable {

  void write(final SpecificRecord avroRecord);

}
