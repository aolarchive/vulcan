package com.aol.advertising.dmp.disruptor.api.builder;

import java.io.File;

public interface MandatorySteps {

  OptionalSteps thatWritesTo(final File avroFilename) throws IllegalArgumentException;

}