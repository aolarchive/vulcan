package com.aol.advertising.vulcan.writer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.vulcan.api.rolling.RollingPolicy;
import com.aol.advertising.vulcan.exception.FileRollingException;
import com.aol.advertising.vulcan.ringbuffer.AvroEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public class AvroEventConsumer implements EventHandler<AvroEvent>, LifecycleAware {

  private static final int TWO_MB_IN_BYTES = 2_097_152;

  private final Path avroFilename;
  private final Schema avroSchema;
  private final RollingPolicy rollingPolicy;
  private final SpecificDatumWriter<SpecificRecord> datumWriter;

  private DataFileWriter<SpecificRecord> avroFileWriter;

  public AvroEventConsumer(Path avroFilename, Schema avroSchema, RollingPolicy rollingPolicy) {
    this.avroFilename = avroFilename;
    this.avroSchema = avroSchema;
    this.rollingPolicy = rollingPolicy;
    this.datumWriter = new SpecificDatumWriter<>(avroSchema);
  }

  @Override
  public void onStart() {
    initializeWriter();
  }

  @Override
  public void onShutdown() {
    try {
      avroFileWriter.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onEvent(AvroEvent event, long sequence, boolean endOfBatch) throws Exception {
    SpecificRecord avroRecord = event.getAvroRecord();
    avroFileWriter.append(avroRecord);
    applyRollingPolicy(avroRecord);
    if (endOfBatch) {
      writeToDisk();
    }
  }

  private void initializeWriter() {
    try {
      tryToInitializeWriter();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void tryToInitializeWriter() throws IOException {
    getNewFileWriter();
    bindWriterToAvroFile();
  }

  private void getNewFileWriter() {
    avroFileWriter = new DataFileWriter<>(datumWriter);
    avroFileWriter.setSyncInterval(TWO_MB_IN_BYTES);
  }

  private void bindWriterToAvroFile() throws IOException {
    if (Files.exists(avroFilename)) {
      ensureBindingToAFileWithConfiguredSchema();
    } else {
      avroFileWriter.create(avroSchema, avroFilename.toFile());
    }
  }

  private void ensureBindingToAFileWithConfiguredSchema() throws IOException {
    if (schemasDiffer()) {
      rollFile();
    } else {
      avroFileWriter.appendTo(avroFilename.toFile());
    }
  }

  private boolean schemasDiffer() throws IOException {
    return !readSchemaFromExistingFile().equals(avroSchema);
  }

  private Schema readSchemaFromExistingFile() {
    try (DataFileReader<SpecificRecord> fileReader =
        new DataFileReader<>(avroFilename.toFile(), new GenericDatumReader<SpecificRecord>())) {
      return fileReader.getSchema();
    } catch (IOException ioe) {
      return Schema.create(Type.NULL);
    }
  }

  private void applyRollingPolicy(SpecificRecord avroRecord) throws IOException {
    if (rollingPolicy.shouldRollover(avroRecord)) {
      rollFile();
    }
  }

  private void rollFile() throws IOException {
    try {
      tryToRollFile();
    } catch (FileRollingException e) {
      avroFileWriter.appendTo(avroFilename.toFile());
      throw new IOException("Failed to do rollover, new events will still be written to old file", e);
    }
  }

  private void tryToRollFile() throws IOException {
    refreshWriter();
    rollingPolicy.rolloverAvroFile();
    avroFileWriter.create(avroSchema, avroFilename.toFile());
  }

  private void refreshWriter() throws IOException {
    avroFileWriter.close();
    getNewFileWriter();
  }

  private void writeToDisk() throws IOException {
    avroFileWriter.flush();
  }
}
