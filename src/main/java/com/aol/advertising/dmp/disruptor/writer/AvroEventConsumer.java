package com.aol.advertising.dmp.disruptor.writer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.dmp.disruptor.api.RollingPolicy;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEvent;
import com.aol.advertising.dmp.disruptor.utils.FilesOpsFacade;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public class AvroEventConsumer implements EventHandler<AvroEvent>, LifecycleAware {
  
  private static final int TWO_MB_IN_BYTES = 2_097_152;

  private final Path avroFileName;
  private final Schema avroSchema;
  private final RollingPolicy rollingPolicy;
  private final SpecificDatumWriter<SpecificRecord> datumWriter;

  private DataFileWriter<SpecificRecord> avroFileWriter;

  public AvroEventConsumer(final Path avroFileName, final Schema avroSchema, final RollingPolicy rollingPolicy) {
    this.avroFileName = avroFileName;
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
  public void onEvent(final AvroEvent event, long _, boolean endOfBatch) throws Exception {
    final SpecificRecord avroRecord = event.getAvroRecord();
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
    if (FilesOpsFacade.facadeInstance.exists(avroFileName)) {
      ensureBindingToAFileWithConfiguredSchema();
    } else {
      avroFileWriter.create(avroSchema, avroFileName.toFile());
    }
  }

  private void ensureBindingToAFileWithConfiguredSchema() throws IOException {
    if (schemasDiffer()) {
      rollFile();
    } else {
      avroFileWriter.appendTo(avroFileName.toFile());
    }
  }
  
  private boolean schemasDiffer() throws IOException {
    final DataFileReader<SpecificRecord> fileReader = new DataFileReader<>(avroFileName.toFile(), new GenericDatumReader<SpecificRecord>());
    final Schema existingFileSchema = fileReader.getSchema();
    fileReader.close();
    return !existingFileSchema.equals(avroSchema);
  }

  private void writeToDisk() throws IOException {
    avroFileWriter.flush();
  }

  private void applyRollingPolicy(final SpecificRecord avroRecord) throws IOException {
    if (rollingPolicy.shouldRollover(avroFileName, avroRecord)) {
      rollFile();
    }
  }

  private void rollFile() throws IOException {
    try {
      avroFileWriter.close();
      renameAvroFile();
    } finally {
      rewireWriter();
    }
  }

  private void renameAvroFile() {
    final Path nextRolledFileName = rollingPolicy.getNextRolledFileName(avroFileName);
    try {
      FilesOpsFacade.facadeInstance.move(avroFileName, nextRolledFileName, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException("File \"" + nextRolledFileName + "\" could not be used to roll the current output Avro file");
    }
  }

  private void rewireWriter() throws IOException {
    getNewFileWriter();
    if (FilesOpsFacade.facadeInstance.exists(avroFileName)) {
      avroFileWriter.appendTo(avroFileName.toFile());
    } else {
      avroFileWriter.create(avroSchema, avroFileName.toFile());
    }
  }
}
