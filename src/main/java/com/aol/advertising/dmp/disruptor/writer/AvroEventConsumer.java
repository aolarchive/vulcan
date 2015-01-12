package com.aol.advertising.dmp.disruptor.writer;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.dmp.disruptor.api.RollingPolicy;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public class AvroEventConsumer implements EventHandler<AvroEvent>, LifecycleAware {
  
  private static final int TWO_MB_IN_BYTES = 2097152;

  private final File avroFileName;
  private final Schema avroSchema;
  private final RollingPolicy rollingPolicy;
  private final SpecificDatumWriter<SpecificRecord> datumWriter;

  private DataFileWriter<SpecificRecord> avroFileWriter;

  public AvroEventConsumer(final File avroFileName, final Schema avroSchema, final RollingPolicy rollingPolicy) {
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
      writeToDisk();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void onEvent(final AvroEvent event, long _, boolean endOfBatch) throws Exception {
    final SpecificRecord avroRecord = event.getAvroRecord();
    avroFileWriter.append(avroRecord);
    if (endOfBatch) {
      writeToDisk();
      applyRollingPolicy(avroRecord);
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
    if (avroFileName.exists()) {
      ensureBindingToAFileWithConfiguredSchema();
    } else {
      avroFileWriter.create(avroSchema, avroFileName);
    }
  }

  private void ensureBindingToAFileWithConfiguredSchema() throws IOException {
    if (schemasDiffer()) {
      rollFile();
    } else {
      avroFileWriter.appendTo(avroFileName);
    }
  }
  
  private boolean schemasDiffer() throws IOException {
    return ! new DataFileReader<>(avroFileName, new GenericDatumReader<SpecificRecord>()).getSchema().equals(avroSchema);
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
    avroFileWriter.close();
    renameAvroFileTo(rollingPolicy.getNextRolledFileName(avroFileName));
    rewireWriter();
  }

  private void renameAvroFileTo(final String rolledFileName) {
    final File disposableFileObjectForRenaming = copyAvroFileObject();
    final boolean renameSucceeded = disposableFileObjectForRenaming.renameTo(new File(rolledFileName));
    if (!renameSucceeded) {
      throw new RuntimeException("File \"" + rolledFileName + "\" could not be used to roll the current output Avro file");
    }
  }
  
  private File copyAvroFileObject() {
    return avroFileName.toPath().toFile();
  }

  private void rewireWriter() throws IOException {
    getNewFileWriter();
    avroFileWriter.create(avroSchema, avroFileName);
  }

}
