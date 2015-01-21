package com.aol.advertising.dmp.disruptor.writer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.aol.advertising.dmp.disruptor.api.RollingPolicy;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEvent;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Files.class, DataFileReader.class, AvroEventConsumer.class})
public class AvroEventConsumerTest {

  private AvroEventConsumer avroEventConsumerUnderTest;

  @Mock
  private Path avroFileNameMock;
  @Mock
  private File avroFileNameAsFileMock;
  @Mock
  private Schema avroSchemaMock;
  @Mock
  private Schema existingFileSchemaMock;
  @Mock
  private RollingPolicy rollingPolicyMock;
  @Mock
  private Path rolledFileNameMock;
  @Mock
  private DataFileWriter<SpecificRecord> avroFileWriterMock;
  @Mock
  private DataFileReader<SpecificRecord> fileReaderMock;
  @Mock
  private AvroEvent avroEventMock;
  @Mock
  private SpecificRecord avroRecordMock;

  @Before
  public void setUp() throws Exception {
    initMocks();
    wireUpMocks();
    
    avroEventConsumerUnderTest = new AvroEventConsumer(avroFileNameMock, avroSchemaMock, rollingPolicyMock);
  }
  
  private void initMocks() throws Exception {
    MockitoAnnotations.initMocks(this);
    mockStatic(Files.class);

    whenNew(DataFileWriter.class).withAnyArguments().thenReturn(avroFileWriterMock);
    whenNew(DataFileReader.class).withParameterTypes(File.class, DatumReader.class)
                                 .withArguments(eq(avroFileNameAsFileMock), any(GenericDatumReader.class))
                                 .thenReturn(fileReaderMock);
  }
  
  private void wireUpMocks() {
    when(avroFileNameMock.toFile()).thenReturn(avroFileNameAsFileMock);
    when(fileReaderMock.getSchema()).thenReturn(existingFileSchemaMock);
    when(rollingPolicyMock.getNextRolledFileName(avroFileNameMock)).thenReturn(rolledFileNameMock);
    when(avroEventMock.getAvroRecord()).thenReturn(avroRecordMock);
  }

  @Test
  public void whenConsumerIsStarted_andDestinationFileExists_andSchemasDiffer_thenExistingFileIsRolled() throws Exception {
    givenDestinationFileExists();
    givenSchemasDiffer();

    avroEventConsumerUnderTest.onStart();

    thenExistingFileIsRolled();
  }
  
  @Test
  public void whenConsumerIsStarted_andDestinationFileExists_andSchemasAreEqual_thenExistingFileIsUsedForWriting() throws Exception {
    givenDestinationFileExists();
    givenSchemasAreEqual();

    avroEventConsumerUnderTest.onStart();

    thenExistingFileIsUsedForWriting();
  }

  @Test
  public void whenConsumerIsShutdown_thenWriterIsCleanedUp() throws Exception {
    givenAnInitializedEventConsumer();

    avroEventConsumerUnderTest.onShutdown();
    
    verify(avroFileWriterMock).close();
  }

  @Test
  public void whenAnEventIsReceived_thenTheAvroRecordIsWrittenToTheDestinationFile() throws Exception {
    givenAnInitializedEventConsumer();

    avroEventConsumerUnderTest.onEvent(avroEventMock, -1, false);

    thenTheAvroRecordIsWrittenToTheDestinationFile();
  }
  
  @Test
  public void whenAnEventIsReceived_andRolloverIsDue_thenExistingFileIsRolled() throws Exception {
    givenAnInitializedEventConsumer();
    givenRollIsDue();

    avroEventConsumerUnderTest.onEvent(avroEventMock, -1, false);

    thenExistingFileIsRolled();
  }
  
  @Test
  public void whenAnEventIsReceived_andItIsAnEndOfBatch_thenRecordsAreWrittenToDisk() throws Exception {
    givenAnInitializedEventConsumer();

    avroEventConsumerUnderTest.onEvent(avroEventMock, -1, true);

    thenRecordsAreWrittenToDisk();
  }

  private void givenDestinationFileExists() {
    when(Files.exists(avroFileNameMock)).thenReturn(true);
  }

  private void givenSchemasDiffer() {
    // Nothing to do
  }
  
  private void givenSchemasAreEqual() {
    when(fileReaderMock.getSchema()).thenReturn(avroSchemaMock);
  }

  private void givenAnInitializedEventConsumer() {
    avroEventConsumerUnderTest.onStart();
  }

  private void givenRollIsDue() {
    when(rollingPolicyMock.shouldRollover(avroFileNameMock, avroRecordMock)).thenReturn(true);
  }

  private void thenExistingFileIsRolled() throws Exception {
    verifyWriterRewiring();
    verifyFileIsRenamed();
    verifyRollEventIsNotifiedToPolicy();
  }

  private void verifyWriterRewiring() throws Exception {
    final InOrder rewiringOrder = inOrder(avroFileWriterMock);
    rewiringOrder.verify(avroFileWriterMock).close();
    rewiringOrder.verify(avroFileWriterMock).create(avroSchemaMock, avroFileNameAsFileMock);
  }

  private void verifyFileIsRenamed() throws Exception {
    verifyStatic();
    Files.move(avroFileNameMock, rolledFileNameMock, StandardCopyOption.REPLACE_EXISTING);
  }

  private void verifyRollEventIsNotifiedToPolicy() {
    verify(rollingPolicyMock).signalRolloverOf(avroFileNameMock);
  }

  private void thenExistingFileIsUsedForWriting() throws Exception {
    verify(avroFileWriterMock).appendTo(avroFileNameAsFileMock);
  }

  private void thenTheAvroRecordIsWrittenToTheDestinationFile() throws Exception {
    verify(avroFileWriterMock).append(avroRecordMock);
  }

  private void thenRecordsAreWrittenToDisk() throws Exception {
    verify(avroFileWriterMock).flush();
  }

}
