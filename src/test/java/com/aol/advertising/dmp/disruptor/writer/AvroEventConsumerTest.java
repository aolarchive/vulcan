package com.aol.advertising.dmp.disruptor.writer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.aol.advertising.dmp.disruptor.api.RollingPolicy;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEvent;
import com.aol.advertising.dmp.disruptor.utils.FilesOpsFacade;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataFileReader.class, AvroEventConsumer.class})
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
  @Mock
  private FilesOpsFacade filesOpsFacadeMock;

  @Before
  public void setUp() throws Exception {
    initMocks();
    wireUpMocks();
    
    avroEventConsumerUnderTest = new AvroEventConsumer(avroFileNameMock, avroSchemaMock, rollingPolicyMock);
  }
  
  private void initMocks() throws Exception {
    MockitoAnnotations.initMocks(this);

    whenNew(DataFileWriter.class).withAnyArguments().thenReturn(avroFileWriterMock);
    whenNew(DataFileReader.class).withParameterTypes(File.class, DatumReader.class)
                                 .withArguments(eq(avroFileNameAsFileMock), any(GenericDatumReader.class))
                                 .thenReturn(fileReaderMock);
    FilesOpsFacade.facadeInstance = filesOpsFacadeMock;
  }
  
  private void wireUpMocks() {
    when(avroFileNameMock.toFile()).thenReturn(avroFileNameAsFileMock);
    when(fileReaderMock.getSchema()).thenReturn(existingFileSchemaMock);
    when(rollingPolicyMock.getNextRolledFileName(avroFileNameMock)).thenReturn(rolledFileNameMock);
    when(avroEventMock.getAvroRecord()).thenReturn(avroRecordMock);
  }

  @After
  public void tearDown() {
    FilesOpsFacade.facadeInstance = new FilesOpsFacade();
  }
  
  @Test
  public void whenConsumerIsStarted_andDestinationFileDoesNotExist_thenANewFileIsUsedForWriting() throws Exception {
    givenDestinationFileDoesNotExist();

    avroEventConsumerUnderTest.onStart();

    thenANewFileIsUsedForWriting();
  }

  @Test
  public void whenConsumerIsStarted_andDestinationFileExists_andSchemasDiffer_thenExistingFileIsRolled() throws Exception {
    givenDestinationFileExists();
    givenSchemasDiffer();
    givenTheFileCanBeMovedSuccessfullyInTheFileSystem();

    avroEventConsumerUnderTest.onStart();

    thenExistingFileIsRolled();
  }
  
  @Test
  public void whenConsumerIsStarted_andDestinationFileExistsButCannotBeMoved_andSchemasDiffer_thenExistingFileIsUsedForWriting() throws Exception {
    givenDestinationFileExists();
    givenSchemasDiffer();
    givenTheFileCannotBeMovedSuccessfullyInTheFileSystem();

    avroEventConsumerUnderTest.onStart();

    thenExistingFileIsUsedForWriting();
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
  public void whenAnEventIsReceived_andRolloverIsDue_andTheFileCouldBeMovedSuccessfully_thenExistingFileIsRolled() throws Exception {
    givenAnInitializedEventConsumer();
    givenRollIsDue();
    givenTheFileCanBeMovedSuccessfullyInTheFileSystem();

    avroEventConsumerUnderTest.onEvent(avroEventMock, -1, false);

    thenExistingFileIsRolled();
  }
  
  @Test
  public void whenAnEventIsReceived_andRolloverIsDue_andTheFileCouldNotBeMovedSuccessfully_thenWriterKeepsWritingToExistingFile() throws Exception {
    givenAnInitializedEventConsumer();
    givenRollIsDue();
    givenTheFileCannotBeMovedSuccessfullyInTheFileSystem();

    avroEventConsumerUnderTest.onEvent(avroEventMock, -1, false);

    thenWriterKeepsUsingExistingFileToWrite();
  }
  
  @Test
  public void whenAnEventIsReceived_andItIsAnEndOfBatch_thenRecordsAreWrittenToDisk() throws Exception {
    givenAnInitializedEventConsumer();

    avroEventConsumerUnderTest.onEvent(avroEventMock, -1, true);

    thenRecordsAreWrittenToDisk();
  }

  private void givenDestinationFileExists() {
    when(filesOpsFacadeMock.exists(avroFileNameMock)).thenReturn(true);
  }

  private void givenDestinationFileDoesNotExist() {
    when(filesOpsFacadeMock.exists(avroFileNameMock)).thenReturn(false);
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

  private void givenTheFileCanBeMovedSuccessfullyInTheFileSystem() throws Exception {
    final Answer<Void> filedMoved = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock _) throws Throwable {
        when(filesOpsFacadeMock.exists(avroFileNameMock)).thenReturn(false);
        return null;
      }
    };
    doAnswer(filedMoved).when(filesOpsFacadeMock).move(avroFileNameMock, rolledFileNameMock, StandardCopyOption.REPLACE_EXISTING);
  }

  private void givenTheFileCannotBeMovedSuccessfullyInTheFileSystem() throws Exception {
    final Answer<Void> filedNotMoved = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock _) throws Throwable {
        when(filesOpsFacadeMock.exists(avroFileNameMock)).thenReturn(true);
        return null;
      }
    };
    doAnswer(filedNotMoved).when(filesOpsFacadeMock).move(avroFileNameMock, rolledFileNameMock, StandardCopyOption.REPLACE_EXISTING);
  }

  private void thenExistingFileIsRolled() throws Exception {
    verifyFileIsRenamed();
    verifyWriterRewiringToNewFile();
  }

  private void thenWriterKeepsUsingExistingFileToWrite() throws Exception {
    verifyWriterRewiringToExistingFile();
  }

  private void verifyWriterRewiringToNewFile() throws Exception {
    final InOrder rewiringOrder = inOrder(avroFileWriterMock);
    rewiringOrder.verify(avroFileWriterMock).close();
    rewiringOrder.verify(avroFileWriterMock).create(avroSchemaMock, avroFileNameAsFileMock);
  }

  private void verifyWriterRewiringToExistingFile() throws Exception {
    final InOrder rewiringOrder = inOrder(avroFileWriterMock);
    rewiringOrder.verify(avroFileWriterMock).close();
    rewiringOrder.verify(avroFileWriterMock).appendTo(avroFileNameAsFileMock);
  }

  private void verifyFileIsRenamed() throws Exception {
    verify(filesOpsFacadeMock).move(avroFileNameMock, rolledFileNameMock, StandardCopyOption.REPLACE_EXISTING);
  }
  
  private void thenANewFileIsUsedForWriting() throws Exception {
    verify(avroFileWriterMock).create(avroSchemaMock, avroFileNameAsFileMock);
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
