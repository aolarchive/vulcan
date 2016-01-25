package com.aol.advertising.vulcan.writer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.aol.advertising.vulcan.ConfiguredUnitTest;
import com.aol.advertising.vulcan.api.rolling.RollingPolicy;
import com.aol.advertising.vulcan.exception.FileRollingException;
import com.aol.advertising.vulcan.ringbuffer.AvroEvent;
import com.aol.advertising.vulcan.writer.AvroEventConsumer;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataFileReader.class, AvroEventConsumer.class})
public class AvroEventConsumerTest extends ConfiguredUnitTest {

  private AvroEventConsumer avroEventConsumerUnderTest;

  @Mock
  private Schema avroSchemaMock;
  @Mock
  private Schema existingFileSchemaMock;
  @Mock
  private RollingPolicy rollingPolicyMock;
  @Mock
  private DataFileWriter<SpecificRecord> avroFileWriterMock;
  @Mock
  private DataFileReader<SpecificRecord> fileReaderMock;
  @Mock
  private AvroEvent avroEventMock;
  @Mock
  private SpecificRecord avroRecordMock;

  private File testAvroFile;
  private Path testAvroPath;

  @Rule
  public TemporaryFolder testDirectory = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    initTestData();
    initMocks();
    wireUpMocks();

    avroEventConsumerUnderTest = new AvroEventConsumer(testAvroPath, avroSchemaMock, rollingPolicyMock);
  }

  private void initTestData() throws IOException {
    initTestAvroFileTo(testDirectory.newFile());
  }

  private void initTestAvroFileTo(File avroFile) {
    testAvroFile = avroFile;
    testAvroPath = testAvroFile.toPath();
  }

  private void initMocks() throws Exception {
    whenNew(DataFileWriter.class).withAnyArguments()
                                 .thenReturn(avroFileWriterMock);
    whenNew(DataFileReader.class).withParameterTypes(File.class, DatumReader.class)
                                 .withArguments(eq(testAvroFile), any(GenericDatumReader.class))
                                 .thenReturn(fileReaderMock);
  }

  private void wireUpMocks() {
    when(fileReaderMock.getSchema()).thenReturn(existingFileSchemaMock);
    when(avroEventMock.getAvroRecord()).thenReturn(avroRecordMock);
  }

  @Test
  public void whenConsumerIsStarted_andDestinationFileDoesNotExist_thenANewFileIsUsedForWriting() throws Exception {
    givenDestinationFileDoesNotExist();

    avroEventConsumerUnderTest.onStart();

    thenANewFileIsUsedForWriting();
  }

  @Test
  public void whenConsumerIsStarted_andDestinationFileExists_andSchemasDiffer_thenExistingFileIsRolled()
      throws Exception {
    givenDestinationFileExists();
    givenSchemasDiffer();
    givenTheFileCanBeRolled();

    avroEventConsumerUnderTest.onStart();

    thenExistingFileIsRolled();
  }

  @Test(expected = RuntimeException.class)
  public void whenConsumerIsStarted_andDestinationFileExists_andSchemasDiffer_andFileCannotBeRolled_thenAnExceptionIsThrown()
      throws Exception {
    givenDestinationFileExists();
    givenSchemasDiffer();
    givenTheFileCannotBeRolled();

    avroEventConsumerUnderTest.onStart();
  }

  @Test
  public void whenConsumerIsStarted_andDestinationFileExists_andSchemasDiffer_andFileCannotBeRolled_thenExistingFileIsUsedForWriting()
      throws Exception {
    givenDestinationFileExists();
    givenSchemasDiffer();
    givenTheFileCannotBeRolled();

    try {
      avroEventConsumerUnderTest.onStart();
      Assert.fail();
    } catch (RuntimeException e) {
      thenExistingFileIsUsedForWriting();
    }
  }

  @Test
  public void whenConsumerIsStarted_andDestinationFileExists_andSchemasAreEqual_thenExistingFileIsUsedForWriting()
      throws Exception {
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
    givenTheFileCanBeRolled();

    avroEventConsumerUnderTest.onEvent(avroEventMock, -1, false);

    thenExistingFileIsRolled();
  }

  @Test(expected = IOException.class)
  public void whenAnEventIsReceived_andRolloverIsDue_andTheFileCannotBeRolled_thenAnExceptionIsThrown()
      throws Exception {
    givenAnInitializedEventConsumer();
    givenRollIsDue();
    givenTheFileCannotBeRolled();

    avroEventConsumerUnderTest.onEvent(avroEventMock, -1, false);
  }

  @Test
  public void whenAnEventIsReceived_andRolloverIsDue_andTheFileCannotBeRolled_thenWriterKeepsWritingToExistingFile()
      throws Exception {
    givenAnInitializedEventConsumer();
    givenRollIsDue();
    givenTheFileCannotBeRolled();

    try {
      avroEventConsumerUnderTest.onEvent(avroEventMock, -1, false);
      Assert.fail();
    } catch (IOException e) {
      thenWriterKeepsUsingExistingFileToWrite();
    }
  }

  @Test
  public void whenAnEventIsReceived_andItIsAnEndOfBatch_thenRecordsAreWrittenToDisk() throws Exception {
    givenAnInitializedEventConsumer();

    avroEventConsumerUnderTest.onEvent(avroEventMock, -1, true);

    thenRecordsAreWrittenToDisk();
  }

  private void givenDestinationFileExists() {
    avroEventConsumerUnderTest = new AvroEventConsumer(testAvroPath, avroSchemaMock, rollingPolicyMock);
  }

  private void givenDestinationFileDoesNotExist() {
    initTestAvroFileTo(new File(testDirectory.getRoot(), "I am nothing" + System.currentTimeMillis()));
    avroEventConsumerUnderTest = new AvroEventConsumer(testAvroPath, avroSchemaMock, rollingPolicyMock);
  }

  private void givenSchemasDiffer() {
    when(fileReaderMock.getSchema()).thenReturn(Schema.create(Type.NULL));
  }

  private void givenSchemasAreEqual() {
    when(fileReaderMock.getSchema()).thenReturn(avroSchemaMock);
  }

  private void givenAnInitializedEventConsumer() {
    givenDestinationFileExists();
    givenSchemasAreEqual();
    avroEventConsumerUnderTest.onStart();
  }

  private void givenRollIsDue() {
    when(rollingPolicyMock.shouldRollover(avroRecordMock)).thenReturn(true);
  }

  private void givenTheFileCanBeRolled() throws Exception {
    testDirectory.getRoot().setWritable(true);
    Answer<Void> deleteTestAvroFile = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock _) throws Throwable {
        testAvroFile.delete();
        return null;
      }
    };
    doAnswer(deleteTestAvroFile).when(rollingPolicyMock).rolloverAvroFile();
  }

  private void givenTheFileCannotBeRolled() throws FileRollingException {
    doThrow(new FileRollingException("boom")).when(rollingPolicyMock).rolloverAvroFile();
  }

  private void thenExistingFileIsRolled() throws Exception {
    verifyFileIsRenamed();
    verifyWriterRewiringToNewFile();
  }

  private void thenWriterKeepsUsingExistingFileToWrite() throws Exception {
    verifyWriterRewiringToExistingFile();
  }

  private void verifyWriterRewiringToNewFile() throws Exception {
    InOrder rewiringOrder = inOrder(avroFileWriterMock);
    rewiringOrder.verify(avroFileWriterMock).close();
    rewiringOrder.verify(avroFileWriterMock).create(avroSchemaMock, testAvroFile);
  }

  private void verifyWriterRewiringToExistingFile() throws Exception {
    InOrder rewiringOrder = inOrder(avroFileWriterMock);
    rewiringOrder.verify(avroFileWriterMock).close();
    rewiringOrder.verify(avroFileWriterMock).appendTo(testAvroFile);
  }

  private void verifyFileIsRenamed() throws Exception {
    assertThat(testAvroFile.exists(), is(equalTo(false)));
  }

  private void thenANewFileIsUsedForWriting() throws Exception {
    verify(avroFileWriterMock).create(avroSchemaMock, testAvroFile);
  }

  private void thenExistingFileIsUsedForWriting() throws Exception {
    verify(avroFileWriterMock).appendTo(testAvroFile);
  }

  private void thenTheAvroRecordIsWrittenToTheDestinationFile() throws Exception {
    verify(avroFileWriterMock).append(avroRecordMock);
  }

  private void thenRecordsAreWrittenToDisk() throws Exception {
    verify(avroFileWriterMock).flush();
  }
}
