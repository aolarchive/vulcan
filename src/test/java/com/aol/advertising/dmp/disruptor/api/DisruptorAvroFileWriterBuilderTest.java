package com.aol.advertising.dmp.disruptor.api;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.aol.advertising.dmp.disruptor.api.builder.steps.AvroFileNameStep;
import com.aol.advertising.dmp.disruptor.api.builder.steps.OptionalSteps;
import com.aol.advertising.dmp.disruptor.exception.DisruptorExceptionHandler;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEvent;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEventFactory;
import com.aol.advertising.dmp.disruptor.rolling.TimeAndSizeBasedRollingPolicy;
import com.aol.advertising.dmp.disruptor.writer.AvroEventConsumer;
import com.aol.advertising.dmp.disruptor.writer.AvroEventPublisher;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AvroEventPublisher.class, AvroEventConsumer.class, TimeAndSizeBasedRollingPolicy.class,
                 Disruptor.class, ProducerType.class, Files.class, DisruptorAvroFileWriterBuilder.class})
public class DisruptorAvroFileWriterBuilderTest {

  private AvroFileNameStep disruptorAvroFileWriterBuilderUnderTest;

  @Mock
  private AvroEventPublisher avroEventPublisherMock;
  @Mock
  private Path avroFileNameMock;
  @Mock
  private TimeAndSizeBasedRollingPolicy timeAndSizeBasedRollingPolicyMock;
  @Mock
  private Schema avroSchemaMock;
  @Mock
  private Path parentDirMock;
  @Mock
  private DirectoryStream<Path> directoryStreamMock;
  @Mock
  private Iterator<Path> iteratorMock;
  @Mock
  private Disruptor<AvroEvent> disruptorMock;
  @Mock
  private AvroEventConsumer avroEventConsumerMock;
  @Mock
  private ProducerType producerTypeMock;
  @Mock
  private WaitStrategy waitStrategyMock;
  @Mock
  private RollingPolicy rollingPolicyMock;

  @Before
  public void setUp() throws Exception {
    initMocks();
    wireUpMocks();

    disruptorAvroFileWriterBuilderUnderTest = DisruptorAvroFileWriterBuilder.startCreatingANewWriter();
  }
  
  private void initMocks() throws Exception {
    MockitoAnnotations.initMocks(this);
    mockStatic(Files.class);
    mockConstructors();
    mockPermissions();
  }

  private void mockConstructors() throws Exception {
    whenNew(AvroEventPublisher.class).withAnyArguments().thenReturn(avroEventPublisherMock);
    whenNew(AvroEventConsumer.class).withAnyArguments().thenReturn(avroEventConsumerMock);
    whenNew(TimeAndSizeBasedRollingPolicy.class).withAnyArguments().thenReturn(timeAndSizeBasedRollingPolicyMock);
    whenNew(Disruptor.class).withAnyArguments().thenReturn(disruptorMock);
  }

  private void mockPermissions() {
    mockDestinationFilePermissions();
    mockParentDirPermissions();
  }

  private void mockDestinationFilePermissions() {
    when(Files.isReadable(avroFileNameMock)).thenReturn(true);
    when(Files.isWritable(avroFileNameMock)).thenReturn(true);
  }

  private void mockParentDirPermissions() {
    when(Files.isReadable(parentDirMock)).thenReturn(true);
    when(Files.isWritable(parentDirMock)).thenReturn(true);
    when(Files.isExecutable(parentDirMock)).thenReturn(true);
  }

  private void wireUpMocks() {
    when(avroFileNameMock.getParent()).thenReturn(parentDirMock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void whenANullDestinationFileIsSpecified_thenAnIllegalArgumentExceptionIsThrown() {
    disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void whenTheDestinationFileIsADirectory_thenAnIllegalArgumentExceptionIsThrown() {
    givenDestinationFileIsADirectory();

    disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(avroFileNameMock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void whenTheDestinationFileExists_andIsNotReadable_thenAnIllegalArgumentExceptionIsThrown() {
    givenDestinationFileIsNotReadable();

    disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(avroFileNameMock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void whenTheDestinationFileExists_andIsNotWritable_thenAnIllegalArgumentExceptionIsThrown() {
    givenDestinationFileIsNotWritable();

    disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(avroFileNameMock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void whenTheDestinationFileDoesNotExist_andTheContainingDirectoryIsNotReadable_thenAnIllegalArgumentExceptionIsThrown() {
    givenDestinationParentDirIsNotReadable();
    
    disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(avroFileNameMock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void whenTheDestinationFileDoesNotExist_andTheContainingDirectoryIsNotWritable_thenAnIllegalArgumentExceptionIsThrown() {
    givenDestinationParentDirIsNotWritable();

    disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(avroFileNameMock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void whenTheDestinationFileDoesNotExist_andTheContainingDirectoryIsNotExecutable_thenAnIllegalArgumentExceptionIsThrown() {
    givenDestinationParentDirIsNotExecutable();

    disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(avroFileNameMock);
  }

  @Test
  public void whenTheDestinationFileIsValid_andAWriterIsBuilt_thenTheConfiguredDestinationFileIsUsedToBuildTheWriter() throws Exception {
    givenDestinationFileIsValid();
    
    disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(avroFileNameMock).thatWritesRecordsOf(avroSchemaMock).createNewWriter();

    thenConfiguredDestinationFileIsUsedInTheFinalWriterObject();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void whenANullSchemaIsSpecified_thenAnIllegalArgumentExceptionIsThrown() {
    disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(avroFileNameMock).thatWritesRecordsOf(null);
  }

  @Test
  public void whenTheSchemaIsValid_andAWriterIsBuilt_thenTheConfiguredIsSchemaUsedToBuildTheWriter() throws Exception {
    givenDestinationFileIsValid();
    
    disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(avroFileNameMock).thatWritesRecordsOf(avroSchemaMock).createNewWriter();

    thenConfiguredSchemaIsUsedInTheFinalWriterObject();
  }
  
  @Test
  public void whenNoOptionalStepsAreCalled_thenDefaultsAreUsedToBuildTheWriter() throws Exception {
    final OptionalSteps disruptorAvroFileWriterBuilderUnderTest = givenABuilderWithMandatoryStepsConfigured();

    disruptorAvroFileWriterBuilderUnderTest.createNewWriter();

    thenDefaultsAreUsedInTheFinalWriterObject();
  }
  
  @Test
  public void whenTheRingBufferSizeIsConfigured_thenItIsUsedToBuildTheWriter() throws Exception {
    final OptionalSteps disruptorAvroFileWriterBuilderUnderTest = givenABuilderWithMandatoryStepsConfigured();
    
    disruptorAvroFileWriterBuilderUnderTest.withRingBufferSize(Integer.MAX_VALUE).createNewWriter();

    thenConfiguredBufferSizeIsUsedInTheFinalWriterObject();
  }

  @Test
  public void whenTheProducerTypeIsConfigured_thenItIsUsedToBuildTheWriter() throws Exception {
    final OptionalSteps disruptorAvroFileWriterBuilderUnderTest = givenABuilderWithMandatoryStepsConfigured();
    
    disruptorAvroFileWriterBuilderUnderTest.withProducerType(producerTypeMock).createNewWriter();

    thenConfiguredProducerTypeIsUsedInTheFinalWriterObject();
  }
  
  @Test
  public void whenTheWaitStrategyIsConfigured_thenItIsUsedToBuildTheWriter() throws Exception {
    final OptionalSteps disruptorAvroFileWriterBuilderUnderTest =
        givenABuilderWithMandatoryStepsConfigured();

    disruptorAvroFileWriterBuilderUnderTest.withWaitStrategy(waitStrategyMock).createNewWriter();

    thenConfiguredWriteStrategyIsUsedInTheFinalWriterObject();
  }

  @Test
  public void whenTheRollingPolicyIsConfigured_thenItIsUsedToBuildTheWriter() throws Exception {
    final OptionalSteps disruptorAvroFileWriterBuilderUnderTest = givenABuilderWithMandatoryStepsConfigured();
    
    disruptorAvroFileWriterBuilderUnderTest.withRollingPolicy(rollingPolicyMock).createNewWriter();

    thenConfiguredRollingPolicyIsUsedInTheFinalWriterObject();
  }

  @Test
  public void whenTheWriterIsBuilt_thenExceptionsWithinDisruptorAreHandledWithADisruptorExceptionHandler() throws Exception {
    final OptionalSteps disruptorAvroFileWriterBuilderUnderTest = givenABuilderWithMandatoryStepsConfigured();
    
    disruptorAvroFileWriterBuilderUnderTest.createNewWriter();
    
    verify(disruptorMock).handleExceptionsWith(isA(DisruptorExceptionHandler.class));
  }
  
  @Test
  public void whenTheWriterIsBuilt_thenTheEventsConsumerExecutorIsRegisteredForShutdown() throws Exception {
    final OptionalSteps disruptorAvroFileWriterBuilderUnderTest = givenABuilderWithMandatoryStepsConfigured();
    
    disruptorAvroFileWriterBuilderUnderTest.createNewWriter();
    
    verify(avroEventPublisherMock).registerConsumerExecutorForShutdown(isA(ExecutorService.class));
  }

  private void givenDestinationFileIsADirectory() {
    when(Files.exists(avroFileNameMock)).thenReturn(true);
    when(Files.isDirectory(avroFileNameMock)).thenReturn(true);
  }

  private void givenDestinationFileIsNotReadable() {
    when(Files.exists(avroFileNameMock)).thenReturn(true);
    when(Files.isReadable(avroFileNameMock)).thenReturn(false);
  }

  private void givenDestinationFileIsNotWritable() {
    when(Files.exists(avroFileNameMock)).thenReturn(true);
    when(Files.isWritable(avroFileNameMock)).thenReturn(false);
  }

  private void givenDestinationParentDirIsNotReadable() {
    when(Files.exists(avroFileNameMock)).thenReturn(false);
    when(Files.isReadable(parentDirMock)).thenReturn(false);
  }
  
  private void givenDestinationParentDirIsNotWritable() {
    when(Files.exists(avroFileNameMock)).thenReturn(false);
    when(Files.isWritable(parentDirMock)).thenReturn(false);
  }
  
  private void givenDestinationParentDirIsNotExecutable() {
    when(Files.exists(avroFileNameMock)).thenReturn(false);
    when(Files.isExecutable(parentDirMock)).thenReturn(false);
  }

  private OptionalSteps givenABuilderWithMandatoryStepsConfigured() throws IOException {
    givenDestinationFileIsValid();
    return disruptorAvroFileWriterBuilderUnderTest.thatWritesTo(avroFileNameMock).thatWritesRecordsOf(avroSchemaMock);
  }

  private void givenDestinationFileIsValid() throws IOException {
    when(Files.exists(avroFileNameMock)).thenReturn(true);
    when(Files.isDirectory(avroFileNameMock)).thenReturn(false);
  }
  
  @SuppressWarnings("unchecked")
  private void thenConfiguredDestinationFileIsUsedInTheFinalWriterObject() throws Exception {
    verifyNew(AvroEventConsumer.class).withArguments(eq(avroFileNameMock), any(Schema.class), any(RollingPolicy.class));
    verify(disruptorMock).handleEventsWith(avroEventConsumerMock);
    verify(avroEventPublisherMock).startPublisherUsing(disruptorMock);
  }
  
  @SuppressWarnings("unchecked")
  private void thenConfiguredSchemaIsUsedInTheFinalWriterObject() throws Exception {
    verifyNew(AvroEventConsumer.class).withArguments(any(Path.class), eq(avroSchemaMock), any(RollingPolicy.class));
    verify(disruptorMock).handleEventsWith(avroEventConsumerMock);
    verify(avroEventPublisherMock).startPublisherUsing(disruptorMock);
  }

  private void thenDefaultsAreUsedInTheFinalWriterObject() throws Exception {
    verifyRollingPolicyDefaults();
    verifyDisruptorDefaults();
    verify(avroEventPublisherMock).startPublisherUsing(disruptorMock);
  }
  
  @SuppressWarnings("unchecked")
  private void verifyRollingPolicyDefaults() throws Exception {
    verifyNew(TimeAndSizeBasedRollingPolicy.class).withArguments(50, avroFileNameMock);
    verifyNew(AvroEventConsumer.class).withArguments(any(Path.class), any(Schema.class), eq(timeAndSizeBasedRollingPolicyMock));
    verify(disruptorMock).handleEventsWith(avroEventConsumerMock);
  }
  
  private void verifyDisruptorDefaults() throws Exception {
    verifyNew(Disruptor.class).withArguments(any(AvroEventFactory.class), eq(1024), any(ExecutorService.class),
                                             eq(ProducerType.MULTI), isA(SleepingWaitStrategy.class));
  }
  
  private void thenConfiguredBufferSizeIsUsedInTheFinalWriterObject() throws Exception {
    verifyNew(Disruptor.class).withArguments(any(AvroEventFactory.class), eq(Integer.MAX_VALUE), any(ExecutorService.class),
                                             any(ProducerType.class), any(WaitStrategy.class));
    verify(avroEventPublisherMock).startPublisherUsing(disruptorMock);
  }
  
  private void thenConfiguredProducerTypeIsUsedInTheFinalWriterObject() throws Exception {
    verifyNew(Disruptor.class).withArguments(any(AvroEventFactory.class), anyInt(), any(ExecutorService.class),
                                             eq(producerTypeMock), any(WaitStrategy.class));
    verify(avroEventPublisherMock).startPublisherUsing(disruptorMock);
  }
  
  private void thenConfiguredWriteStrategyIsUsedInTheFinalWriterObject() throws Exception {
    verifyNew(Disruptor.class).withArguments(any(AvroEventFactory.class), anyInt(),
                                             any(ExecutorService.class), any(ProducerType.class), eq(waitStrategyMock));
    verify(avroEventPublisherMock).startPublisherUsing(disruptorMock);
  }

  @SuppressWarnings("unchecked")
  private void thenConfiguredRollingPolicyIsUsedInTheFinalWriterObject() throws Exception {
    verifyNew(AvroEventConsumer.class).withArguments(any(Path.class), any(Schema.class), eq(rollingPolicyMock));
    verify(disruptorMock).handleEventsWith(avroEventConsumerMock);
    verify(avroEventPublisherMock).startPublisherUsing(disruptorMock);
  }
}
