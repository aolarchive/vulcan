package com.aol.advertising.dmp.disruptor.api;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.nio.file.Path;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ProducerType.class, DisruptorAvroFileWriterBuilder.class, DisruptorAvroFileWriterFactory.class})
public class DisruptorAvroFileWriterFactoryTest {

  private static final int BUFFER_SIZE = 123456;

  @Mock
  private DisruptorAvroFileWriterBuilder disruptorAvroFileWriterBuilderMock;
  @Mock
  private Path avroFileNameMock;
  @Mock
  private Schema avroSchemaMock;
  @Mock
  private ProducerType producerTypeMock;
  @Mock
  private WaitStrategy waitStrategyMock;
  @Mock
  private RollingPolicy rollingPolicyMock;

  @Before
  public void setUp() {
    mockStatic(DisruptorAvroFileWriterBuilder.class);
    when(DisruptorAvroFileWriterBuilder.startCreatingANewWriter()).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.thatWritesTo(avroFileNameMock)).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.thatWritesRecordsOf(avroSchemaMock)).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.withRingBufferSize(BUFFER_SIZE)).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.withProducerType(producerTypeMock)).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.withWaitStrategy(waitStrategyMock)).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.withRollingPolicy(rollingPolicyMock)).thenReturn(disruptorAvroFileWriterBuilderMock);
  }

  @Test
  public void whenANewWriterIsCreated_thenConstructionIsDelegatedToTheBuilder() {
    final DisruptorAvroFileWriterFactory disruptorAvroFileWriterFactoryUnderTest = new DisruptorAvroFileWriterFactory();
    populateFactoryFields(disruptorAvroFileWriterFactoryUnderTest);
    
    disruptorAvroFileWriterFactoryUnderTest.createNewWriter();
    
    verifyDelegationUsedAllOfTheFactoryFields();
  }

  private void populateFactoryFields(DisruptorAvroFileWriterFactory disruptorAvroFileWriterFactoryUnderTest) {
    disruptorAvroFileWriterFactoryUnderTest.setAvroFileName(avroFileNameMock);
    disruptorAvroFileWriterFactoryUnderTest.setAvroSchema(avroSchemaMock);
    disruptorAvroFileWriterFactoryUnderTest.setRingBufferSize(BUFFER_SIZE);
    disruptorAvroFileWriterFactoryUnderTest.setProducerType(producerTypeMock);
    disruptorAvroFileWriterFactoryUnderTest.setWaitStrategy(waitStrategyMock);
    disruptorAvroFileWriterFactoryUnderTest.setRollingPolicy(rollingPolicyMock);
  }
  
  private void verifyDelegationUsedAllOfTheFactoryFields() {
    verify(disruptorAvroFileWriterBuilderMock).thatWritesTo(avroFileNameMock);
    verify(disruptorAvroFileWriterBuilderMock).thatWritesRecordsOf(avroSchemaMock);
    verify(disruptorAvroFileWriterBuilderMock).withRingBufferSize(BUFFER_SIZE);
    verify(disruptorAvroFileWriterBuilderMock).withProducerType(producerTypeMock);
    verify(disruptorAvroFileWriterBuilderMock).withWaitStrategy(waitStrategyMock);
    verify(disruptorAvroFileWriterBuilderMock).withRollingPolicy(rollingPolicyMock);
    verify(disruptorAvroFileWriterBuilderMock).createNewWriter();
  }

}
