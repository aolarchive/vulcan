package com.aol.advertising.vulcan.api;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.aol.advertising.vulcan.ConfiguredUnitTest;
import com.aol.advertising.vulcan.api.AvroWriterBuilder;
import com.aol.advertising.vulcan.api.AvroWriterFactory;
import com.aol.advertising.vulcan.api.rolling.RollingPolicy;
import com.aol.advertising.vulcan.rolling.TimeAndSizeBasedRollingPolicyConfig;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ProducerType.class, AvroWriterBuilder.class, AvroWriterFactory.class})
public class DisruptorAvroFileWriterFactoryTest extends ConfiguredUnitTest {

  private static final String AVRO_FILE_NAME = "Eufrasio";
  private static final int BUFFER_SIZE = 123456;
  private static final TimeAndSizeBasedRollingPolicyConfig ROLLING_POLICY_CONFIGURATION =
      new TimeAndSizeBasedRollingPolicyConfig().withFileRollingSizeOf(345);

  @Mock
  private AvroWriterBuilder disruptorAvroFileWriterBuilderMock;
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
    mockStatic(AvroWriterBuilder.class);
    when(AvroWriterBuilder.startCreatingANewWriter()).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.thatWritesTo(AVRO_FILE_NAME)).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.thatWritesRecordsOf(avroSchemaMock)).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.withRingBufferSize(BUFFER_SIZE)).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.withProducerType(producerTypeMock)).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.withWaitStrategy(waitStrategyMock)).thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.withDefaultRollingPolicyConfiguration(ROLLING_POLICY_CONFIGURATION))
      .thenReturn(disruptorAvroFileWriterBuilderMock);
    when(disruptorAvroFileWriterBuilderMock.withRollingPolicy(rollingPolicyMock)).thenReturn(disruptorAvroFileWriterBuilderMock);
  }

  @Test
  public void whenANewWriterIsCreated_thenConstructionIsDelegatedToTheBuilder() {
    final AvroWriterFactory disruptorAvroFileWriterFactoryUnderTest = new AvroWriterFactory();
    populateFactoryFields(disruptorAvroFileWriterFactoryUnderTest);
    
    disruptorAvroFileWriterFactoryUnderTest.createNewWriter();
    
    verifyDelegationUsedAllOfTheFactoryFields();
  }

  private void populateFactoryFields(AvroWriterFactory disruptorAvroFileWriterFactoryUnderTest) {
    disruptorAvroFileWriterFactoryUnderTest.setAvroFilename(AVRO_FILE_NAME);
    disruptorAvroFileWriterFactoryUnderTest.setAvroSchema(avroSchemaMock);
    disruptorAvroFileWriterFactoryUnderTest.setRingBufferSize(BUFFER_SIZE);
    disruptorAvroFileWriterFactoryUnderTest.setProducerType(producerTypeMock);
    disruptorAvroFileWriterFactoryUnderTest.setWaitStrategy(waitStrategyMock);
    disruptorAvroFileWriterFactoryUnderTest.setDefaultRollingPolicyConfiguration(ROLLING_POLICY_CONFIGURATION);
    disruptorAvroFileWriterFactoryUnderTest.setRollingPolicy(rollingPolicyMock);
  }
  
  private void verifyDelegationUsedAllOfTheFactoryFields() {
    verify(disruptorAvroFileWriterBuilderMock).thatWritesTo(AVRO_FILE_NAME);
    verify(disruptorAvroFileWriterBuilderMock).thatWritesRecordsOf(avroSchemaMock);
    verify(disruptorAvroFileWriterBuilderMock).withRingBufferSize(BUFFER_SIZE);
    verify(disruptorAvroFileWriterBuilderMock).withProducerType(producerTypeMock);
    verify(disruptorAvroFileWriterBuilderMock).withWaitStrategy(waitStrategyMock);
    verify(disruptorAvroFileWriterBuilderMock).withDefaultRollingPolicyConfiguration(ROLLING_POLICY_CONFIGURATION);
    verify(disruptorAvroFileWriterBuilderMock).withRollingPolicy(rollingPolicyMock);
    verify(disruptorAvroFileWriterBuilderMock).createNewWriter();
  }

}
