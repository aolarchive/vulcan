package com.aol.advertising.dmp.disruptor.exception;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LoggerFactory.class, DisruptorExceptionHandler.class})
public class DisruptorExceptionHandlerTest {

  @Mock
  private Logger logMock;
  @Mock
  private Throwable exceptionMock;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    mockStatic(LoggerFactory.class);
  }

  @Test
  public void whenAnExceptionOccursDuringStartup_thenAnErrorWithTheExceptionIsLogged() {
    when(LoggerFactory.getLogger(DisruptorExceptionHandler.class)).thenReturn(logMock);

    new DisruptorExceptionHandler().handleOnStartException(exceptionMock);
    
    verify(logMock).error(anyString(), same(exceptionMock));
  }

}
