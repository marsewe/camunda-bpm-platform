/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.bpm.engine.rest.impl;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.ws.rs.container.AsyncResponse;

import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.IdentityService;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.externaltask.ExternalTaskQueryTopicBuilder;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.camunda.bpm.engine.rest.dto.externaltask.FetchExternalTasksExtendedDto;
import org.camunda.bpm.engine.rest.helper.MockProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ConcurrentFetchAndLockTest {

  @Mock
  public ProcessEngine processEngine;
  
  @Mock
  public IdentityService identityService;
  
  @Mock
  public ExternalTaskService externalTaskService;
  
  @Mock
  public ExternalTaskQueryTopicBuilder fetchTopicBuilder;
  
  protected ControllableFetchAndLockHandler handler;

  private LockedExternalTask lockedExternalTaskMock;

  protected static final Date START_DATE = new Date(1457326800000L);

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
    
    when(processEngine.getIdentityService()).thenReturn(identityService);
    when(processEngine.getExternalTaskService()).thenReturn(externalTaskService);

    when(externalTaskService.fetchAndLock(anyInt(), any(String.class), any(Boolean.class)))
      .thenReturn(fetchTopicBuilder);
    when(fetchTopicBuilder.topic(any(String.class), anyLong()))
      .thenReturn(fetchTopicBuilder);
    when(fetchTopicBuilder.variables(anyListOf(String.class)))
      .thenReturn(fetchTopicBuilder);
    when(fetchTopicBuilder.enableCustomObjectDeserialization())
      .thenReturn(fetchTopicBuilder);

    handler = new ControllableFetchAndLockHandler();
    lockedExternalTaskMock = MockProvider.createMockLockedExternalTask();
  }

  @After
  public void tearDown() {
    handler.shutdown();
  }
  
  @Test
  public void shouldFoo() throws InterruptedException {
    handler.start();
    handler.getAcquisitionThreadControl().waitForSync();

    final CountDownLatch latch1 = new CountDownLatch(1);
    
    when(fetchTopicBuilder.execute())
      .thenReturn(Collections.<LockedExternalTask>emptyList());
  
    AsyncResponse asyncResponse1 = mock(AsyncResponse.class);
    when(asyncResponse1.resume(any())).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws InterruptedException {
        latch1.countDown();
        return true;
      }
    });

    handler.addPendingRequest(createDto(5000L), asyncResponse1, processEngine);

    handler.getAcquisitionThreadControl().makeContinue();

    latch1.await();

    
  }
  
//  @Test//(timeout = 30000)
//  public void shouldResume() throws InterruptedException {
//    final CountDownLatch latch1 = new CountDownLatch(1);
//    
//    when(fetchTopicBuilder.execute())
//      .thenReturn(Collections.<LockedExternalTask>emptyList());
//
//    AsyncResponse asyncResponse1 = mock(AsyncResponse.class);
//    when(asyncResponse1.resume(any())).thenAnswer(new Answer<Boolean>() {
//      @Override
//      public Boolean answer(InvocationOnMock invocation) throws InterruptedException {
//        latch1.countDown();
//        return true;
//      }
//    });
//
//    final CountDownLatch latch2 = new CountDownLatch(1);
//    
//    AsyncResponse asyncResponse2 = mock(AsyncResponse.class);
//    when(asyncResponse2.resume(any())).thenAnswer(new Answer<Boolean>() {
//      @Override
//      public Boolean answer(InvocationOnMock invocation) throws InterruptedException {
//        latch2.countDown();
//        return true;
//      }
//    });
//    
//    
//    handler.start();
//
//    handler.addPendingRequest(createDto(5000L), asyncResponse1, processEngine);
//    latch1.await();
//    handler.addPendingRequest(createDto(5000L), asyncResponse2, processEngine);
//    latch2.await();
//
//    assertThat(handler.getPendingRequests().size(), is(0));
//
//    ArgumentCaptor<List> argumentCaptor = ArgumentCaptor.forClass(List.class);
//    verify(asyncResponse1, times(1)).resume(argumentCaptor.capture());
//    verify(asyncResponse2, times(1)).resume(argumentCaptor.capture());
//
//    for (List lockedExternalTasks : argumentCaptor.getAllValues()) {
//      assertThat(lockedExternalTasks.isEmpty(), is(true));
//    }
//  }

  protected FetchExternalTasksExtendedDto createDto(Long responseTimeout) {
    FetchExternalTasksExtendedDto externalTask = new FetchExternalTasksExtendedDto();

    FetchExternalTasksExtendedDto.FetchExternalTaskTopicDto topic = new FetchExternalTasksExtendedDto.FetchExternalTaskTopicDto();
    topic.setTopicName("aTopicName");
    topic.setLockDuration(12354L);

    externalTask.setMaxTasks(5);
    externalTask.setWorkerId("aWorkerId");
    externalTask.setTopics(Collections.singletonList(topic));

    if (responseTimeout != null) {
      externalTask.setAsyncResponseTimeout(responseTimeout);
    }

    return externalTask;
  }

  public static class ThreadControl {

    protected volatile boolean syncAvailable = false;

    protected Thread executingThread;

    protected volatile boolean reportFailure;
    protected volatile Exception exception;

    protected boolean ignoreSync = false;

    public ThreadControl() {
    }

    public ThreadControl(Thread executingThread) {
      this.executingThread = executingThread;
    }

    public void waitForSync() {
      waitForSync(Long.MAX_VALUE);
    }

    public void waitForSync(long timeout) {
      synchronized (this) {
        if(exception != null) {
          if (reportFailure) {
            return;
          }
          else {
            Assert.fail();
          }
        }
        try {
          if(!syncAvailable) {
            try {
              wait(timeout);
            } catch (InterruptedException e) {
              if (!reportFailure || exception == null) {
                Assert.fail("unexpected interruption");
              }
            }
          }
        } finally {
          syncAvailable = false;
        }
      }
    }

    public void waitUntilDone() {
      waitUntilDone(false);
    }

    public void waitUntilDone(boolean ignoreUpcomingSyncs) {
      ignoreSync = ignoreUpcomingSyncs;
      makeContinue();
      join();
    }

    public void join() {
      try {
        executingThread.join();
      } catch (InterruptedException e) {
        if (!reportFailure || exception == null) {
          Assert.fail("Unexpected interruption");
        }
      }
    }

    public void sync() {
      synchronized (this) {
        if (ignoreSync) {
          return;
        }

        syncAvailable = true;
        try {
          notifyAll();
          wait();
        } catch (InterruptedException e) {
          if (!reportFailure || exception == null) {
            Assert.fail("Unexpected interruption");
          }
        }
      }
    }

    public void makeContinue() {
      synchronized (this) {
        if(exception != null) {
          Assert.fail();
        }
        notifyAll();
      }
    }

    public void makeContinueAndWaitForSync() {
      makeContinue();
      waitForSync();
    }

    public void reportInterrupts() {
      this.reportFailure = true;
    }

    public void ignoreFutureSyncs() {
      this.ignoreSync = true;
    }

    public synchronized void setException(Exception e) {
      this.exception = e;
    }

    public Throwable getException() {
      return exception;
    }
  }
  
}
