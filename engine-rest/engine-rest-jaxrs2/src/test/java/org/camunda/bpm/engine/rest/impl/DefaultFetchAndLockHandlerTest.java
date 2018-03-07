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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.ws.rs.container.AsyncResponse;

import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.IdentityService;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.externaltask.ExternalTaskQueryTopicBuilder;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.camunda.bpm.engine.rest.dto.externaltask.FetchExternalTasksExtendedDto;
import org.camunda.bpm.engine.rest.helper.MockProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

public class DefaultFetchAndLockHandlerTest {
  
  @Mock
  public ProcessEngine processEngine;
  
  @Mock
  public IdentityService identityService;
  
  @Mock
  public ExternalTaskService externalTaskService;
  
  @Mock
  public ExternalTaskQueryTopicBuilder fetchTopicBuilder;
  
  @Spy
  protected FetchAndLockHandlerImpl handler;

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

    doNothing().when(handler).suspend(anyLong());

    lockedExternalTaskMock = MockProvider.createMockLockedExternalTask();
  }

  @Before
  public void setClock() {
    ClockUtil.setCurrentTime(START_DATE);
  }

  @Test
  public void shouldResumeAsyncResponse() {
    // given
    List<LockedExternalTask> tasks = new ArrayList<LockedExternalTask>();
    tasks.add(lockedExternalTaskMock);
    doReturn(tasks).when(fetchTopicBuilder).execute();

    AsyncResponse asyncResponse = mock(AsyncResponse.class);
    handler.addPendingRequest(createDto(5000L), asyncResponse, processEngine);

    // when
    handler.acquire();

    // then
    verify(asyncResponse).resume(any());
    assertThat(handler.getPendingRequests().size(), is(0));
    verify(handler).suspend(Long.MAX_VALUE);
  }

  @Test
  public void shouldNotResumeAsyncResponse() {
    // given
    doReturn(Collections.emptyList()).when(fetchTopicBuilder).execute();

    AsyncResponse asyncResponse = mock(AsyncResponse.class);
    handler.addPendingRequest(createDto(5000L), asyncResponse, processEngine);

    // when
    handler.acquire();

    // then
    verify(asyncResponse, never()).resume(any());
    assertThat(handler.getPendingRequests().size(), is(1));
    verify(handler).suspend(5000L);
  }

  @Test
  public void shouldResumeAsyncResponseWhenTimeoutIsExpired_1() {
    // given
    doReturn(Collections.emptyList()).when(fetchTopicBuilder).execute();

    AsyncResponse asyncResponse = mock(AsyncResponse.class);
    handler.addPendingRequest(createDto(5000L), asyncResponse, processEngine);
    handler.acquire();

    // assume
    assertThat(handler.getPendingRequests().size(), is(1));
    verify(handler).suspend(5000L);

    List<LockedExternalTask> tasks = new ArrayList<LockedExternalTask>();
    tasks.add(lockedExternalTaskMock);
    doReturn(tasks).when(fetchTopicBuilder).execute();

    addSecondsToClock(5);

    // when
    handler.acquire();

    // then
    verify(asyncResponse).resume(any());
    assertThat(handler.getPendingRequests().size(), is(0));
    verify(handler).suspend(Long.MAX_VALUE);
  }

  @Test
  public void shouldResumeAsyncResponseWhenTimeoutIsExpired_2() {
    // given
    doReturn(Collections.emptyList()).when(fetchTopicBuilder).execute();

    AsyncResponse asyncResponse = mock(AsyncResponse.class);
    handler.addPendingRequest(createDto(5000L), asyncResponse, processEngine);

    addSecondsToClock(1);
    handler.acquire();

    // assume
    assertThat(handler.getPendingRequests().size(), is(1));
    verify(handler).suspend(4000L);

    addSecondsToClock(4);

    // when
    handler.acquire();

    // then
    verify(asyncResponse).resume(any());
    assertThat(handler.getPendingRequests().size(), is(0));
    verify(handler).suspend(Long.MAX_VALUE);
  }


  
  
//  @Test
//  public void shouldSetCorrectSlacktime() {
//    // given
//    List<LockedExternalTask> tasks = new ArrayList<LockedExternalTask>();
//    tasks.add(lockedExternalTaskMock);
//    doReturn(tasks).when(fetchTopicBuilder).execute();
//
//    ClockUtil.setCurrentTime(new Date(ClockUtil.getCurrentTime().getTime()));
//
//    AsyncResponse asyncResponse = mock(AsyncResponse.class);
//    handler.addPendingRequest(createDto(5000L), asyncResponse, processEngine);
//
//    // when
//    ClockUtil.setCurrentTime(new Date(ClockUtil.getCurrentTime().getTime() + 1000L));
//    handler.acquire();
//
//    // then
//    assertThat(handler.getPendingRequests().size(), is(0));
//    verify(asyncResponse).resume(any());
//    verify(handler).suspend(Long.MAX_VALUE);
//    
//  }

//  @Test
//  public void shouldResumeAsyncResponse() {
//    
//    // given
//    
//    List mockedList = mock(List.class);
//    Iterator iterator = mock(Iterator.class);
//    FetchAndLockRequest request = mock(FetchAndLockRequest.class);
//    AsyncResponse response = mock(AsyncResponse.class);
//    
//    when(request.getAsyncResponse()).thenReturn(response);
//    when(mockedList.iterator()).thenReturn(iterator);
//    when(iterator.hasNext()).thenReturn(true, false);
//    when(iterator.next()).thenReturn(request);
//    
//
//    doReturn(mockedList).when(handler).getPendingRequests();
//
//    
//    handler.acquire2();
//    
//    
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

  protected Date addSeconds(Date date, int seconds) {
    return new Date(date.getTime() + seconds * 1000);
  }

  protected Date addSecondsToClock(int seconds) {
    Date newDate = addSeconds(ClockUtil.getCurrentTime(), seconds);
    ClockUtil.setCurrentTime(newDate);
    return newDate;
  }

}
