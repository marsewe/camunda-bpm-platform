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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response.Status;

import org.camunda.bpm.engine.IdentityService;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.externaltask.ExternalTaskQueryBuilder;
import org.camunda.bpm.engine.externaltask.ExternalTaskQueryTopicBuilder;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.camunda.bpm.engine.impl.identity.Authentication;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.camunda.bpm.engine.rest.dto.externaltask.FetchExternalTasksExtendedDto;
import org.camunda.bpm.engine.rest.dto.externaltask.LockedExternalTaskDto;
import org.camunda.bpm.engine.rest.exception.InvalidRequestException;
import org.camunda.bpm.engine.rest.spi.FetchAndLockHandler;

/**
 * @author Tassilo Weidner
 */
public class FetchAndLockHandlerImpl implements Runnable, FetchAndLockHandler {
  
  protected static final long MAX_BACK_OFF_TIME = Long.MAX_VALUE;
  protected static final long MAX_TIMEOUT = 1800000; // 30 minutes

  // TODO: Is it somehow possible to make the queue capacity configurable? 
  protected BlockingQueue<FetchAndLockRequest> queue = new ArrayBlockingQueue<FetchAndLockRequest>(100);
  protected List<FetchAndLockRequest> pendingRequests = new ArrayList<FetchAndLockRequest>();

  protected final Object MONITOR = new Object();
  protected Thread handlerThread = new Thread(this, this.getClass().getSimpleName());

  protected boolean isWaiting = false;
  protected boolean isRunning = false;

  @Override
  public void run() {
    while (isRunning) {
      acquire();
    }

    System.out.println("shutdown");
    for (FetchAndLockRequest pendingRequest : pendingRequests) {
      invalidRequest(pendingRequest.getAsyncResponse(), "Request rejected due to shutdown of application server.");
    }
  }

  protected void acquire() {

    System.out.println("acquire queue " + queue.size());
    queue.drainTo(pendingRequests);
    System.out.println("acquire pending " + pendingRequests.size());

    long backoffTime = MAX_BACK_OFF_TIME;
    long start = ClockUtil.getCurrentTime().getTime();
    
    Iterator<FetchAndLockRequest> iterator = pendingRequests.iterator();
    while(iterator.hasNext()) {

      FetchAndLockRequest pendingRequest = iterator.next();
      FetchAndLockResult result = tryFetchAndLock(pendingRequest);

      if (result.wasSuccessful()) {

        List<LockedExternalTaskDto> lockedTasks = result.tasks;

        FetchExternalTasksExtendedDto dto = pendingRequest.getDto();
        long requestTime = pendingRequest.getRequestTime().getTime();
        long asyncResponseTimeout = dto.getAsyncResponseTimeout();
        long currentTime = ClockUtil.getCurrentTime().getTime();

        long timeout = requestTime + asyncResponseTimeout;

        if (!lockedTasks.isEmpty() || timeout <= currentTime) {
          AsyncResponse asyncResponse = pendingRequest.getAsyncResponse();
          iterator.remove();
          asyncResponse.resume(lockedTasks);
        } else {
          long slackTime = timeout - currentTime;
          if (slackTime < backoffTime) {
            backoffTime = slackTime;
          }
        }
      } else {
        handleProcessEngineException(pendingRequest, result.processEngineException);
      }
    }

    backoffTime = Math.max(0, (start + backoffTime) - ClockUtil.getCurrentTime().getTime());
    suspend(backoffTime);
  }

  @Override
  public void start() {
    if (isRunning) {
      return;
    }

    isRunning = true;
    handlerThread.start();
  }

  @Override
  public void shutdown() {
    synchronized (MONITOR) {
      isRunning = false;
      if(isWaiting) {
        MONITOR.notifyAll();
      }
    }
  }

  protected void suspend(long millis) {
    System.out.println("suspend (0) " +  millis);
    if (millis <= 0) {
      return;
    }
    
    if (!queue.isEmpty()) {
      System.out.println("suspend queue (1)");
      return;
    }
    
    suspendAcquisition(millis);
  }
  
  protected void suspendAcquisition(long millis) {
    try {
      synchronized (MONITOR) {
        if (queue.isEmpty()) {
          System.out.println("pendingRequests " + pendingRequests.size());
          System.out.println("wait " + millis);
          isWaiting = true;
          MONITOR.wait(millis);
          System.out.println("leave wait");
        }
        else {
          System.out.println("suspend queue (2)");
        }
      }

    }
    catch (InterruptedException ignore) {}
    finally {
      isWaiting = false;
    }
  }

  protected void addRequest(FetchAndLockRequest request) {
    // TODO: what happens when the queue is full?
    while (!queue.offer(request)) {
      System.out.println("offer failed");
      // noop;
    }
    
    notifyAcquisition();
  }
  
  protected void notifyAcquisition() {
    synchronized (MONITOR) {
      if (isWaiting) {
        MONITOR.notifyAll();
        System.out.println("notify all");
      }
    }    
  }

  protected FetchAndLockResult tryFetchAndLock(FetchAndLockRequest request) {
    ProcessEngine processEngine = request.getProcessEngine();
    IdentityService identityService = processEngine.getIdentityService();

    FetchAndLockResult result;

    try {
      identityService.setAuthentication(request.getAuthentication());
      FetchExternalTasksExtendedDto fetchingDto = request.getDto();
      List<LockedExternalTaskDto> lockedTasks = executeFetchAndLock(fetchingDto, processEngine);
      result = FetchAndLockResult.successful(lockedTasks);
    }
    catch (ProcessEngineException e) {
      result = FetchAndLockResult.failed(e);
    }
    finally {
      identityService.clearAuthentication();
    }

    return result;
  }

  protected List<LockedExternalTaskDto> executeFetchAndLock(FetchExternalTasksExtendedDto fetchingDto, ProcessEngine processEngine) {
    ExternalTaskQueryBuilder fetchBuilder = processEngine
      .getExternalTaskService()
      .fetchAndLock(fetchingDto.getMaxTasks(), fetchingDto.getWorkerId(), fetchingDto.isUsePriority());

    if (fetchingDto.getTopics() != null) {
      for (FetchExternalTasksExtendedDto.FetchExternalTaskTopicDto topicDto : fetchingDto.getTopics()) {
        ExternalTaskQueryTopicBuilder topicFetchBuilder =
          fetchBuilder.topic(topicDto.getTopicName(), topicDto.getLockDuration());

        if (topicDto.getBusinessKey() != null) {
          topicFetchBuilder = topicFetchBuilder.businessKey(topicDto.getBusinessKey());
        }

        if (topicDto.getVariables() != null) {
          topicFetchBuilder = topicFetchBuilder.variables(topicDto.getVariables());
        }

        if (topicDto.getProcessVariables() != null) {
          topicFetchBuilder = topicFetchBuilder.processInstanceVariableEquals(topicDto.getProcessVariables());
        }

        if (topicDto.isDeserializeValues()) {
          topicFetchBuilder = topicFetchBuilder.enableCustomObjectDeserialization();
        }

        fetchBuilder = topicFetchBuilder;
      }
    }

    List<LockedExternalTask> externalTasks = fetchBuilder.execute();
    return LockedExternalTaskDto.fromLockedExternalTasks(externalTasks);
  }

  protected void invalidRequest(AsyncResponse asyncResponse, String message) {
    InvalidRequestException invalidRequestException = new InvalidRequestException(Status.BAD_REQUEST, message);
    asyncResponse.resume(invalidRequestException);
  }

  protected void handleProcessEngineException(FetchAndLockRequest request, ProcessEngineException exception) {
    AsyncResponse asyncResponse = request.getAsyncResponse();
    pendingRequests.remove(request);
    asyncResponse.resume(exception);
  }

  @Override
  public void addPendingRequest(FetchExternalTasksExtendedDto dto, AsyncResponse asyncResponse, ProcessEngine processEngine) {

    Long asyncResponseTimeout = dto.getAsyncResponseTimeout();
    if (asyncResponseTimeout != null && asyncResponseTimeout > MAX_TIMEOUT) {
      invalidRequest(asyncResponse, "The asynchronous response timeout cannot be set to a value greater than "
        + MAX_TIMEOUT + " milliseconds");
      return;
    }

    IdentityService identityService = processEngine.getIdentityService();
    Authentication authentication = identityService.getCurrentAuthentication();

    FetchAndLockRequest incomingRequest = new FetchAndLockRequest()
      .setProcessEngine(processEngine)
      .setAsyncResponse(asyncResponse)
      .setAuthentication(authentication)
      .setDto(dto);

    FetchAndLockResult result = tryFetchAndLock(incomingRequest);

    if (result.wasSuccessful()) {

      List<LockedExternalTaskDto> lockedTasks = result.tasks;
      if (!lockedTasks.isEmpty() || dto.getAsyncResponseTimeout() == null) { // response immediately if tasks available
        asyncResponse.resume(lockedTasks);
      } else {
        addRequest(incomingRequest);
      }
    }
    else {
      handleProcessEngineException(incomingRequest, result.processEngineException);
    }

  }

  public Thread getHandlerThread() {
    return handlerThread;
  }

  public List<FetchAndLockRequest> getPendingRequests() {
    return pendingRequests;
  }
  
  public boolean hasPendingRequests() {
    int size = 0;
    synchronized (pendingRequests) {
      size = pendingRequests.size();
    }
    
    return size > 0;
  }

}
