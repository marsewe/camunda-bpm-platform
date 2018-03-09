package org.camunda.bpm.engine.test.bpmn.event.signal;

import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.TaskService;
import org.camunda.bpm.engine.history.HistoricProcessInstance;
import org.camunda.bpm.engine.history.HistoricVariableInstance;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.test.Deployment;
import org.camunda.bpm.engine.test.util.ProcessEngineBootstrapRule;
import org.camunda.bpm.engine.test.util.ProcessEngineTestRule;
import org.camunda.bpm.engine.test.util.ProvidedProcessEngineRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author Nikola Koevski
 */
public class SignalEventPayloadTest {

  protected ProcessEngineBootstrapRule bootstrapRule = new ProcessEngineBootstrapRule() {
    public ProcessEngineConfiguration configureEngine(ProcessEngineConfigurationImpl configuration) {
      configuration.setJavaSerializationFormatEnabled(true);
      return configuration;
    }
  };

  protected ProvidedProcessEngineRule engineRule = new ProvidedProcessEngineRule(bootstrapRule);

  public ProcessEngineTestRule testRule = new ProcessEngineTestRule(engineRule);

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(bootstrapRule).around(engineRule).around(testRule);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private RuntimeService runtimeService;
  private TaskService taskService;
  private RepositoryService repositoryService;
  private ManagementService managementService;
  private ProcessEngineConfigurationImpl processEngineConfiguration;

  @Before
  public void init() {
    runtimeService = engineRule.getRuntimeService();
    taskService = engineRule.getTaskService();
    repositoryService = engineRule.getRepositoryService();
    managementService = engineRule.getManagementService();
    processEngineConfiguration = engineRule.getProcessEngineConfiguration();
  }


  /**
   * Test case for CAM-8820 with a catching Start Signal event.
   * Using Source and Target Variable name mapping attributes.
   */
  @Test
  @Deployment(resources = {
    "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.throwSignalWithPayload.bpmn20.xml",
    "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.catchSignalWithPayloadStart.bpmn20.xml" })
  public void testSignalPayloadStart() {
    // given
    Map<String, Object> variables = new HashMap<String, Object>();
    variables.put("payloadVar1", "payloadVal1");
    variables.put("payloadVar2", "payloadVal2");
    ProcessInstance catchingPI = runtimeService.startProcessInstanceByKey("catchStartPayloadSignal");

    // when
    ProcessInstance throwingPI = runtimeService.startProcessInstanceByKey("throwPayloadSignal", variables);

    // then
    assertEquals(0, runtimeService.createProcessInstanceQuery()
      .processInstanceIds(new HashSet<String>(Arrays.asList(throwingPI.getId(), catchingPI.getId())))
      .count());

    List<HistoricVariableInstance> catchingPiVariables = processEngineConfiguration.getHistoryService().createHistoricVariableInstanceQuery().processInstanceId(catchingPI.getId()).list();
    assertEquals(2, catchingPiVariables.size());

    for(HistoricVariableInstance variable : catchingPiVariables) {
      if(variable.getName().equals("payloadVar1"))
        assertEquals("payloadVal1", variable.getValue());
      else
        assertEquals("payloadVal2", variable.getValue());
    }
  }

  /**
   * Test case for CAM-8820 with a catching Intermediate Signal event.
   * Using Source and Target Variable name mapping attributes.
   */
  @Test
  @Deployment(resources = { "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.throwSignalWithPayload.bpmn20.xml",
    "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.catchSignalWithPayloadIntermediate.bpmn20.xml" })
  public void testSignalPayloadIntermediate() {
    // given
    Map<String, Object> variables = new HashMap<String, Object>();
    variables.put("payloadVar1", "payloadVal1");
    variables.put("payloadVar2", "payloadVal2");
    ProcessInstance catchingPI = runtimeService.startProcessInstanceByKey("catchIntermediatePayloadSignal");

    // when
    ProcessInstance throwingPI = runtimeService.startProcessInstanceByKey("throwPayloadSignal", variables);

    // then
    assertEquals(0, runtimeService.createProcessInstanceQuery()
      .processInstanceIds(new HashSet<String>(Arrays.asList(throwingPI.getId(), catchingPI.getId())))
      .count());

    List<HistoricVariableInstance> catchingPiVariables = processEngineConfiguration.getHistoryService().createHistoricVariableInstanceQuery().processInstanceId(catchingPI.getId()).list();
    assertEquals(2, catchingPiVariables.size());

    for(HistoricVariableInstance variable : catchingPiVariables) {
      if(variable.getName().equals("payloadVar1"))
        assertEquals("payloadVal1", variable.getValue());
      else
        assertEquals("payloadVal2", variable.getValue());
    }
  }

  /**
   * Test case for CAM-8820 with an expression as a source.
   */
  @Test
  @Deployment(resources = { "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.throwSignalWithExpressionPayload.bpmn20.xml",
    "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.catchSignalWithPayloadIntermediate.bpmn20.xml" })
  public void testSignalSourceExpressionPayload() {
    // given
    Map<String, Object> variables = new HashMap<String, Object>();
    variables.put("payloadVar", "Val");
    ProcessInstance catchingPI = runtimeService.startProcessInstanceByKey("catchIntermediatePayloadSignal");

    // when
    ProcessInstance throwingPI = runtimeService.startProcessInstanceByKey("throwExpressionPayloadSignal", variables);

    // then
    assertEquals(0, runtimeService.createProcessInstanceQuery()
      .processInstanceIds(new HashSet<String>(Arrays.asList(throwingPI.getId(), catchingPI.getId())))
      .count());

    List<HistoricVariableInstance> catchingPiVariables = processEngineConfiguration.getHistoryService().createHistoricVariableInstanceQuery().processInstanceId(catchingPI.getId()).list();
    assertEquals(1, catchingPiVariables.size());

    assertEquals("srcExpressionResVal", catchingPiVariables.get(0).getName());
    assertEquals("sourceVal", catchingPiVariables.get(0).getValue());
  }

  /**
   * Test case for CAM-8820 with all the (global) source variables
   * as the signal payload.
   */
  @Test
  @Deployment(resources = { "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.throwSignalWithAllVariablesPayload.bpmn20.xml",
    "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.catchSignalWithPayloadIntermediate.bpmn20.xml" })
  public void testSignalAllSourceVariablesPayload() {
    // given
    Map<String, Object> variables = new HashMap<String, Object>();
    variables.put("payloadVar1", "payloadVal1");
    variables.put("payloadVar2", "payloadVal2");
    ProcessInstance catchingPI = runtimeService.startProcessInstanceByKey("catchIntermediatePayloadSignal");

    // when
    ProcessInstance throwingPI = runtimeService.startProcessInstanceByKey("throwPayloadSignal", variables);

    // then
    assertEquals(0, runtimeService.createProcessInstanceQuery()
      .processInstanceIds(new HashSet<String>(Arrays.asList(throwingPI.getId(), catchingPI.getId())))
      .count());

    List<HistoricVariableInstance> catchingPiVariables = processEngineConfiguration.getHistoryService().createHistoricVariableInstanceQuery().processInstanceId(catchingPI.getId()).list();
    assertEquals(2, catchingPiVariables.size());

    for(HistoricVariableInstance variable : catchingPiVariables) {
      if(variable.getName().equals("payloadVar1"))
        assertEquals("payloadVal1", variable.getValue());
      else
        assertEquals("payloadVal2", variable.getValue());
    }
  }

  /**
   * Test case for CAM-8820 with all the (local) source variables
   * as the signal payload.
   */
  @Test
  @Deployment(resources = { "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.throwSignalWithAllLocalVariablesPayload.bpmn20.xml",
    "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.catchSignalWithPayloadIntermediate.bpmn20.xml" })
  public void testSignalAllLocalSourceVariablesPayload() {
    // given
    String localVar1 = "localVar1";
    String localVal1 = "localVal1";;
    String localVar2 = "localVar2";
    String localVal2 = "localVal2";
    ProcessInstance catchingPI = runtimeService.startProcessInstanceByKey("catchIntermediatePayloadSignal");

    // when
    // TODO Put Local vars
    ProcessInstance throwingPI = runtimeService.startProcessInstanceByKey("throwPayloadSignal");

    // then
    assertEquals(0, runtimeService.createProcessInstanceQuery()
      .processInstanceIds(new HashSet<String>(Arrays.asList(throwingPI.getId(), catchingPI.getId())))
      .count());

    List<HistoricVariableInstance> catchingPiVariables = processEngineConfiguration.getHistoryService().createHistoricVariableInstanceQuery().processInstanceId(catchingPI.getId()).list();
    assertEquals(2, catchingPiVariables.size());

    for(HistoricVariableInstance variable : catchingPiVariables) {
      if(variable.getName().equals(localVar1))
        assertEquals(localVal1, variable.getValue());
      else
        assertEquals(localVal2, variable.getValue());
    }
  }

  /**
   * Test case for CAM-8820 with a Business Key
   * as signal payload.
   */
  @Test
  @Deployment(resources = { "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.throwSignalWithBusinessKeyPayload.bpmn20.xml",
    "org/camunda/bpm/engine/test/bpmn/event/signal/SignalEventPayloadTests.catchSignalWithBusinessKeyPayload.bpmn20.xml" })
  public void testSignalBusinessKeyPayload() {
    // given
    String businessKey = "aBusinessKey";
    ProcessInstance catchingPI = runtimeService.startProcessInstanceByKey("catchBusinessKeyPayloadSignal");

    // when
    ProcessInstance throwingPI = runtimeService.startProcessInstanceByKey("throwBusinessKeyPayloadSignal", businessKey);

    // then
    assertEquals(0, runtimeService.createProcessInstanceQuery()
      .processInstanceIds(new HashSet<String>(Arrays.asList(throwingPI.getId(), catchingPI.getId())))
      .count());

    HistoricProcessInstance cPi = processEngineConfiguration.getHistoryService().createHistoricProcessInstanceQuery().processInstanceId(catchingPI.getId()).singleResult();
    assertEquals(businessKey, cPi.getBusinessKey());
  }
}
