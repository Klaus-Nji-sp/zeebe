/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.message;

import io.zeebe.engine.processor.workflow.BpmnStepContext;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableFlowElementContainer;
import io.zeebe.engine.state.message.MessageState;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.value.BpmnElementType;
import java.util.function.Consumer;

public class MessageStartWorkflowInstancePoller
    implements Consumer<BpmnStepContext<ExecutableFlowElementContainer>> {

  private final MessageState messageState;

  private final WorkflowInstanceRecord startEventRecord =
      new WorkflowInstanceRecord().setBpmnElementType(BpmnElementType.START_EVENT);

  private boolean messageCorrelated;

  public MessageStartWorkflowInstancePoller(final MessageState messageState) {
    this.messageState = messageState;
  }

  @Override
  public void accept(final BpmnStepContext<ExecutableFlowElementContainer> context) {

    messageCorrelated = false;

    final var bpmnProcessId = context.getValue().getBpmnProcessIdBuffer();
    final var workflowInstanceKey = context.getValue().getWorkflowInstanceKey();
    final var correlationKey = messageState.getWorkflowInstanceCorrelationKey(workflowInstanceKey);

    if (correlationKey != null) {

      /*
      final var deployedWorkflow =
          context.getStateDb().getLatestWorkflowVersionByProcessId(bpmnProcessId);
      final var workflow = deployedWorkflow.getWorkflow();

      for (final ExecutableStartEvent startEvent : workflow.getStartEvents()) {

        if (startEvent.isMessage() && !messageCorrelated) {

          messageState.visitMessages(
              startEvent.getMessage().getMessageName(),
              correlationKey,
              message -> {
                // correlate first message with same correlation key that was not correlated yet
                if (!messageState.existMessageCorrelation(message.getKey(), bpmnProcessId)) {

                  messageState.putMessageCorrelation(message.getKey(), bpmnProcessId);

                  // TODO (saig0): extract correlation logic from PublishMessageProcessor
                  context
                      .getOutput()
                      .appendFollowUpEvent(
                          message.getKey(),
                          WorkflowInstanceIntent.EVENT_OCCURRED,
                          startEventRecord
                              .setWorkflowKey(deployedWorkflow.getKey())
                              .setElementId(startEvent.getId()));

                  messageCorrelated = true;
                  return false;
                }

                return true;
              });
        }
      }
      */

      messageState.removeWorkflowInstanceCorrelationKey(workflowInstanceKey);

      if (!messageCorrelated) {
        messageState.removeActiveWorkflowInstance(bpmnProcessId, correlationKey);
      }
    }
  }
}
