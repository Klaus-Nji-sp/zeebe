/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.message;

import io.zeebe.engine.processor.KeyGenerator;
import io.zeebe.engine.processor.workflow.BpmnStepContext;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableFlowElementContainer;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableStartEvent;
import io.zeebe.engine.state.message.MessageState;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import java.util.function.Consumer;

public class MessageStartWorkflowInstancePoller
    implements Consumer<BpmnStepContext<ExecutableFlowElementContainer>> {

  private final MessageState messageState;
  private final KeyGenerator keyGenerator;

  // private final MessageStartEventSubscriptionState subscriptionState;

  private final WorkflowInstanceRecord startEventRecord =
      new WorkflowInstanceRecord().setBpmnElementType(BpmnElementType.START_EVENT);

  private boolean messageCorrelated;

  public MessageStartWorkflowInstancePoller(
      final KeyGenerator keyGenerator, final MessageState messageState) {
    this.keyGenerator = keyGenerator;
    this.messageState = messageState;
  }

  @Override
  public void accept(final BpmnStepContext<ExecutableFlowElementContainer> context) {

    messageCorrelated = false;

    final var bpmnProcessId = context.getValue().getBpmnProcessIdBuffer();
    final var workflowInstanceKey = context.getValue().getWorkflowInstanceKey();
    final var correlationKey = messageState.getWorkflowInstanceCorrelationKey(workflowInstanceKey);

    if (correlationKey != null) {

      messageState.removeWorkflowInstanceCorrelationKey(workflowInstanceKey);

      final var deployedWorkflow =
          context.getStateDb().getLatestWorkflowVersionByProcessId(bpmnProcessId);
      final var workflow = deployedWorkflow.getWorkflow();

      // TODO (saig0): keep order of messages - also for multiple message start events
      for (final ExecutableStartEvent startEvent : workflow.getStartEvents()) {

        if (startEvent.isMessage() && !messageCorrelated) {

          // TODO (saig0): extract correlation logic from PublishMessageProcessor
          messageState.visitMessages(
              startEvent.getMessage().getMessageName(),
              correlationKey,
              message -> {
                // correlate first message with same correlation key that was not correlated yet
                if (!messageState.existMessageCorrelation(message.getKey(), bpmnProcessId)) {

                  final var workflowKey = deployedWorkflow.getKey();

                  final boolean wasTriggered =
                      context
                          .getStateDb()
                          .getEventScopeInstanceState()
                          .triggerEvent(
                              workflowKey,
                              message.getKey(),
                              startEvent.getId(),
                              message.getVariables());

                  if (wasTriggered) {

                    final var newWorkflowInstanceKey = keyGenerator.nextKey();

                    context
                        .getOutput()
                        .appendFollowUpEvent(
                            newWorkflowInstanceKey,
                            WorkflowInstanceIntent.EVENT_OCCURRED,
                            startEventRecord
                                .setWorkflowKey(workflowKey)
                                .setWorkflowInstanceKey(newWorkflowInstanceKey)
                                .setElementId(startEvent.getId()));

                    messageState.putMessageCorrelation(message.getKey(), bpmnProcessId);
                    messageState.putWorkflowInstanceCorrelationKey(
                        newWorkflowInstanceKey, correlationKey);

                    messageCorrelated = true;
                    return false;
                  }
                }

                return true;
              });
        }
      }

      if (!messageCorrelated) {
        messageState.removeActiveWorkflowInstance(bpmnProcessId, correlationKey);
      }
    }
  }
}
