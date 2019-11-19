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
import io.zeebe.util.sched.clock.ActorClock;
import java.util.function.Consumer;

public class MessageStartWorkflowInstancePoller
    implements Consumer<BpmnStepContext<ExecutableFlowElementContainer>> {

  private final MessageState messageState;
  private final KeyGenerator keyGenerator;

  // private final MessageStartEventSubscriptionState subscriptionState;

  private final WorkflowInstanceRecord startEventRecord =
      new WorkflowInstanceRecord().setBpmnElementType(BpmnElementType.START_EVENT);

  private long messageKeyToCorrelate;
  private ExecutableStartEvent startEventToCorrelate;

  public MessageStartWorkflowInstancePoller(
      final KeyGenerator keyGenerator, final MessageState messageState) {
    this.keyGenerator = keyGenerator;
    this.messageState = messageState;
  }

  @Override
  public void accept(final BpmnStepContext<ExecutableFlowElementContainer> context) {

    messageKeyToCorrelate = Long.MAX_VALUE;
    startEventToCorrelate = null;

    final var bpmnProcessId = context.getValue().getBpmnProcessIdBuffer();
    final var workflowInstanceKey = context.getValue().getWorkflowInstanceKey();
    final var correlationKey = messageState.getWorkflowInstanceCorrelationKey(workflowInstanceKey);

    if (correlationKey != null) {

      messageState.removeWorkflowInstanceCorrelationKey(workflowInstanceKey);

      final var deployedWorkflow =
          context.getStateDb().getLatestWorkflowVersionByProcessId(bpmnProcessId);
      final var workflow = deployedWorkflow.getWorkflow();

      for (final ExecutableStartEvent startEvent : workflow.getStartEvents()) {
        if (startEvent.isMessage()) {

          messageState.visitMessages(
              startEvent.getMessage().getMessageName(),
              correlationKey,
              message -> {
                // correlate first message with same correlation key that was not correlated yet
                if (message.getDeadline() > ActorClock.currentTimeMillis()
                    && !messageState.existMessageCorrelation(message.getKey(), bpmnProcessId)) {

                  // correlate to the first message across all message start events
                  // - using the message key to decide which message was correlated before
                  if (message.getKey() < messageKeyToCorrelate) {
                    messageKeyToCorrelate = message.getKey();
                    startEventToCorrelate = startEvent;
                  }

                  return false;
                }

                return true;
              });
        }
      }

      if (startEventToCorrelate == null) {
        // no pending message to correlate
        messageState.removeActiveWorkflowInstance(bpmnProcessId, correlationKey);

      } else {
        // correlate next pending message by creating a new workflow instance
        final var message = messageState.getMessage(messageKeyToCorrelate);

        final var workflowKey = deployedWorkflow.getKey();

        // TODO (saig0): extract correlation logic from PublishMessageProcessor
        final boolean wasTriggered =
            context
                .getStateDb()
                .getEventScopeInstanceState()
                .triggerEvent(
                    workflowKey,
                    message.getKey(),
                    startEventToCorrelate.getId(),
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
                      .setElementId(startEventToCorrelate.getId()));

          messageState.putMessageCorrelation(message.getKey(), bpmnProcessId);
          messageState.putWorkflowInstanceCorrelationKey(newWorkflowInstanceKey, correlationKey);
        }
      }
    }
  }
}
