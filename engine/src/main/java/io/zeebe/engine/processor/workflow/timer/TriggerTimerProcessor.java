/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.timer;

import static io.zeebe.engine.state.instance.TimerInstance.NO_ELEMENT_INSTANCE;
import static io.zeebe.util.buffer.BufferUtil.bufferAsString;

import io.zeebe.engine.processor.KeyGenerator;
import io.zeebe.engine.processor.TypedRecord;
import io.zeebe.engine.processor.TypedRecordProcessor;
import io.zeebe.engine.processor.TypedResponseWriter;
import io.zeebe.engine.processor.TypedStreamWriter;
import io.zeebe.engine.processor.workflow.CatchEventBehavior;
import io.zeebe.engine.processor.workflow.deployment.model.element.AbstractFlowElement;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableCatchEventElement;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableStartEvent;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.deployment.DeployedWorkflow;
import io.zeebe.engine.state.deployment.WorkflowState;
import io.zeebe.engine.state.instance.ElementInstance;
import io.zeebe.engine.state.instance.TimerInstance;
import io.zeebe.model.bpmn.util.time.RepeatingInterval;
import io.zeebe.msgpack.value.DocumentValue;
import io.zeebe.protocol.impl.record.value.timer.TimerRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.intent.TimerIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.util.buffer.BufferUtil;
import java.util.List;
import org.agrona.DirectBuffer;

public class TriggerTimerProcessor implements TypedRecordProcessor<TimerRecord> {
  private static final String NO_TIMER_FOUND_MESSAGE =
      "Expected to trigger timer with key '%d', but no such timer was found";
  private static final String NO_ACTIVE_TIMER_MESSAGE =
      "Expected to trigger a timer with key '%d', but the timer is not active anymore";

  private final CatchEventBehavior catchEventBehavior;
  private final WorkflowState workflowState;
  private final WorkflowInstanceRecord eventOccurredRecord = new WorkflowInstanceRecord();
  private final KeyGenerator keyGenerator;

  public TriggerTimerProcessor(
      final ZeebeState zeebeState, final CatchEventBehavior catchEventBehavior) {
    workflowState = zeebeState.getWorkflowState();
    keyGenerator = zeebeState.getKeyGenerator();
    this.catchEventBehavior = catchEventBehavior;
  }

  @Override
  public void processRecord(
      final TypedRecord<TimerRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    final TimerRecord timer = record.getValue();
    final long elementInstanceKey = timer.getElementInstanceKey();

    final TimerInstance timerInstance =
        workflowState.getTimerState().get(elementInstanceKey, record.getKey());
    if (timerInstance == null) {
      streamWriter.appendRejection(
          record, RejectionType.NOT_FOUND, String.format(NO_TIMER_FOUND_MESSAGE, record.getKey()));
      return;
    }
    workflowState.getTimerState().remove(timerInstance);

    processTimerTrigger(record, streamWriter, timer, elementInstanceKey);
  }

  private void processTimerTrigger(
      final TypedRecord<TimerRecord> record,
      final TypedStreamWriter streamWriter,
      final TimerRecord timer,
      final long elementInstanceKey) {
    final long eventScopeKey =
        isTimerStartEvent(elementInstanceKey)
            ? record.getValue().getWorkflowKey()
            : elementInstanceKey;

    final boolean wasActiveTimer = tryTriggerTimer(eventScopeKey, timer);
    if (wasActiveTimer) {
      final long eventOccurredKey = prepareEventOccurredEvent(timer, elementInstanceKey);

      streamWriter.appendFollowUpEvent(record.getKey(), TimerIntent.TRIGGERED, timer);
      streamWriter.appendFollowUpEvent(
          eventOccurredKey, WorkflowInstanceIntent.EVENT_OCCURRED, eventOccurredRecord);

      // todo(npepinpe): migrate to bpmn step processor
      if (shouldReschedule(timer)) {
        final ExecutableCatchEventElement timerEvent = getTimerEvent(elementInstanceKey, timer);

        if (timerEvent != null && timerEvent.isTimer()) {
          rescheduleTimer(timer, streamWriter, timerEvent);
        }
      }
    } else {
      streamWriter.appendRejection(
          record,
          RejectionType.INVALID_STATE,
          String.format(NO_ACTIVE_TIMER_MESSAGE, record.getKey()));
    }
  }

  private boolean tryTriggerTimer(final long eventScopeKey, final TimerRecord timer) {
    final long eventKey = keyGenerator.nextKey();
    return workflowState
        .getEventScopeInstanceState()
        .triggerEvent(
            eventScopeKey,
            eventKey,
            timer.getTargetElementIdBuffer(),
            DocumentValue.EMPTY_DOCUMENT);
  }

  private long prepareEventOccurredEvent(final TimerRecord timer, final long elementInstanceKey) {
    final long eventOccurredKey;

    eventOccurredRecord.reset();
    if (isTimerStartEvent(elementInstanceKey)) {

      eventOccurredKey = keyGenerator.nextKey();
      eventOccurredRecord
          .setBpmnElementType(BpmnElementType.START_EVENT)
          .setWorkflowKey(timer.getWorkflowKey())
          .setWorkflowInstanceKey(keyGenerator.nextKey())
          .setElementId(timer.getTargetElementIdBuffer());
    } else if (isInEventSubprocess(timer)) {
      eventOccurredKey = keyGenerator.nextKey();
      eventOccurredRecord.wrap(
          workflowState.getElementInstanceState().getInstance(elementInstanceKey).getValue());
      eventOccurredRecord
          .setBpmnElementType(BpmnElementType.START_EVENT)
          .setElementId(timer.getTargetElementIdBuffer())
          .setFlowScopeKey(elementInstanceKey);
    } else {
      eventOccurredKey = elementInstanceKey;
      eventOccurredRecord.wrap(
          workflowState.getElementInstanceState().getInstance(elementInstanceKey).getValue());
    }
    return eventOccurredKey;
  }

  private boolean isTimerStartEvent(final long elementInstanceKey) {
    return elementInstanceKey == NO_ELEMENT_INSTANCE;
  }

  private boolean isInEventSubprocess(final TimerRecord timer) {
    final ExecutableCatchEventElement catchEvent =
        getCatchEventById(timer.getWorkflowKey(), timer.getTargetElementIdBuffer());

    return catchEvent instanceof ExecutableStartEvent
        && ((ExecutableStartEvent) catchEvent).getEventSubProcess() != null;
  }

  private boolean shouldReschedule(final TimerRecord timer) {
    return timer.getRepetitions() == RepeatingInterval.INFINITE || timer.getRepetitions() > 1;
  }

  private ExecutableCatchEventElement getTimerEvent(
      final long elementInstanceKey, final TimerRecord timer) {
    if (isTimerStartEvent(elementInstanceKey)) {
      final List<ExecutableStartEvent> startEvents =
          workflowState.getWorkflowByKey(timer.getWorkflowKey()).getWorkflow().getStartEvents();

      for (final ExecutableCatchEventElement startEvent : startEvents) {
        if (startEvent.getId().equals(timer.getTargetElementIdBuffer())) {
          return startEvent;
        }
      }
    } else {
      final ElementInstance elementInstance =
          workflowState.getElementInstanceState().getInstance(elementInstanceKey);

      if (elementInstance != null) {
        return getCatchEventById(
            elementInstance.getValue().getWorkflowKey(), timer.getTargetElementIdBuffer());
      }
    }
    return null;
  }

  private void rescheduleTimer(
      final TimerRecord record,
      final TypedStreamWriter writer,
      final ExecutableCatchEventElement event) {
    if (event.getTimer() == null) {
      final String message =
          String.format(
              "Expected to reschedule repeating timer for element with id '%s', but no timer definition was found",
              BufferUtil.bufferAsString(event.getId()));
      throw new IllegalStateException(message);
    }

    int repetitions = record.getRepetitions();
    if (repetitions != RepeatingInterval.INFINITE) {
      repetitions--;
    }

    final RepeatingInterval timer =
        new RepeatingInterval(repetitions, event.getTimer().getInterval());
    catchEventBehavior.subscribeToTimerEvent(
        record.getElementInstanceKey(),
        record.getWorkflowInstanceKey(),
        record.getWorkflowKey(),
        event.getId(),
        timer,
        writer);
  }

  private ExecutableCatchEventElement getCatchEventById(
      final long workflowKey, final DirectBuffer id) {
    final DeployedWorkflow workflow = workflowState.getWorkflowByKey(workflowKey);
    if (workflow == null) {
      throw new IllegalStateException(
          String.format(
              "Expected to reschedule timer in workflow with key '%d', but no such workflow was found",
              workflowKey));
    }

    final AbstractFlowElement element = workflow.getWorkflow().getElementById(id);
    if (element == null) {
      throw new IllegalStateException(
          String.format(
              "Expected to reschedule timer for element with id '%s', but no such element was found",
              bufferAsString(id)));
    }

    if (!(element instanceof ExecutableCatchEventElement)) {
      throw new IllegalStateException(
          String.format(
              "Expected to reschedule timer for element with id '%s', but the element is not a timer catch event",
              workflowKey));
    }

    return (ExecutableCatchEventElement) element;
  }
}
