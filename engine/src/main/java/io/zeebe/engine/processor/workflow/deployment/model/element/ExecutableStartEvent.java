/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.deployment.model.element;

import org.agrona.DirectBuffer;

public class ExecutableStartEvent extends ExecutableCatchEventElement {
  private DirectBuffer eventSubProcess;

  public ExecutableStartEvent(String id) {
    super(id);
  }

  public DirectBuffer getEventSubProcess() {
    return eventSubProcess;
  }

  public void setEventSubProcess(DirectBuffer eventSubProcess) {
    this.eventSubProcess = eventSubProcess;
  }

  @Override
  public boolean shouldCloseMessageSubscriptionOnCorrelate() {
    return interrupting();
  }
}
