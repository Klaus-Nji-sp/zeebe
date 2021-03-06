/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.cmd;

import io.grpc.Status;

public class InvalidBrokerRequestArgumentException extends ClientException
    implements GrpcStatusException {

  private static final String MESSAGE_FORMAT = "Expected argument '%s' to be %s, but was %s";
  private static final long serialVersionUID = -1582037715962211105L;
  private final String argument;
  private final String expectedValue;
  private final String actualValue;

  public InvalidBrokerRequestArgumentException(
      String argument, String expectedValue, String actualValue) {
    this(argument, expectedValue, actualValue, null);
  }

  public InvalidBrokerRequestArgumentException(
      String argument, String expectedValue, String actualValue, Throwable cause) {
    super(String.format(MESSAGE_FORMAT, argument, expectedValue, actualValue), cause);

    this.argument = argument;
    this.expectedValue = expectedValue;
    this.actualValue = actualValue;
  }

  public String getArgument() {
    return argument;
  }

  public String getExpectedValue() {
    return expectedValue;
  }

  public String getActualValue() {
    return actualValue;
  }

  @Override
  public Status getGrpcStatus() {
    return Status.INVALID_ARGUMENT.augmentDescription(getMessage());
  }
}
