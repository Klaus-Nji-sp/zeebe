/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.msgpack;

import io.zeebe.msgpack.property.ArrayProperty;
import io.zeebe.msgpack.value.ValueArray;

public class POJOArray extends UnpackedObject {

  protected ArrayProperty<MinimalPOJO> simpleArrayProp;

  public POJOArray() {
    this.simpleArrayProp = new ArrayProperty<>("simpleArray", new MinimalPOJO());

    this.declareProperty(simpleArrayProp);
  }

  public ValueArray<MinimalPOJO> simpleArray() {
    return simpleArrayProp;
  }
}
