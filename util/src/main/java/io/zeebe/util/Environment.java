/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public class Environment {

  private static final Logger LOG = Loggers.CONFIG_LOGGER;

  private final Map<String, String> environment;

  public Environment() {
    this(System.getenv());
  }

  public Environment(Map<String, String> environment) {
    this.environment = environment;
  }

  public Optional<String> get(String name) {
    return Optional.ofNullable(environment.get(name));
  }

  public Optional<Integer> getInt(String name) {
    try {
      return get(name).map(Integer::valueOf);
    } catch (Exception e) {
      LOG.warn("Failed to parse environment variable {}", name, e);
      return Optional.empty();
    }
  }

  public Optional<Double> getDouble(String name) {
    try {
      return get(name).map(Double::valueOf);
    } catch (Exception e) {
      LOG.warn("Failed to parse environment variable {}", name, e);
      return Optional.empty();
    }
  }

  public Optional<Long> getLong(String name) {
    try {
      return get(name).map(Long::valueOf);
    } catch (Exception e) {
      LOG.warn("Failed to parse environment variable {}", name, e);
      return Optional.empty();
    }
  }

  public Optional<Boolean> getBool(String name) {
    try {
      return get(name).map(Boolean::valueOf);
    } catch (Exception e) {
      LOG.warn("Failed to parse environment variable {}", name, e);
      return Optional.empty();
    }
  }

  public Optional<List<String>> getList(final String name) {
    return get(name)
        .map(v -> v.split(","))
        .map(Arrays::asList)
        .map(
            list ->
                list.stream()
                    .map(String::trim)
                    .filter(e -> !e.isEmpty())
                    .collect(Collectors.toList()));
  }
}
