/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl;

import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStorageServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStreamRootServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStreamServiceName;
import static io.zeebe.util.EnsureUtil.ensureGreaterThanOrEqual;

import io.zeebe.logstreams.impl.log.fs.FsLogStorage;
import io.zeebe.logstreams.impl.log.fs.FsLogStorageConfiguration;
import io.zeebe.logstreams.impl.service.FsLogStorageService;
import io.zeebe.logstreams.impl.service.LogStreamService;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.CompositeServiceBuilder;
import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.util.ByteValue;
import io.zeebe.util.sched.channel.ActorConditions;
import io.zeebe.util.sched.future.ActorFuture;
import java.io.File;
import java.util.Objects;
import java.util.function.Function;
import org.agrona.concurrent.status.AtomicLongPosition;

public class LogStreamBuilder {
  protected final int partitionId;
  protected final AtomicLongPosition commitPosition = new AtomicLongPosition();
  protected final ActorConditions onCommitPositionUpdatedConditions = new ActorConditions();
  protected String logName;
  protected String logRootPath;
  protected String logDirectory;
  protected int initialLogSegmentId = 0;
  protected boolean deleteOnClose;
  protected int maxFragmentSize = 1024 * 1024 * 4;
  protected int logSegmentSize = 1024 * 1024 * 128;
  protected Function<FsLogStorage, FsLogStorage> logStorageStubber = Function.identity();

  public LogStreamBuilder(final int partitionId) {
    this.partitionId = partitionId;
  }

  public LogStreamBuilder logName(final String logName) {
    this.logName = logName;
    return this;
  }

  public LogStreamBuilder logRootPath(final String logRootPath) {
    this.logRootPath = logRootPath;
    return this;
  }

  public LogStreamBuilder logDirectory(final String logDir) {
    logDirectory = logDir;
    return this;
  }

  public LogStreamBuilder initialLogSegmentId(final int logFragmentId) {
    initialLogSegmentId = logFragmentId;
    return this;
  }

  public LogStreamBuilder logSegmentSize(final int logSegmentSize) {
    this.logSegmentSize = logSegmentSize;
    return this;
  }

  public LogStreamBuilder deleteOnClose(final boolean deleteOnClose) {
    this.deleteOnClose = deleteOnClose;
    return this;
  }

  public LogStreamBuilder logStorageStubber(
      final Function<FsLogStorage, FsLogStorage> logStorageStubber) {
    this.logStorageStubber = logStorageStubber;
    return this;
  }

  public LogStreamBuilder maxFragmentSize(final int maxFragmentSize) {
    this.maxFragmentSize = maxFragmentSize;
    return this;
  }

  public int getMaxFragmentSize() {
    return maxFragmentSize;
  }

  public String getLogName() {
    return logName;
  }

  public String getLogDirectory() {
    if (logDirectory == null) {
      logDirectory = logRootPath + File.separatorChar + logName + File.separatorChar;
    }
    return logDirectory;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public AtomicLongPosition getCommitPosition() {
    return commitPosition;
  }

  public ActorConditions getOnCommitPositionUpdatedConditions() {
    return onCommitPositionUpdatedConditions;
  }

  public LogStream build() {
    validate();

    final FsLogStorageConfiguration storageConfig =
      new FsLogStorageConfiguration(
        logSegmentSize, getLogDirectory(), initialLogSegmentId, deleteOnClose);

    final FsLogStorage logStorage = logStorageStubber.apply(new FsLogStorage(storageConfig));

    return new LogStreamService(this, logStorage);
  }


  private void validate() {
    Objects.requireNonNull(logName, "logName");
    ensureGreaterThanOrEqual("partitionId", partitionId, 0);

    if (logSegmentSize < maxFragmentSize) {
      throw new IllegalArgumentException(
          String.format(
              "Expected the log segment size greater than the max fragment size of %s, but was %s.",
              ByteValue.ofBytes(maxFragmentSize), ByteValue.ofBytes(logSegmentSize)));
    }
  }
}
