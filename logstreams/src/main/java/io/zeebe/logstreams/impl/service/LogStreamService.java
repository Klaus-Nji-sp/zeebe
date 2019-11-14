/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl.service;

import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.distributedLogPartitionServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStorageAppenderRootService;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStorageAppenderServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStreamRootServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logWriteBufferServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logWriteBufferSubscriptionServiceName;

import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.dispatcher.DispatcherBuilder;
import io.zeebe.dispatcher.Dispatchers;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.dispatcher.impl.PositionUtil;
import io.zeebe.logstreams.impl.LogStorageAppender;
import io.zeebe.logstreams.impl.LogStreamBuilder;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.CompositeServiceBuilder;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.ByteValue;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.channel.ActorConditions;
import io.zeebe.util.sched.future.ActorFuture;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.agrona.concurrent.status.Position;
import org.slf4j.Logger;

public class LogStreamService implements LogStream {
  public static final long INVALID_ADDRESS = -1L;

  private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;
  private static final String APPENDER_SUBSCRIPTION_NAME = "appender";

  private final ActorConditions onCommitPositionUpdatedConditions;
  private final String logName;
  private final int partitionId;
  private final ByteValue maxFrameLength;
  private final Position commitPosition;

  private BufferedLogStreamReader reader;
  private ServiceStartContext serviceContext;
  private LogStorage logStorage;
  private ActorFuture<Dispatcher> writeBufferFuture;
  private ActorFuture<LogStorageAppender> appenderFuture;
  private Dispatcher writeBuffer;
  private LogStorageAppender appender;

  public LogStreamService(final LogStreamBuilder builder) {
    logName = builder.getLogName();
    partitionId = builder.getPartitionId();
    onCommitPositionUpdatedConditions = builder.getOnCommitPositionUpdatedConditions();
    commitPosition = builder.getCommitPosition();
    maxFrameLength = ByteValue.ofBytes(builder.getMaxFragmentSize());
    commitPosition.setVolatile(INVALID_ADDRESS);
    this.reader = new BufferedLogStreamReader(logStorage);
    setCommitPosition(reader.seekToEnd());
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public String getLogName() {
    return logName;
  }

  @Override
  public void close() {
    closeAsync().join();
  }

  @Override
  public ActorFuture<Void> closeAsync() {

    // todo close everything
    throw new UnsupportedOperationException("");
  }

  @Override
  public long getCommitPosition() {
    return commitPosition.get();
  }

  private void setCommitPosition(final long commitPosition) {
    this.commitPosition.setOrdered(commitPosition);

    onCommitPositionUpdatedConditions.signalConsumers();
  }

  @Override
  public long append(long commitPosition, ByteBuffer buffer) {
    long appendResult = -1;
    boolean notAppended = true;
    do {
      try {
        appendResult = logStorage.append(buffer);
        notAppended = false;
      } catch (IOException ioe) {
        // we want to retry the append
        // we avoid recursion, otherwise we can get stack overflow exceptions
        LOG.error(
            "Expected to append new buffer, but caught IOException. Will retry this operation.",
            ioe);
      }
    } while (notAppended);

    setCommitPosition(commitPosition);

    return appendResult;
  }

  @Override
  public LogStorage getLogStorage() {
    return logStorage;
  }

  @Override
  public Dispatcher getWriteBuffer() {
    if (writeBuffer == null && writeBufferFuture != null) {
      writeBuffer = writeBufferFuture.join();
    }
    return writeBuffer;
  }

  @Override
  public LogStorageAppender getLogStorageAppender() {
    if (appender == null && appenderFuture != null) {
      appender = appenderFuture.join();
    }
    return appender;
  }

  @Override
  public ActorFuture<Void> closeAppender() {
    appenderFuture = null;
    writeBufferFuture = null;
    appender = null;
    writeBuffer = null;

    final String logName = getLogName();
    return serviceContext.removeService(logStorageAppenderRootService(logName));
  }

  @Override
  public ActorFuture<LogStorageAppender> openAppender() {
    final String logName = getLogName();
    final ServiceName<Void> logStorageAppenderRootService = logStorageAppenderRootService(logName);
    final ServiceName<Dispatcher> logWriteBufferServiceName = logWriteBufferServiceName(logName);
    final ServiceName<Subscription> appenderSubscriptionServiceName =
        logWriteBufferSubscriptionServiceName(logName, APPENDER_SUBSCRIPTION_NAME);
    final ServiceName<LogStorageAppender> logStorageAppenderServiceName =
        logStorageAppenderServiceName(logName);

    final DispatcherBuilder writeBufferBuilder =
        Dispatchers.create(logWriteBufferServiceName.getName()).maxFragmentLength(maxFrameLength);

    final CompositeServiceBuilder installOperation =
        serviceContext.createComposite(logStorageAppenderRootService);

    final int partitionId = determineInitialPartitionId();
    writeBuffer = writeBufferBuilder
      .initialPartitionId(partitionId + 1)
      .actorScheduler(actorScheduler)
      .name(logWriteBufferServiceName.getName())
      .build();

    final LogWriteBufferSubscriptionService subscriptionService =
        new LogWriteBufferSubscriptionService(APPENDER_SUBSCRIPTION_NAME);

    ActorFuture<Subscription> subscriptionFuture = writeBuffer
      .openSubscriptionAsync(APPENDER_SUBSCRIPTION_NAME);

    // todo close subscription
    // stopContext.async(logBuffer.closeSubscriptionAsync(subscriptionFuture.join()));



    new LogStorageAppender(
      logStorageAppenderServiceName.getName(),
      primitive,
      subscription,
      maxAppendBlockSize)

    final LogStorageAppenderService appenderService =
        new LogStorageAppenderService((int) maxFrameLength.toBytes());
    appenderFuture =
        installOperation
            .createService(logStorageAppenderServiceName, appenderService)
            .dependency(
                appenderSubscriptionServiceName, appenderService.getAppenderSubscriptionInjector())
            .dependency(
                distributedLogPartitionServiceName(logName),
                appenderService.getDistributedLogstreamInjector())
            .install();

    return installOperation.installAndReturn(logStorageAppenderServiceName);
  }


  private int determineInitialPartitionId() {
    try (BufferedLogStreamReader logReader = new BufferedLogStreamReader()) {
      logReader.wrap(logStorage);

      // Get position of last entry
      final long lastPosition = logReader.seekToEnd();

      // dispatcher needs to generate positions greater than the last position
      int partitionId = 0;

      if (lastPosition > 0) {
        partitionId = PositionUtil.partitionId(lastPosition);
      }

      return partitionId;
    }
  }

  @Override
  public void delete(long position) {
    final boolean positionNotExist = !reader.seek(position);
    if (positionNotExist) {
      LOG.debug(
          "Tried to delete from log stream, but found no corresponding address in the log block index for the given position {}.",
          position);
      return;
    }

    final long blockAddress = reader.lastReadAddress();
    LOG.debug(
        "Delete data from log stream until position '{}' (address: '{}').", position, blockAddress);

    logStorage.delete(blockAddress);
  }

  @Override
  public void registerOnCommitPositionUpdatedCondition(final ActorCondition condition) {
    onCommitPositionUpdatedConditions.registerConsumer(condition);
  }

  @Override
  public void removeOnCommitPositionUpdatedCondition(final ActorCondition condition) {
    onCommitPositionUpdatedConditions.removeConsumer(condition);
  }

  public Injector<LogStorage> getLogStorageInjector() {
    return logStorageInjector;
  }
}
