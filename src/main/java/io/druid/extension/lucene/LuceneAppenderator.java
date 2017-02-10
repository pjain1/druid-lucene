/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.extension.lucene;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.collect.JavaCompatUtils;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.AppenderatorConfig;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class LuceneAppenderator implements Appenderator, Runnable
{
  private static final EmittingLogger log = new EmittingLogger(
      LuceneAppenderator.class);
  private static final long DEFAULT_INDEX_REFRESH_INTERVAL_SECONDS = 5;

  private final DataSchema schema;
  private final LuceneDocumentBuilder docBuilder;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final AppenderatorConfig appenderatorConfig;
  private final ExecutorService queryExecutorService;
  private final ListeningExecutorService persistExecutor;
  private final ListeningExecutorService pushExecutor;
  private final Thread indexRefresher;
  private final ObjectMapper mapper;
  private final DataSegmentPusher dataSegmentPusher;
  private final DataSegmentAnnouncer dataSegmentAnnouncer;
  private final ServiceEmitter emitter;
  private volatile boolean isClosed = false;
  private final Map<SegmentIdentifier, LuceneDruidSegment> segments = Maps
      .newConcurrentMap();
  private final VersionedIntervalTimeline<String, LuceneDruidSegment> timeline = new VersionedIntervalTimeline<>(
      Ordering.natural());

  public LuceneAppenderator(
      DataSchema schema,
      AppenderatorConfig appenderatorConfig,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService queryExecutorService,
      ObjectMapper mapper,
      DataSegmentPusher segmentPusher,
      DataSegmentAnnouncer segmentAnnouncer,
      ServiceEmitter emitter
  )
  {
    this.schema = Preconditions.checkNotNull(schema);
    this.docBuilder = new LuceneDocumentBuilder(schema.getParser()
                                                      .getParseSpec().getDimensionsSpec());
    this.appenderatorConfig = Preconditions.checkNotNull(appenderatorConfig);
    this.queryExecutorService = Preconditions.checkNotNull(queryExecutorService);
    this.conglomerate = Preconditions.checkNotNull(conglomerate);
    this.mapper = Preconditions.checkNotNull(mapper);
    this.dataSegmentAnnouncer = Preconditions.checkNotNull(segmentAnnouncer);
    this.dataSegmentPusher = Preconditions.checkNotNull(segmentPusher);
    this.emitter = Preconditions.checkNotNull(emitter);
    this.indexRefresher = new Thread(this, "lucene index refresher");
    this.indexRefresher.setDaemon(true);
    this.persistExecutor = MoreExecutors.listeningDecorator(Execs.newBlockingSingleThreaded(
        "lucene_appenderator_persister_%d",
        0
    ));
    this.pushExecutor = MoreExecutors.listeningDecorator(Execs.newBlockingSingleThreaded(
        "lucene_appenderator_pusher_%d",
        0
    ));
  }

  @Override
  public String getDataSource()
  {
    return schema.getDataSource();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      Query<T> query,
      Iterable<Interval> intervals
  )
  {
    final List<SegmentDescriptor> specs = Lists.newArrayList();

    Iterables
        .addAll(
            specs,
            FunctionalIterable
                .create(intervals)
                .transformCat(
                    new Function<Interval, Iterable<TimelineObjectHolder<String, LuceneDruidSegment>>>()
                    {
                      @Override
                      public Iterable<TimelineObjectHolder<String, LuceneDruidSegment>> apply(
                          final Interval interval
                      )
                      {
                        return timeline.lookup(interval);
                      }
                    })
                .transformCat(
                    new Function<TimelineObjectHolder<String, LuceneDruidSegment>, Iterable<SegmentDescriptor>>()
                    {
                      @Override
                      public Iterable<SegmentDescriptor> apply(
                          final TimelineObjectHolder<String, LuceneDruidSegment> holder
                      )
                      {
                        return FunctionalIterable
                            .create(holder.getObject())
                            .transform(
                                new Function<PartitionChunk<LuceneDruidSegment>, SegmentDescriptor>()
                                {
                                  @Override
                                  public SegmentDescriptor apply(
                                      final PartitionChunk<LuceneDruidSegment> chunk
                                  )
                                  {
                                    return new SegmentDescriptor(holder
                                                                     .getInterval(), holder.getVersion(),
                                                                 chunk.getChunkNumber()
                                    );
                                  }
                                });
                      }
                    })
        );

    return getQueryRunnerForSegments(query, specs);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      Query<T> query,
      Iterable<SegmentDescriptor> specs
  )
  {
    // We only handle one dataSource. Make sure it's in the list of names, then
    // ignore from here on out.
    if (!query.getDataSource().getNames().contains(getDataSource())) {
      log.makeAlert("Received query for unknown dataSource")
         .addData("dataSource", query.getDataSource()).emit();
      return new NoopQueryRunner<>();
    }

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate
        .findFactory(query);
    if (factory == null) {
      log.makeAlert("Unknown query type, [%s]", query.getClass())
         .addData("dataSource", query.getDataSource()).emit();
      return new NoopQueryRunner<>();
    }

    final QueryToolChest<T, Query<T>> toolchest = factory.getToolchest();

    return toolchest.mergeResults(factory.mergeRunners(
        queryExecutorService,
        FunctionalIterable.create(specs).transform(
            new Function<SegmentDescriptor, QueryRunner<T>>()
            {
              @Override
              public QueryRunner<T> apply(final SegmentDescriptor descriptor)
              {
                final PartitionHolder<LuceneDruidSegment> holder = timeline
                    .findEntry(
                        descriptor.getInterval(),
                        descriptor.getVersion()
                    );
                if (holder == null) {
                  return new ReportTimelineMissingSegmentQueryRunner<>(
                      descriptor);
                }

                final PartitionChunk<LuceneDruidSegment> chunk = holder
                    .getChunk(descriptor.getPartitionNumber());
                if (chunk == null) {
                  return new ReportTimelineMissingSegmentQueryRunner<>(
                      descriptor);
                }

                final LuceneDruidSegment segment = chunk.getObject();

                return new SpecificSegmentQueryRunner<>(
                    new BySegmentQueryRunner<>(segment.getIdentifier(),
                                               descriptor.getInterval().getStart(), factory
                                                   .createRunner(segment)
                    ), new SpecificSegmentSpec(
                    descriptor));
              }
            })
    ));
  }

  @Override
  public Object startJob()
  {
    indexRefresher.start();
    return null;
  }

  @Override
  public int add(
      SegmentIdentifier identifier, InputRow row,
      Supplier<Committer> committerSupplier
  ) throws IndexSizeExceededException,
           SegmentNotWritableException
  {
    LuceneDruidSegment segment = segments.get(identifier);

    try {
      if (segment == null) {
        segment = new LuceneDruidSegment(
            identifier,
            appenderatorConfig.getBasePersistDirectory(),
            docBuilder,
            appenderatorConfig.getMaxRowsInMemory()
        );
        segments.put(identifier, segment);
        timeline.add(identifier.getInterval(), identifier.getVersion(),
                     identifier.getShardSpec().createChunk(segment)
        );
        dataSegmentAnnouncer.announceSegment(segment.getDataSegment());
      }
      segment.add(row);
      return segment.numRows();
    }
    catch (IOException ioe) {
      ioe.printStackTrace();
      throw new SegmentNotWritableException(ioe.getMessage(), ioe);
    }
  }

  @Override
  public List<SegmentIdentifier> getSegments()
  {
    return ImmutableList.copyOf(segments.keySet());
  }

  @Override
  public int getRowCount(SegmentIdentifier identifier)
  {
    LuceneDruidSegment segment = segments.get(identifier);
    return segment == null ? 0 : segment.numRows();
  }

  @Override
  public void clear() throws InterruptedException
  {
    List<ListenableFuture<?>> futures = Lists.newArrayList();
    for (SegmentIdentifier segmentIdentifier : JavaCompatUtils.keySet(segments)) {
      futures.add(drop(segmentIdentifier));
    }
    segments.clear();
    try {
      Futures.allAsList(futures).get();
    }
    catch (ExecutionException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * Insert a barrier into the merge-and-push queue. When this future resolves, all pending pushes will have finished.
   * This is useful if we're going to do something that would otherwise potentially break currently in-progress
   * pushes.
   */
  private ListenableFuture<?> pushBarrier()
  {
    return pushExecutor.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            // Do nothing
          }
        }
    );
  }

  @Override
  public ListenableFuture<?> drop(SegmentIdentifier identifier)
  {
    final LuceneDruidSegment segment = segments.get(identifier);
    if (segment != null) {
      return Futures.transform(
          pushBarrier(),
          new Function<Object, Object>()
          {
            @Override
            public Object apply(Object input)
            {
              timeline.remove(identifier.getInterval(), identifier.getVersion(),
                              identifier.getShardSpec().createChunk(segment)
              );
              segments.remove(identifier);
              try {
                segment.close();
                dataSegmentAnnouncer.unannounceSegment(segment.getDataSegment());
              }
              catch (IOException e) {
                log.error(e.getMessage(), e);
              }
              finally {
                try {
                  FileUtils.deleteDirectory(segment.getPersistDir());
                }
                catch (IOException e) {
                  log.makeAlert("Unable to delete directory: [%s], exception: [%s]", segment.getPersistDir(), e).emit();
                }
              }
              return null;
            }
          },
          persistExecutor
      );
    }
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Object> persistAll(Committer committer)
  {
    final String threadName = String.format("%s-incremental-persist", schema.getDataSource());
    return persistExecutor.submit(
        new ThreadRenamingCallable<Object>(threadName)
        {
          @Override
          public Object doCall()
          {
            for (LuceneDruidSegment segment : segments.values()) {
              try {
                segment.persist();
              }
              catch (IOException e) {
                log.error(e.getMessage(), e);
              }
            }
            committer.run();
            return committer.getMetadata();
          }
        }
    );
  }

  @Override
  public ListenableFuture<SegmentsAndMetadata> push(
      final List<SegmentIdentifier> identifiers, final Committer committer
  )
  {
    return Futures.transform(
        persistAll(committer),
        new Function<Object, SegmentsAndMetadata>()
        {
          @Override
          public SegmentsAndMetadata apply(Object commitMetadata)
          {
            final List<DataSegment> pushedSegments = Lists.newArrayList();
            for (SegmentIdentifier segmentIdentifier : identifiers) {
              log.info("Closing and merging segment [%s]", segmentIdentifier);
              LuceneDruidSegment segment = segments.get(segmentIdentifier);
              try {
                segment.close();
                final File pushedMarker = new File(segment.getPersistDir(), "pushed");
                if (pushedMarker.exists()) {
                  // Already pushed
                  log.warn("Already pushed [%s]", segmentIdentifier);
                  continue;
                }
                final Stopwatch mergeWatch = Stopwatch.createStarted();
                final File smooshedTarget = LuceneIndexMerger.merge(segment.getPersistDir(), mapper);

                log.info(
                    "Segment [%s] merged in [%d] mills at location [%s]",
                    segmentIdentifier,
                    mergeWatch.elapsed(TimeUnit.MILLISECONDS),
                    smooshedTarget
                );

                DataSegment pushedSegment = dataSegmentPusher.push(
                    smooshedTarget,
                    segment.getDataSegment().withDimensions(schema.getParser()
                                                                  .getParseSpec()
                                                                  .getDimensionsSpec()
                                                                  .getDimensionNames())
                );
                if(pushedSegment == null) {
                  log.warn("Skipping segment [%s]", segmentIdentifier);
                }
                pushedSegments.add(pushedSegment);
              }
              catch (IOException e) {
                log.makeAlert("Failed to push segment [%s]", segmentIdentifier).emit();
                Throwables.propagate(e);
              }
            }
            return new SegmentsAndMetadata(pushedSegments, committer.getMetadata());
          }
        },
        pushExecutor
    );
  }

  @Override
  public void close()
  {
    isClosed = true;
    try {
      clear();
      persistExecutor.shutdownNow();
      pushExecutor.shutdownNow();
      indexRefresher.interrupt();
      indexRefresher.join();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ISE("Failed to close() [%s]", e);
    }
  }

  @Override
  public void run()
  {
    while (!isClosed) {
      log.info("refresh index segments");
      for (LuceneDruidSegment segment : segments.values()) {
        try {
          segment.refreshRealtimeReader();
        }
        catch (IOException e) {
          log.error(e.getMessage(), e);
        }
      }

      try {
        Thread.sleep(DEFAULT_INDEX_REFRESH_INTERVAL_SECONDS * 1000); // refresh
        // eery
      }
      catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted!!");
      }
    }
  }
}
