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

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.LinearPartitionChunk;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class LuceneAppenderatorTest
{
  private static final List<SegmentIdentifier> IDENTIFIERS = ImmutableList.of(
      SI("2000/2001", "A", 0),
      SI("2000/2001", "A", 1),
      SI("2001/2002", "A", 0)
  );

  @Test
  public void testSimpleIngestion() throws Exception
  {
    try (final LuceneAppenderatorTester tester = new LuceneAppenderatorTester(1)) {
      final Appenderator appenderator = tester.getAppenderator();

      final ConcurrentMap<String, String> commitMetadata = new ConcurrentHashMap<>();
      final Supplier<Committer> committerSupplier = committerSupplierFromConcurrentMap(commitMetadata);

      // startJob
      Assert.assertEquals(null, appenderator.startJob());

      // getDataSource
      Assert.assertEquals(LuceneAppenderatorTester.DATASOURCE, appenderator.getDataSource());

      // add
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);

      appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 4), committerSupplier);
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 4), committerSupplier);
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 4), committerSupplier);

      appenderator.persistAll(committerSupplier.get());

      Assert.assertEquals(3, appenderator.getRowCount(IDENTIFIERS.get(0)));
      Assert.assertEquals(3, appenderator.getRowCount(IDENTIFIERS.get(1)));


      final SegmentsAndMetadata segmentsAndMetadata = appenderator.push(
          appenderator.getSegments(),
          committerSupplier.get()
      ).get();

      // clear
      appenderator.clear();
      Assert.assertTrue(appenderator.getSegments().isEmpty());
    }
  }


  @Test
  public void testQuery() throws Exception
  {
    try (final LuceneAppenderatorTester tester = new LuceneAppenderatorTester(10000)) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "bar", 2), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 4), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001", "foo", 8), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001T01", "foo", 16), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001T02", "foo", 32), Suppliers.ofInstance(Committers.nil()));

      Thread.sleep(5000);

      // Query1: foo/bar
      final LuceneDruidQuery query1 = new LuceneDruidQuery(
          new TableDataSource(LuceneAppenderatorTester.DATASOURCE),
          new LegacySegmentSpec(ImmutableList.of(new Interval("2000/2002"))),
          null,
          "dim",
          "bar",
          1
      );

      final List<Result<LuceneQueryResultValue>> results1 = Lists.newArrayList();
      Sequences.toList(query1.run(appenderator, ImmutableMap.<String, Object>of()), results1);
      Assert.assertEquals(
          "query1",
          ImmutableList.of(
              new Result<>(
                  new DateTime("2000"),
                  new LuceneQueryResultValue(2, 6)
              )

          ),
          results1
      );

    }
  }

  @Test
  public void testQueryPersistedSegment() throws Exception
  {
    final VersionedIntervalTimeline<String, LuceneDruidSegment> timeline = new VersionedIntervalTimeline<>(
        Ordering.natural());
    final File persistDir = Files.createTempDir();
    final Map<String, File> segmentsDir = new HashMap<>();
    try (final LuceneAppenderatorTester tester = new LuceneAppenderatorTester(1, persistDir)) {
      final Appenderator appenderator = tester.getAppenderator();

      //final ConcurrentMap<String, String> commitMetadata = new ConcurrentHashMap<>();
      final Supplier<Committer> committerSupplier = Suppliers.ofInstance(Committers.nil());

      // startJob
      Assert.assertEquals(null, appenderator.startJob());

      // getDataSource
      Assert.assertEquals(LuceneAppenderatorTester.DATASOURCE, appenderator.getDataSource());

      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "bar", 2), committerSupplier);
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 4), committerSupplier);
      appenderator.add(IDENTIFIERS.get(2), IR("2001", "foo", 8), committerSupplier);
      appenderator.add(IDENTIFIERS.get(2), IR("2001T01", "foo", 16), committerSupplier);
      appenderator.add(IDENTIFIERS.get(2), IR("2001T02", "foo", 32), committerSupplier);

      /*
      // add
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);

      appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 4), committerSupplier);
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 4), committerSupplier);
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 4), committerSupplier);
      */

      appenderator.persistAll(committerSupplier.get());

      Assert.assertEquals(2, appenderator.getRowCount(IDENTIFIERS.get(0)));
      Assert.assertEquals(1, appenderator.getRowCount(IDENTIFIERS.get(1)));
      Assert.assertEquals(3, appenderator.getRowCount(IDENTIFIERS.get(2)));

      final SegmentsAndMetadata segmentsAndMetadata = appenderator.push(
          appenderator.getSegments(),
          committerSupplier.get()
      ).get();

      for (SegmentIdentifier segment : appenderator.getSegments()) {
        segmentsDir.put(
            String.format(
                "%s_%s_%s",
                segment.getInterval().toString(),
                segment.getVersion(),
                segment.getShardSpec().getPartitionNum()
            ),
            new File(new File(persistDir, segment.getIdentifierAsString()), LuceneIndexMerger.SMOOSH_SUB_DIR)
        );
      }

      // Query1: foo/bar
      final LuceneDruidQuery query1 = new LuceneDruidQuery(
          new TableDataSource(LuceneAppenderatorTester.DATASOURCE),
          new LegacySegmentSpec(ImmutableList.of(new Interval("2000/2002"))),
          null,
          "dim",
          "bar",
          1
      );

      final List<Result<LuceneQueryResultValue>> results1 = Lists.newArrayList();

      Sequences.toList(query1.run(new QuerySegmentWalker()
      {
        @Override
        public <T> QueryRunner<T> getQueryRunnerForIntervals(
            Query<T> query, Iterable<Interval> intervals
        )
        {
          List<SegmentDescriptor> specs = Lists.newArrayList();
          for (SegmentIdentifier segment : appenderator.getSegments()) {
            timeline.add(segment.getInterval(), segment.getVersion(), new LinearPartitionChunk<>(
                segment.getShardSpec().getPartitionNum(),
                null // we don't require this for the test
            ));

          }

          Iterables.addAll(
              specs,
              FunctionalIterable.create(intervals).transformCat(
                  new Function<Interval, Iterable<SegmentDescriptor>>()
                  {
                    @Override
                    public Iterable<SegmentDescriptor> apply(Interval interval)
                    {
                      return FunctionalIterable.create(timeline.lookup(interval)).transformCat(
                          new Function<TimelineObjectHolder<String, LuceneDruidSegment>, Iterable<SegmentDescriptor>>()
                          {
                            @Override
                            public Iterable<SegmentDescriptor> apply(
                                TimelineObjectHolder<String, LuceneDruidSegment> holder
                            )
                            {
                              return FunctionalIterable.create(holder.getObject()).transform(
                                  new Function<PartitionChunk<LuceneDruidSegment>, SegmentDescriptor>()
                                  {
                                    @Override
                                    public SegmentDescriptor apply(
                                        PartitionChunk<LuceneDruidSegment> input
                                    )
                                    {
                                      return new SegmentDescriptor(
                                          holder.getInterval(),
                                          holder.getVersion(),
                                          input.getChunkNumber()
                                      );
                                    }
                                  }
                              );
                            }
                          }
                      );
                    }
                  }
              )
          );
          return getQueryRunnerForSegments(query, specs);
        }

        @Override
        public <T> QueryRunner<T> getQueryRunnerForSegments(
            Query<T> query, Iterable<SegmentDescriptor> specs
        )
        {
          final LuceneQueryRunnerFactoryConglomerate conglomerate = new LuceneQueryRunnerFactoryConglomerate();
          final QueryRunnerFactory factory = conglomerate.findFactory(query);
          final QueryToolChest toolChest = new LuceneQueryToolChest();
          return toolChest.mergeResults(
              factory.mergeRunners(
                  Execs.singleThreaded("lucene-runner"),
                  FunctionalIterable.create(specs).transform(
                      new Function<SegmentDescriptor, QueryRunner>()
                      {
                        @Nullable
                        @Override
                        public QueryRunner apply(@Nullable SegmentDescriptor input)
                        {
                          try {
                            return factory.createRunner(LuceneDruidSegment.fromPersistedDir(
                                new DataSegment(
                                    LuceneAppenderatorTester.DATASOURCE,
                                    input.getInterval(),
                                    input.getVersion(),
                                    null,
                                    null,
                                    null,
                                    new LinearShardSpec(input.getPartitionNumber()),
                                    1,
                                    0
                                ),
                                segmentsDir.get(String.format(
                                    "%s_%s_%s",
                                    input.getInterval().toString(),
                                    input.getVersion(),
                                    input.getPartitionNumber()
                                ))
                            ));
                          }
                          catch (IOException e) {
                            e.printStackTrace();
                            Throwables.propagate(e);
                          }
                          return null;
                        }
                      }
                  )
              )
          );
        }
      }, ImmutableMap.<String, Object>of()), results1);

      Assert.assertEquals(
          "query1",
          ImmutableList.of(
              new Result<>(
                  new DateTime("2000"),
                  new LuceneQueryResultValue(2, 6)
              )

          ),
          results1
      );

      // clear
      appenderator.clear();
      Assert.assertTrue(appenderator.getSegments().isEmpty());
    }
  }

  private static class LuceneQueryRunnerFactoryConglomerate implements QueryRunnerFactoryConglomerate
  {
    QueryRunnerFactory luceneQueryRunnerFactory = new LuceneQueryRunnerFactory(new QueryWatcher()
    {
      @Override
      public void registerQuery(Query query, ListenableFuture future)
      {
        //do nothing
      }
    });

    @Override
    public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query)
    {
      return luceneQueryRunnerFactory;
    }
  }

  private static SegmentIdentifier SI(String interval, String version, int partitionNum)
  {
    return new SegmentIdentifier(
        LuceneAppenderatorTester.DATASOURCE,
        new Interval(interval),
        version,
        new LinearShardSpec(partitionNum)
    );
  }

  static InputRow IR(String ts, String dim, long met)
  {
    return new MapBasedInputRow(
        new DateTime(ts).getMillis(),
        ImmutableList.of("dim", "met"),
        ImmutableMap.<String, Object>of(
            "dim",
            dim,
            "met",
            met
        )
    );
  }

  private static Supplier<Committer> committerSupplierFromConcurrentMap(final ConcurrentMap<String, String> map)
  {
    return new Supplier<Committer>()
    {
      @Override
      public Committer get()
      {
        final Map<String, String> mapCopy = ImmutableMap.copyOf(map);

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return mapCopy;
          }

          @Override
          public void run()
          {
            // Do nothing
          }
        };
      }
    };
  }

  private static <T> List<T> sorted(final List<T> xs)
  {
    final List<T> xsSorted = Lists.newArrayList(xs);
    Collections.sort(
        xsSorted, new Comparator<T>()
        {
          @Override
          public int compare(T a, T b)
          {
            if (a instanceof SegmentIdentifier && b instanceof SegmentIdentifier) {
              return ((SegmentIdentifier) a).getIdentifierAsString()
                                            .compareTo(((SegmentIdentifier) b).getIdentifierAsString());
            } else if (a instanceof DataSegment && b instanceof DataSegment) {
              return ((DataSegment) a).getIdentifier()
                                      .compareTo(((DataSegment) b).getIdentifier());
            } else {
              throw new IllegalStateException("WTF??");
            }
          }
        }
    );
    return xsSorted;
  }

  public static void main(String[] args)
  {
    Interval i1 = new Interval(0, 10);
    Interval i2 = new Interval(1000000, 10000001);
    Interval i3 = new Interval(10000, 100003);
    TreeMap<Interval, Seg> map = new TreeMap<Interval, Seg>(new Comparator<Interval>()
    {
      @Override
      public int compare(Interval o1, Interval o2)
      {
        return o1.getStart().compareTo(o2.getStart());
      }
    });

  }

  static class Seg
  {
    Interval interval;
    String version;
  }
}