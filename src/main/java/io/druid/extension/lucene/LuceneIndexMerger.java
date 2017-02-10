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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.io.smoosh.FileSmoosher;
import com.metamx.emitter.EmittingLogger;
import io.druid.segment.IndexIO;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class LuceneIndexMerger
{
  private static final EmittingLogger log = new EmittingLogger(LuceneIndexMerger.class);
  public static final String SMOOSH_SUB_DIR = "smooshed";

  public static File merge(File persistDir, ObjectMapper mapper) throws IOException
  {
    Preconditions.checkArgument(
        persistDir != null && persistDir.isDirectory(),
        String.format("Persist dir [%s] is not valid", persistDir)
    );
    List<Directory> directories = Lists.newArrayList();
    for (File segment : persistDir.listFiles()) {
      if (segment.isDirectory()) {
        directories.add(FSDirectory.open(segment.toPath()));
      } else {
        log.warn("Skipping file [%s] as it is not a directory", segment);
      }
    }
    if (directories.isEmpty()) {
      return null;
    }

    final File tmpMergeTarget = new File(persistDir, "merged-tmp");
    FileUtils.deleteDirectory(tmpMergeTarget);
    final IndexWriterConfig config = new IndexWriterConfig();
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    // final TieredMergePolicy mergePolicy = new TieredMergePolicy();
    // mergePolicy.setNoCFSRatio(1.0);
    // config.setMergePolicy(mergePolicy);
    // config.setCommitOnClose(true);
    // config.setUseCompoundFile(true);
    config.setRAMBufferSizeMB(5000);
    final IndexWriter writer = new IndexWriter(FSDirectory.open(tmpMergeTarget.toPath()), config);
    writer.addIndexes((Directory[]) directories.toArray(new Directory[directories.size()]));
    // merge into a single segment and wait until the merge is complete
    writer.forceMerge(1, true);
    writer.commit();
    writer.close();
    File smooshedDir = new File(persistDir, SMOOSH_SUB_DIR);
    FileUtils.deleteDirectory(smooshedDir);
    smoosh(tmpMergeTarget, smooshedDir, mapper);
    FileUtils.deleteDirectory(tmpMergeTarget);
    return smooshedDir;
  }

  public static File smoosh(File luceneDirectory, File outDir, ObjectMapper mapper) throws IOException
  {
    // TODO it would be good to validate that luceneDirectory contains only valid lucene index files
    Preconditions.checkNotNull(luceneDirectory, "luceneDirectory is null");
    Preconditions.checkNotNull(
        luceneDirectory.listFiles(),
        String.format("[%s] is not a directory", luceneDirectory.getAbsolutePath())
    );
    Preconditions.checkNotNull(outDir, "output directory is null");

    Files.deleteIfExists(outDir.toPath());
    Files.createDirectory(outDir.toPath());

    final FileSmoosher fileSmoosher = new FileSmoosher(outDir);
    // does not return files names in alphabetical order, should not be a problem
    int count = 0;
    for (File file : luceneDirectory.listFiles()) {
      if (file.getName().endsWith(".lock")) {
        // skip write.lock file
        continue;
      }
      fileSmoosher.add(file);
      count++;
    }
    fileSmoosher.close();

    long startTime = System.currentTimeMillis();
    try (final FileOutputStream fos = new FileOutputStream(new File(outDir, "version.bin"))) {
      fos.write(Ints.toByteArray(IndexIO.V9_VERSION));
    }
    log.info("Completed version.bin in %,d millis.", System.currentTimeMillis() - startTime);

    try (final FileOutputStream factoryFos = new FileOutputStream(new File(outDir, "factory.json"))) {
      mapper.writeValue(factoryFos, new LuceneSegmentizerFactory());
    }

    if (count != fileSmoosher.getInternalFilenames().size()) {
      throw new ISE(
          "Excepted to write [%d] files but wrote [%d] ??",
          count,
          fileSmoosher.getInternalFilenames().size()
      );
    }
    return outDir;
  }
}
