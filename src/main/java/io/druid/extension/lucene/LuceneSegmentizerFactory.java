package io.druid.extension.lucene;

import io.druid.segment.Segment;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.loading.SegmentizerFactory;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;

public class LuceneSegmentizerFactory implements SegmentizerFactory
{
  @Override
  public Segment factorize(DataSegment dataSegment, File file) throws SegmentLoadingException
  {
    try {
      return LuceneDruidSegment.fromPersistedDir(dataSegment, file);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "%s", e.getMessage());
    }
  }
}
