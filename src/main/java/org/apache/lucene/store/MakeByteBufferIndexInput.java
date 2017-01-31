package org.apache.lucene.store;

import java.nio.ByteBuffer;

/**
 * Used to access {@link ByteBufferIndexInput} which has
 * default access in org.apache.lucene.store package
 */
public class MakeByteBufferIndexInput
{
  public static ByteBufferIndexInput newByteBufferIndexInput(
      String resourceDescription,
      ByteBuffer[] buffers,
      long length,
      int chunkSizePower,
      ByteBufferGuard guard
  )
  {
    return ByteBufferIndexInput.newInstance(resourceDescription, buffers, length, chunkSizePower, guard);
  }

  public static ByteBufferGuard newByteBufferGuard(String resourceDescription, ByteBufferGuard.BufferCleaner bufferCleaner) {
    return new ByteBufferGuard(resourceDescription, bufferCleaner);
  }
}
