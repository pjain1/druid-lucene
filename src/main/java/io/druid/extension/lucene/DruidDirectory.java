package io.druid.extension.lucene;

import com.metamx.common.io.smoosh.Smoosh;
import com.metamx.common.io.smoosh.SmooshedFileMapper;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MakeByteBufferIndexInput;
import org.apache.lucene.store.SingleInstanceLockFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DruidDirectory extends BaseDirectory
{
  private SmooshedFileMapper smooshedFileMapper;
  private Map<String, ByteBuffer> bufferMap;

  public DruidDirectory(File basePath) throws IOException
  {
    // Since this directory is only used for reading lucene segments
    // use SingleInstanceLockFactory instead of NativeFSLockFactory
    this(basePath, new SingleInstanceLockFactory());
  }

  public DruidDirectory(File basePath, LockFactory lockFactory) throws IOException
  {
    super(lockFactory);
    this.smooshedFileMapper = Smoosh.map(basePath);
    this.bufferMap = new HashMap<>();
  }

  @Override
  public String[] listAll() throws IOException
  {
    return smooshedFileMapper.getInternalFilenames()
                                        .toArray(new String[smooshedFileMapper.getInternalFilenames().size()]);
  }

  @Override
  public void deleteFile(String name) throws IOException
  {
    throw new UnsupportedOperationException("Don't call me bro!");
  }

  @Override
  public long fileLength(String name) throws IOException
  {
    return getByteBufferFor(name).remaining();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException
  {
    throw new UnsupportedOperationException("Don't call me bro!");
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException
  {
    throw new UnsupportedOperationException("Don't call me bro!");
  }

  @Override
  public void sync(Collection<String> names) throws IOException {}

  @Override
  public void rename(String source, String dest) throws IOException
  {
    throw new UnsupportedOperationException("Don't call me bro!");
  }

  @Override
  public void syncMetaData() throws IOException {}

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException
  {
    return MakeByteBufferIndexInput.newByteBufferIndexInput(
        String.format("%s_resource", name),
        new ByteBuffer[]{getByteBufferFor(name).asReadOnlyBuffer()},
        fileLength(name),
        30, // used when there are multiple bytebuffers to find out the buffer index to seek to
        MakeByteBufferIndexInput.newByteBufferGuard(String.format("%s_resource", name), null)
    );
  }

  // Returns ByteBuffer for file corresponding to the input name
  private ByteBuffer getByteBufferFor(String name) throws IOException
  {
    if (!bufferMap.containsKey(name)) {
      bufferMap.put(name, smooshedFileMapper.mapFile(name));
    }
    return bufferMap.get(name);
  }

  @Override
  public void close() throws IOException {}
}
