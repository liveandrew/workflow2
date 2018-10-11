package com.liveramp.workflow2.workflow_hadoop;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.cascading_ext.resource.InMemoryStorage;
import com.liveramp.cascading_ext.resource.Storage;
import com.liveramp.commons.util.serialization.JavaObjectSerializationHandler;
import com.liveramp.commons.util.serialization.SerializationHandler;
import com.rapleaf.formats.stream.RecordInputStream;
import com.rapleaf.formats.stream.RecordOutputStream;
import com.rapleaf.formats.stream.SequenceFileInputStream;
import com.rapleaf.formats.stream.SequenceFileOutputStream;

public class HdfsStorage implements Storage {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsStorage.class);

  private final SerializationHandler handler;
  private final InMemoryStorage cache;
  private String resourceRoot;
  private FileSystem fs;

  public HdfsStorage(String resourceRoot) {
    this.handler = new JavaObjectSerializationHandler();
    this.cache = new InMemoryStorage();
    this.resourceRoot = resourceRoot;
    this.fs = FileSystemHelper.getFileSystemForPath(resourceRoot);

  }

  private Path getPath(String name) {
    return new Path(resourceRoot + name);
  }

  @Override
  public <T> void store(String name, T object) {
    cache.store(name, object);
    try {

      Path path =  getPath(name);
      if (fs.exists(path)) {
        TrashHelper.deleteUsingTrashIfEnabled(fs, path);
      }

      byte[] serialized;

      if(object instanceof Serializable) {
        serialized = handler.serialize(object);
      } else {
        throw new RuntimeException("Could not serialize resource '" + name + "' with value " + object);
      }

      RecordOutputStream os = new SequenceFileOutputStream(fs, path);
      os.write(serialized);
      os.close();

      LOG.info("Stored resource '" + name + "' to " + path.toString());

    } catch (Throwable t) {
      throw new RuntimeException("Unable to store resource '" + name + "' and value " + object, t);
    }
  }

  @Override
  public <T> T retrieve(String name) {

    if(cache.isStored(name)) {
      LOG.info("Retrieved resource '" + name + "' from the cache");
      return (T)cache.retrieve(name);
    }

    try {

      Path path = getPath(name);
      if (fs.exists(path)) {

        RecordInputStream is = new SequenceFileInputStream(fs, path);
        byte[] data = is.readRecord();

        if(is.readRecord() != null) {
          throw new AssertionError("Resource should not contain more than a single record");
        }

        LOG.info("Retrieved resource '" + name + "' from path " + path.toString());

        return (T)handler.deserialize(data);

      } else {

        LOG.info("Failed to find Resource'" + name + "' at path " + path.toString());

        return null;

      }

    } catch (Throwable t) {
      throw new RuntimeException("Unable to retrieve resource with name " + name, t);
    }
  }

  public String getRoot() {
    return resourceRoot;
  }

  @Override
  public boolean isStored(String name) {
    if(cache.isStored(name)) {
      return true;
    }

    try {
      return fs.exists(getPath(name));
    } catch (Throwable t) {
      throw new RuntimeException("Unable to determine whether resource with name'" + name + "' is stored or not", t);
    }
  }

  public static class Factory implements Storage.Factory<String>{

    @Override
    public HdfsStorage forResourceRoot(String root) throws IOException {
      return new HdfsStorage(root);
    }

  }

}
