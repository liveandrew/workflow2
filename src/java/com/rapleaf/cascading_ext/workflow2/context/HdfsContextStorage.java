package com.rapleaf.cascading_ext.workflow2.context;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.commons.util.serialization.JavaObjectSerializationHandler;
import com.liveramp.commons.util.serialization.SerializationHandler;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.Resource;

//  TODO very proof-of-concept, should really have an in-memory cache.  could also stream directly to file
public class HdfsContextStorage extends ContextStorage {

  private final String root;
  private final org.apache.hadoop.fs.FileSystem fs;
  private final SerializationHandler handler;

  public HdfsContextStorage(String root){
    this.fs = FileSystemHelper.getFS();
    this.handler = new JavaObjectSerializationHandler();
    this.root = root;
  }

  private String getPath(Resource ref){
    return root + "/" + ref.getId();
  }

  @Override
  public <T> void set(Resource<T> ref, T value) throws IOException {

    Path path = new Path(getPath(ref));
    if(fs.exists(path)){
      TrashHelper.deleteUsingTrashIfEnabled(fs, path);
    }

    byte[] serialized = handler.serialize(value);

    Hfs hfs = new Hfs(new SequenceFile(new Fields("data")), path.toString());
    TupleEntryCollector collector = hfs.openForWrite(CascadingHelper.get().getFlowProcess());
    collector.add(new Tuple(new BytesWritable(serialized)));
    collector.close();

  }

  @Override
  public <T> T get(Resource<T> ref) throws IOException, ClassNotFoundException {

    Hfs hfs = new Hfs(new SequenceFile(new Fields("data")), getPath(ref));
    TupleEntryIterator read = hfs.openForRead(CascadingHelper.get().getFlowProcess());
    TupleEntry tup = read.next();

    return (T) handler.deserialize(Bytes.getBytes((BytesWritable)tup.getObject("data")));

  }

}
