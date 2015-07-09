package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Map;

import junit.framework.Assert;
import org.junit.Test;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;

public class TestHdfsDomainVersionStorage extends WorkflowTestCase {


  @Test
  public void testStorage(){

    String path = getTestRoot() + "/storage";
    HankDomainBuilderAction.HdfsDomainVersionStorage storage
        = new HankDomainBuilderAction.HdfsDomainVersionStorage(path, FileSystemHelper.getFileSystemForPath(path));

    Map<String, Integer> read = storage.read();
    Assert.assertEquals(0, read.size());

    storage.store("domain", 10);

    Map<String, Integer> read1 = storage.read();
    Assert.assertEquals(Integer.valueOf(10), read1.get("domain"));


  }

}
