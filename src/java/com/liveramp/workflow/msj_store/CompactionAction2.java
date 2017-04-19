package com.liveramp.workflow.msj_store;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.workflow_core.runner.BaseAction;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.BucketDataStoreImpl;
import com.rapleaf.cascading_ext.datastore.SeekingBucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.compaction.CompactionUtil;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.tap.bucket2.ThriftBucketScheme;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;

public class CompactionAction2<T extends Comparable, K extends Comparable> extends MultiStepAction {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionAction2.class);

  public static <T extends Comparable, K extends Comparable> BaseAction build(String checkpointToken, String tmpRoot,
                                                                              Class<T> type,
                                                                              MSJDataStore<K> store) throws IOException {

    try {

      if (CompactionUtil.canExecute(store)) {
        return new CompactionAction2<>(checkpointToken, tmpRoot, type, store);
      } else if (CompactionUtil.isBaseCreationInProgress(store)) {
        Long start = store.getStore().getBaseCreationStartTime();
        //  kinda weird but this way it's a workflow failure
        if ((System.currentTimeMillis() - start) > CompactionUtil.DEFAULT_BASE_CREATION_TIMEOUT) {
          return new FailNotify<>(checkpointToken, store);
        }
      }

      return new NoOpAction(checkpointToken);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class FailNotify<T extends Comparable, K extends Comparable> extends Action {

    private final MSJDataStore<K> store;

    public FailNotify(String checkpointToken, MSJDataStore<K> store) {
      super(checkpointToken);
      this.store = store;
    }

    @Override
    protected void execute() throws Exception {
      LOG.info("Store " + store + "locked for longer than timeout!");
      throw new RuntimeException("MSJ Store " + store + "base creation locked for > " + TimeUnit.MILLISECONDS.toHours(CompactionUtil.DEFAULT_BASE_CREATION_TIMEOUT) + " hours!");
    }
  }

  public CompactionAction2(String checkpointToken, String tmpRoot,
                            Class<T> type,
                            MSJDataStore<K> store) throws IOException, IllegalAccessException, InstantiationException {
    super(checkpointToken, tmpRoot);

    //  because of the field name
    BucketDataStore<T> temp = new BucketDataStoreImpl<>(getFS(), "temp base", getTmpRoot(), "/new_base", type, ThriftBucketScheme.getFieldName(type));
    if (store.isIndexOnWrite()) {
      temp = new SeekingBucketDataStore<>(temp, store.getExtractor());
    }

    Step compact = new Step(new CompactMSJStore<>("compact",
        getTmpRoot(),
        type,
        store,
        temp)
    );

    Step commit = new Step(new CommitCompactedBase<>("commit",
        temp,
        store),
        compact
    );

    setSubStepsFromTail(commit);
  }


  public class CommitCompactedBase<RecordType, KeyType extends Comparable> extends Action {
    private final BucketDataStore<RecordType> baseToCommit;
    private final MSJDataStore<KeyType> store;

    public CommitCompactedBase(String checkpointToken, BucketDataStore<RecordType> baseToCommit, MSJDataStore<KeyType> store) {
      super(checkpointToken);

      this.baseToCommit = baseToCommit;
      this.store = store;

      consumes(baseToCommit);
      writesTo(store);
    }

    @Override
    protected void execute() throws Exception {
      store.commitBase(baseToCommit.getPath());
    }
  }


}
