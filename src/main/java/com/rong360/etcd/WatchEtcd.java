package com.rong360.etcd;

import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;
import com.rong360.binlogutil.GlobalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuchi
 * @date 2018/4/3
 */
public class WatchEtcd implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(WatchEtcd.class);
    private String key;

    public WatchEtcd(String key) {
        this.key = key;
    }

    @Override
    public void run() {

        Watch.Watcher watcher = EtcdClient.getInstance().getWatchClient().watch(
                ByteSequence.fromString(this.key),
                WatchOption.newBuilder().withPrefix(ByteSequence.fromString(this.key)).build()
        );
        while (true) {
            try {
                WatchResponse response = watcher.listen();
                for (WatchEvent event : response.getEvents()) {
                    logger.info("watch and update cdc config\n{} {} {}",
                            event.getEventType().toString(),
                            event.getKeyValue().getKey().toStringUtf8(),
                            event.getKeyValue().getValue().toStringUtf8());
                }
                GlobalConfig.loadCdcConf();
            } catch (Exception e) {
                logger.warn("watch error", e);
                if (watcher != null) {
                    watcher.close();
                }
            }
        }
    }
}
