package com.rong360.etcd;

import com.coreos.jetcd.Client;
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
 * 2018/4/3
 */
public class WatchEtcd implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(WatchEtcd.class);
    private String key;
    private ByteSequence watchKey;
    private WatchOption watchOption;

    public WatchEtcd(String key) {
        this.key = key;
        this.watchKey = ByteSequence.fromString(this.key);
        this.watchOption = WatchOption.newBuilder().withPrefix(this.watchKey).build();
    }

    @Override
    public void run() {
        while (true) {
            try (Client client = EtcdClient.getOneInstance().getEtcdClient();
                 Watch watchClient = client.getWatchClient();
                 Watch.Watcher watcher = watchClient.watch(this.watchKey, this.watchOption)) {
                while (true) {
                    WatchResponse response = watcher.listen();
                    for (WatchEvent event : response.getEvents()) {
                        logger.info("watch and update cdc config\n{} {} {}",
                                event.getEventType().toString(),
                                event.getKeyValue().getKey().toStringUtf8(),
                                event.getKeyValue().getValue().toStringUtf8());
                    }
                    try {
                        GlobalConfig.loadCdcConf();
                    } catch (Exception e) {
                        logger.error("watch reload config", e);
                    }
                }
            } catch (Exception e) {
                logger.warn("watch error", e);
            }
        }
    }
}
