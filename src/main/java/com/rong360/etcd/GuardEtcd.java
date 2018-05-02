package com.rong360.etcd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuchi
 * 2018/4/3
 */
public class GuardEtcd implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(GuardEtcd.class);

    final Integer refreshInterval = 5000;
    private long leaseId;

    public GuardEtcd(long leaseId) {
        this.leaseId = leaseId;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(refreshInterval);
                EtcdApi.keepAliveOnce(this.leaseId);
                logger.info("keep lease ttl with etcd...");
            } catch (Exception e) {
                logger.error("keep lease ttl with etcd error", e);
            }
        }
    }
}
