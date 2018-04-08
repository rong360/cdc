package com.rong360.etcd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuchi
 * @date 2018/4/3
 */
public class GuardEtcd implements Runnable{
    private final static Logger logger = LoggerFactory.getLogger(GuardEtcd.class);

    final Integer refreshInterval =5000;
    private long leaseId;
    public GuardEtcd(long leaseId){
        this.leaseId = leaseId;
    }
    @Override
    public void run() {
        // TODO Auto-generated method stub
        while (true) {
            try {
                Thread.sleep(refreshInterval);
                EtcdApi.keepAliveOnce(this.leaseId);
                logger.info("keep lease ttl with etcd...");
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                logger.error("keep lease ttl with etcd {} ", e.getMessage());
            }
        }
    }
}
