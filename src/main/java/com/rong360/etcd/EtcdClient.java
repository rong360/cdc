package com.rong360.etcd;

import com.coreos.jetcd.*;
import com.coreos.jetcd.data.ByteSequence;
import com.rong360.binlogutil.GlobalConfig;
import io.grpc.PickFirstBalancerFactory;

/**
 * @author liuchi
 * @date 2018/4/2
 */
public class EtcdClient {
    private static volatile EtcdClient sInstance;
    private Client client;

    private EtcdClient() {
        client = Client.builder().
                endpoints(GlobalConfig.etcd_host).
                user(ByteSequence.fromString(GlobalConfig.etcd_username)).
                password(ByteSequence.fromString(GlobalConfig.etcd_password)).loadBalancerFactory(PickFirstBalancerFactory.getInstance()).
                build();
    }

    public static EtcdClient getInstance() {
        if (sInstance == null) {
            synchronized (EtcdClient.class) {
                if (sInstance == null) {
                    sInstance = new EtcdClient();
                }
            }
        }
        return sInstance;
    }

    public Client getEtcdClient() {
        return client;
    }

    public KV getKVClient() {
        return client.getKVClient();
    }

    public Lock getLockClient() {
        return client.getLockClient();
    }

    public Lease getLeaseClient() {
        return client.getLeaseClient();
    }

    public Watch getWatchClient() {
        return client.getWatchClient();
    }
}
