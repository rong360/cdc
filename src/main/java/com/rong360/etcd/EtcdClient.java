package com.rong360.etcd;

import com.coreos.jetcd.*;
import com.coreos.jetcd.data.ByteSequence;
import com.mysql.jdbc.StringUtils;
import com.rong360.binlogutil.GlobalConfig;
import io.grpc.PickFirstBalancerFactory;

/**
 * @author liuchi
 * 2018/4/2
 */
public class EtcdClient {
    private static volatile EtcdClient sInstance;
    private Client client;
    private ClientBuilder clientBuilder;

    private EtcdClient() {
        clientBuilder = Client.builder().
                endpoints(GlobalConfig.etcd_host).
                loadBalancerFactory(PickFirstBalancerFactory.getInstance());

        if (!StringUtils.isNullOrEmpty(GlobalConfig.etcd_username)
                && !StringUtils.isNullOrEmpty(GlobalConfig.etcd_password)) {
            clientBuilder.user(ByteSequence.fromString(GlobalConfig.etcd_username)).
                    password(ByteSequence.fromString(GlobalConfig.etcd_password));
        }
        client = clientBuilder.build();
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

    public static EtcdClient getOneInstance() {
        return new EtcdClient();
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
