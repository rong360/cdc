package com.rong360.etcd;

import com.coreos.jetcd.Lock;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.options.PutOption;
import com.rong360.binlogutil.RongUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author liuchi
 * @date 2018/4/2
 */
public class EtcdApi {
    private final static Logger logger = LoggerFactory.getLogger(EtcdApi.class);
    private final static int timeOut = 1000;

    public static void set(String key, String value) {
        try {
            EtcdClient.getInstance().getKVClient().put(
                    ByteSequence.fromString(key),
                    ByteSequence.fromString(value)).get(timeOut, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("etcd set error key:{},value{},msg,{}", key, value, e.getMessage());
        }
    }

    public static String get(String key) {
        try {
            CompletableFuture<GetResponse> getFuture = EtcdClient.getInstance().getKVClient().get(
                    ByteSequence.fromString(key));
            GetResponse response = getFuture.get(timeOut, TimeUnit.MILLISECONDS);
            return response.getKvs().size() > 0 ? response.getKvs().get(0).getValue().toStringUtf8() : "";
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("etcd get error key:{},msg,{}", key, e.getMessage());
        }
        return "";
    }

    public static void getLock(long leaseId) {
        while (true) {
            try {
                logger.info("try to get distribute lock....");
                Lock lockClient = EtcdClient.getInstance().getLockClient();
                lockClient.lock(
                        ByteSequence.fromString(RongUtil.getLockKey()),
                        leaseId).get(timeOut, TimeUnit.MILLISECONDS).getKey();
                logger.info("get distribute lock suc!!");
                return;
            } catch (Exception e) {
                logger.info("get distribute lock fail..., try again");
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void keepAliveOnce(long leaseId) {
        try {
            EtcdClient.getInstance().getLeaseClient().keepAliveOnce(leaseId).get(timeOut, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("keep alive leaseId:{} error", leaseId);
        }
    }

    public static long setTtl(String key, String value, long ttl) throws Exception {
        long leaseID = EtcdClient.getInstance().getLeaseClient().grant(ttl).get().getID();
        EtcdClient.getInstance().getKVClient().put(
                ByteSequence.fromString(key),
                ByteSequence.fromString(value),
                PutOption.newBuilder().withLeaseId(leaseID).build()).get();
        return leaseID;
    }

    public static long register(String key, String value) throws Exception {
        return setTtl(key, value, 60);
    }

    public static void registerByLeaseId(String key, String value, long leaseId) {
        try {
            setTtlByLeaseId(key, value, leaseId);
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("registerByLeaseId key:{},leaseId:{}", key, leaseId);
        }
    }


    public static void setTtlByLeaseId(String key, String value, long leaseId) throws Exception {
        EtcdClient.getInstance().getKVClient().put(
                ByteSequence.fromString(key),
                ByteSequence.fromString(value),
                PutOption.newBuilder().withLeaseId(leaseId).build()).get(timeOut, TimeUnit.MILLISECONDS);
    }
}
