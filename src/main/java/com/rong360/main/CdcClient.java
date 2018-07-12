package com.rong360.main;

import com.coreos.jetcd.data.KeyValue;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.TraceEventListener;
import com.github.shyiko.mysql.binlog.TraceLifecycleListener;
import com.mysql.jdbc.StringUtils;
import com.rong360.binlogutil.GlobalConfig;
import com.rong360.binlogutil.RongUtil;
import com.rong360.etcd.EtcdApi;
import com.rong360.etcd.EtcdClient;
import com.rong360.etcd.GuardEtcd;
import com.rong360.etcd.WatchEtcd;
import com.rong360.model.QueueData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author zhangtao@rong360.com
 */
public class CdcClient {

    private final static Logger logger = LoggerFactory.getLogger(CdcClient.class);
    private static BinaryLogClient client;
    public static String instance = "";
    public static String cluster = "master";
    public static String pid;
    public static String ip;
    public static Long registerLeaseId;
    public static String logBaseHome = "";
    public static String uniEtcdKeyPrefix = "cdc";
    public static boolean watchAllTable = false;
    private static final List<MessageListener> messageListeners = new LinkedList<>();

    public void setInstance(String instance) {
        CdcClient.instance = instance;
    }

    public void setCluster(String cluster) {
        CdcClient.cluster = cluster;
    }

    public void setWatchAllTable(boolean watchAllTable) {
        CdcClient.watchAllTable = watchAllTable;
    }

    public void setLogBaseHome(String logBaseHome) {
        CdcClient.logBaseHome = logBaseHome;
    }

    public CdcClient(String etcdHost, String etcdUsername, String etcdPassword) {
        GlobalConfig.etcd_host = etcdHost.trim().split(",");
        GlobalConfig.etcd_username = etcdUsername.trim();
        GlobalConfig.etcd_password = etcdPassword.trim();
    }


    public CdcClient(String etcdHost, String etcdUsername, String etcdPassword, String prefix) {
        this(etcdHost, etcdUsername, etcdPassword);
        uniEtcdKeyPrefix = prefix;
    }

    public void start() {
        try {
            String baseLogHome = System.getProperty("cdc.log.base.home");
            if (StringUtils.isEmptyOrWhitespaceOnly(baseLogHome)) {
                System.setProperty("LOG_HOME", logBaseHome + instance);
            } else {
                System.setProperty("LOG_HOME", baseLogHome + "/" + instance);
            }
            final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            ctx.reconfigure();

            ip = InetAddress.getLocalHost().getHostAddress();
            String name = ManagementFactory.getRuntimeMXBean().getName();
            pid = name.split("@")[0];

            registerLeaseId = EtcdApi.register(RongUtil.getRegisterKey(), Constant.REGISTER_STATUS_OK);
            logger.info("register success: {}", registerLeaseId);
            new Thread(new GuardEtcd(registerLeaseId)).start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("revoke {}", registerLeaseId);
                try {
                    EtcdClient.getInstance().getLeaseClient().revoke(registerLeaseId).get();
                } catch (Exception e) {
                    logger.error("revoke {}", registerLeaseId, e);
                }
            }));

            EtcdApi.getLock(registerLeaseId);

            GlobalConfig.loadConfig();

            client = new BinaryLogClient(GlobalConfig.mysql_host, GlobalConfig.mysql_port, GlobalConfig.mysql_username, GlobalConfig.mysql_password);

            client.setKeepAlive(true);
            client.registerEventListener(new TraceEventListener());
            client.registerLifecycleListener(new TraceLifecycleListener());
            setServerId();

            new Thread(new RongTimer(client)).start();
            client.connect(TimeUnit.SECONDS.toMillis(GlobalConfig.connect_timedout));

            new Thread(new WatchEtcd(RongUtil.etcdPrefix() + "config/cdc")).start();
        } catch (IOException e) {
            logger.error("IOException:", e);
            System.exit(1);
        } catch (TimeoutException e) {
            logger.error("TimeoutException:", e);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Exception:", e);
            System.exit(1);
        }
    }

    private static void setServerId() throws Exception {
        List<KeyValue> list = EtcdApi.getPrefix(CdcClient.uniEtcdKeyPrefix);
        Long localServerId = client.getServerId();
        for (KeyValue keyValue : list) {
            String key = keyValue.getKey().toStringUtf8();
            if (key.contains("/" + instance + "/serverid")) {
                String[] split = key.split("/");
                Long serverId = Long.parseLong(keyValue.getValue().toStringUtf8());
                if (split[1].equals(cluster)) {
                    localServerId = serverId;
                    break;
                }
                if (localServerId >= serverId) {
                    localServerId = serverId - 1;
                }
            }
        }
        EtcdApi.set(RongUtil.etcdPrefix() + "serverid", String.valueOf(localServerId));
        logger.info("{} set server id:{}", cluster, localServerId);
        client.setServerId(localServerId);
    }

    /**
     * Register message listener. Note that multiple message listeners will be called in order they
     * where registered.
     *
     * @param messageListener message listener
     */
    public void registerMessageListener(MessageListener messageListener) {
        messageListeners.add(messageListener);
    }

    /**
     * @return registered message listeners
     */
    public static List<MessageListener> getMessageListeners() {
        return Collections.unmodifiableList(messageListeners);
    }

    public static boolean notifyMessageListeners(List<QueueData> dataList) {
        boolean result = true;
        for (MessageListener messageListener : messageListeners) {
            try {
                result = messageListener.publishBatch(dataList);
                if (!result) {
                    return result;
                }
            } catch (Exception e) {
                logger.warn(messageListener + " choked on " + dataList, e);
            }
        }
        return result;
    }

    /**
     * {@link CdcClient}'s message listener.
     */
    public interface MessageListener {

        boolean publishBatch(List<QueueData> dataList);
    }
}
