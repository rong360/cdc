package com.rong360.main;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.TraceEventListener;
import com.github.shyiko.mysql.binlog.TraceLifecycleListener;
import com.mysql.jdbc.StringUtils;
import com.rong360.binlogutil.GlobalConfig;
import com.rong360.binlogutil.RongUtil;
import com.rong360.etcd.EtcdApi;
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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 
 * @author zhangtao@rong360.com
 *
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
	private static final List<MessageListener> messageListeners = new LinkedList<>();

	public void setInstance(String instance) {
		CdcClient.instance = instance;
	}

	public void setCluster(String cluster) {
		CdcClient.cluster = cluster;
	}

	public void setLogBaseHome(String logBaseHome) {
		CdcClient.logBaseHome = logBaseHome;
	}

	public CdcClient(String etcdHost, String etcdUsername, String ectdPassword) {
		GlobalConfig.etcd_host = etcdHost.trim().split(",");
		GlobalConfig.etcd_username = etcdUsername.trim();
		GlobalConfig.etcd_password = ectdPassword.trim();
	}

	public void start() {

		try {
			String baseLogHome = System.getProperty("CDC_LOG_BASE_HOME");
			if (StringUtils.isEmptyOrWhitespaceOnly(baseLogHome)){
				System.setProperty("LOG_HOME", logBaseHome + instance);
			}else{
				System.setProperty("LOG_HOME", baseLogHome + instance);
			}
			final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
			ctx.reconfigure();

			GlobalConfig.loadConfig();

			client = new BinaryLogClient(GlobalConfig.mysql_host, GlobalConfig.mysql_port,GlobalConfig.mysql_username , GlobalConfig.mysql_password);
			client.setServerId(client.getServerId() - 1); 
			client.setKeepAlive(true);
			client.registerEventListener(new TraceEventListener());
			client.registerLifecycleListener(new TraceLifecycleListener());

			ip = InetAddress.getLocalHost().getHostName();
			String name = ManagementFactory.getRuntimeMXBean().getName();
			pid = name.split("@")[0];

			registerLeaseId = EtcdApi.register(RongUtil.getRegisterKey(), "1");
			new Thread(new GuardEtcd(registerLeaseId)).start();

            EtcdApi.getLock(registerLeaseId);

            new Thread(new RongTimer(client)).start();
			client.connect(TimeUnit.SECONDS.toMillis(GlobalConfig.connect_timedout));

			new Thread(new WatchEtcd(RongUtil.etcdPrefix() + "config/cdc")).start();
		} catch (IOException e) {
			logger.error("IOException:" + e.getMessage());
			System.exit(1);
		} catch (TimeoutException e) {
			logger.error("TimeoutException:" + e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception:" + e.getMessage());
			System.exit(1);
		}
	}

	/**
	 * Register message listener. Note that multiple message listeners will be called in order they
	 * where registered.
	 */
	public void registerMessageListener(MessageListener messageListener) {
		messageListeners.add(messageListener);
	}

	public static boolean notifyMessageListeners(List<QueueData> dataList) {
		boolean result = true;
		for (MessageListener messageListener : messageListeners) {
			try {
				result = messageListener.publishBatch(dataList);
				if (!result){
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
