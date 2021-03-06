package com.rong360.main;


import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.rong360.binlogutil.GlobalConfig;
import com.rong360.binlogutil.RongUtil;
import com.rong360.database.ColumnDao;
import com.rong360.etcd.EtcdApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangtao@rong360.com
 * CDC Scheduled
 */
public class RongTimer implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(RongTimer.class);
    private BinaryLogClient client;

    final Integer refreshInterval = 5000;
    final Integer workerNum = 4;
    private boolean setLockInf = false;

    public RongTimer(BinaryLogClient client) {
        this.client = client;
    }

    @Override
    public void run() {
        try {
            EventHandler[] threadArr = new EventHandler[workerNum + 1];
            for (int i = 0; i < workerNum + 1; i++) {
                if (i < workerNum && GlobalConfig.isNeedperThread) {
                    threadArr[i] = new EventHandler("performance priority queue-" + i, client, Constant.THREAD_TYPE_PER);
                    threadArr[i].start();
                } else if (i == workerNum && GlobalConfig.isNeedseqThread) {
                    threadArr[i] = new EventHandler("seq priority queue", client, Constant.THREAD_TYPE_SEQ);
                    threadArr[i].start();
                } else {
                    threadArr[i] = new EventHandler("no need", client, Constant.THREAD_TYPE_OTH);
                }
            }

            while (true) {
                if (client.isMainThreadFinish())
                    break;
                EventHandler.dumpBinlogPos();
                ColumnDao.getSlaveStatus();
                if (!setLockInf) {
                    //Identify who takes the lock
                    setLockInf = EtcdApi.registerByLeaseId(RongUtil.getRegisterKey(), Constant.REGISTER_STATUS_RUN, CdcClient.registerLeaseId);
                }
                for (int i = 0; i < workerNum + 1; i++) {
                    boolean needRevoke = false;
                    if (i < workerNum && !threadArr[i].isAlive() && GlobalConfig.isNeedperThread) {
                        threadArr[i] = new EventHandler("performance priority queue-" + i, client, Constant.THREAD_TYPE_PER);
                        needRevoke = true;
                    }
                    if (i == workerNum && !threadArr[i].isAlive() && GlobalConfig.isNeedseqThread) {
                        threadArr[i] = new EventHandler("seq priority queue", client, Constant.THREAD_TYPE_SEQ);
                        needRevoke = true;
                    }
                    if (needRevoke) {
                        logger.info(threadArr[i].getThreadName() + " is not alive,revoke it!");
                        threadArr[i].start();
                    }
                }
                Thread.sleep(refreshInterval);
            }

        } catch (Exception e) {
            logger.warn("rong timer", e);
        }

    }

}
