package uw.mydb.mysql;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * mysql维护服务。
 * 为了减少线程数，以及尽量共享线程，在此服务中对所有主机连接池进行维护。
 * 连接池分为3种，分别为houseKeeping服务，新建连接服务，关闭连接服务。
 *
 * @author axeon
 */
public class MySqlMaintenanceService {

    /**
     * 当前启动状态.
     */
    private static final AtomicBoolean STATE = new AtomicBoolean(false);

    /**
     * House Keeping服务。
     */
    private static ScheduledThreadPoolExecutor houseKeepingExecutor;

    /**
     * 关闭链接服务。
     */
    private static ThreadPoolExecutor closeSessionExecutor;

    /**
     * 开始服务.
     */
    static boolean start() {
        if (STATE.compareAndSet(false, true)) {
            houseKeepingExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setNameFormat("mysql-house-keeping-%d").setDaemon(true).build(), new ThreadPoolExecutor.DiscardPolicy());
            houseKeepingExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            houseKeepingExecutor.setRemoveOnCancelPolicy(true);

            closeSessionExecutor = new ThreadPoolExecutor(1, 10, 20, SECONDS, new LinkedBlockingQueue<>(100), new ThreadFactoryBuilder().setNameFormat("mysql-house-keeping-%d").setDaemon(true).build(), new ThreadPoolExecutor.CallerRunsPolicy());
            return true;
        } else {
            return false;
        }
    }

    /**
     * 加入session关闭队列。
     *
     * @param runnable
     */
    static void queueCloseSession(Runnable runnable) {
        if (closeSessionExecutor != null) {
            closeSessionExecutor.submit(runnable);
        }
    }

    /**
     * 调度houseKeeping服务。
     */
    static void scheduleHouseKeeping(MySqlService.HouseKeeper houseKeeper) {
        houseKeepingExecutor.scheduleWithFixedDelay(houseKeeper, 0L, 10_000, MILLISECONDS);
    }

    /**
     * 停止服务.
     */
    static boolean stop() {
        if (STATE.compareAndSet(true, false)) {
            if (houseKeepingExecutor != null) {
                houseKeepingExecutor.shutdown();
                houseKeepingExecutor = null;
            }

            if (closeSessionExecutor != null) {
                closeSessionExecutor.shutdown();
                closeSessionExecutor = null;
            }
            return true;
        } else {
            return false;
        }
    }
}
