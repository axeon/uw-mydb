package uw.mydb.util;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 弱精度的计时器，在超高并发下可以提高性能。
 *
 * @author axeon
 */
public class SystemClock {

    public static final SystemClock INSTANCE = new SystemClock(1L);

    /**
     * 更新时间。
     */
    private final long period;

    /**
     * 当前时间戳。
     */
    private final AtomicLong now;

    /**
     * 默认构造器。
     *
     * @param period
     */
    private SystemClock(long period) {
        this.period = period;
        this.now = new AtomicLong(System.currentTimeMillis());

        ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "System Clock");
                thread.setDaemon(true);
                return thread;
            }
        });

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                SystemClock.this.now.set(System.currentTimeMillis());
            }
        }, this.period, this.period, TimeUnit.MILLISECONDS);
    }

    public static long now() {
        return INSTANCE.now.get();
    }

    public static long elapsedMillis(long startTime) {
        return now() - startTime;
    }

    public static long elapsedMillis(final long startTime, final long endTime) {
        return endTime - startTime;
    }

    public static long plusMillis(final long time, final long millis) {
        return time + millis;
    }


}