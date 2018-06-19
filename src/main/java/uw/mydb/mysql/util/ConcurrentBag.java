package uw.mydb.mysql.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.util.SystemClock;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Thread.yield;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static uw.mydb.util.SystemClock.elapsedMillis;


/**
 * 在hikariCP连接池的版本上做了定制修改，主要针对中间件场景做了优化，否则无法使用。
 *
 * @param <T> the templated type to store in the bag
 * @author Brett Wooldridge
 * @author axeon
 */
public class ConcurrentBag<T extends ConcurrentBag.IConcurrentBagEntry> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentBag.class);

    private final CopyOnWriteArrayList<T> sharedList;

    private final ThreadLocal<List<T>> threadList;

    private final IBagStateListener listener;

    private final AtomicInteger waiters;

    private final SynchronousQueue<T> handoffQueue;

    private volatile boolean closed;

    /**
     * Construct a ConcurrentBag with the specified listener.
     *
     * @param listener the IBagStateListener to attach to this bag
     */
    public ConcurrentBag(final IBagStateListener listener) {
        this.listener = listener;

        this.handoffQueue = new SynchronousQueue<>(true);
        this.waiters = new AtomicInteger();
        this.sharedList = new CopyOnWriteArrayList<>();
        this.threadList = ThreadLocal.withInitial(() -> new FastList<>(IConcurrentBagEntry.class, 16));
    }

    /**
     * The method will borrow a BagEntry from the bag, blocking for the
     * specified timeout if none are available.
     *
     * @param timeout how long to wait before giving up, in units of unit
     * @return a borrowed instance from the bag or null if a timeout occurs
     * @throws InterruptedException if interrupted while waiting
     */
    public T borrow(long timeout) throws InterruptedException {
        T retEntry = null;
        // 先在ThreadLocal列表中获取
        final List<T> list = threadList.get();
        for (int i = list.size() - 1; i >= 0; i--) {
            final T bagEntry = list.remove(i);
            if (bagEntry != null && bagEntry.compareAndSet(IConcurrentBagEntry.STATE_NORMAL, IConcurrentBagEntry.STATE_USING)) {
                retEntry = bagEntry;
                break;
            }
        }
        if (retEntry == null) {
            // 再走公共列表
            final int waiting = waiters.incrementAndGet();
            try {
                for (T bagEntry : sharedList) {
                    if (bagEntry.compareAndSet(IConcurrentBagEntry.STATE_NORMAL, IConcurrentBagEntry.STATE_USING)) {
                        addToThreadLocal(bagEntry);
                        retEntry = bagEntry;
                        break;
                    }
                }
                if (retEntry == null) {
                    listener.addBagItem(waiting);
                    long startTime = SystemClock.now();
                    do {
                        final T bagEntry = handoffQueue.poll(timeout, MILLISECONDS);
                        if (bagEntry != null && bagEntry.compareAndSet(IConcurrentBagEntry.STATE_NORMAL, IConcurrentBagEntry.STATE_USING)) {
                            addToThreadLocal(bagEntry);
                            retEntry = bagEntry;
                            break;
                        }
                    } while (timeout >= elapsedMillis(startTime));
                }
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
            } finally {
                waiters.decrementAndGet();
            }
        }
        return retEntry;
    }

    /**
     * 加入ThreadLocal中。
     * 在mydb的架构中，获得链接和归还链接是两个线程，所以此方法应在borrow时候执行。
     *
     * @param bagEntry
     */
    private void addToThreadLocal(final T bagEntry) {
        final List<T> threadLocalList = threadList.get();
        threadLocalList.add(bagEntry);
    }


    /**
     * This method will return a borrowed object to the bag.  Objects
     * that are borrowed from the bag but never "requited" will result
     * in a memory leak.
     *
     * @param bagEntry the value to return to the bag
     * @throws NullPointerException  if value is null
     * @throws IllegalStateException if the bagEntry was not borrowed from the bag
     */
    public boolean requite(final T bagEntry) {
        if (bagEntry.compareAndSet(IConcurrentBagEntry.STATE_USING, IConcurrentBagEntry.STATE_NORMAL)) {
            for (int i = 0; waiters.get() > 0; i++) {
                if (bagEntry.getState() != IConcurrentBagEntry.STATE_NORMAL || handoffQueue.offer(bagEntry)) {
                    return true;
                } else if ((i & 0xff) == 0xff) {
                    parkNanos(MICROSECONDS.toNanos(10));
                } else {
                    yield();
                }
            }
            return true;
        } else {
            logger.warn("session requite status error!");
            return false;
        }
    }

    /**
     * Add a new object to the bag for others to borrow.
     *
     * @param bagEntry an object to add to the bag
     */
    public void add(final T bagEntry) {
        if (closed) {
            logger.info("ConcurrentBag has been closed, ignoring add()");
            throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
        }

        sharedList.add(bagEntry);
        long start = SystemClock.now();
        // spin until a thread takes it or none are waiting
        while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
            yield();
            if (SystemClock.elapsedMillis(start) > 10000) {
                logger.info("卡逼了{}。。。。", waiters.get());
                break;
            }
        }
    }

    /**
     * Remove a value from the bag.  This method should only be called
     * with objects obtained by <code>borrow(long, TimeUnit)</code> or <code>reserve(T)</code>
     *
     * @param bagEntry the value to remove
     * @return true if the entry was removed, false otherwise
     * @throws IllegalStateException if an attempt is made to remove an object
     *                               from the bag that was not borrowed or reserved first
     */
    public boolean remove(final T bagEntry) {
        if (!bagEntry.compareAndSet(IConcurrentBagEntry.STATE_RESERVED, IConcurrentBagEntry.STATE_REMOVED) && !closed) {
            logger.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
            return false;
        }

        final boolean removed = sharedList.remove(bagEntry);
        if (!removed && !closed) {
            logger.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
        }

        return removed;
    }

    /**
     * Close the bag to further adds.
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * This method provides a "snapshot" in time of the BagEntry
     * items in the bag in the specified state.  It does not "lock"
     * or reserve items in any way.  Call <code>reserve(T)</code>
     * on items in list before performing any action on them.
     *
     * @param state one of the {@link IConcurrentBagEntry} states
     * @return a possibly empty list of objects having the state specified
     */
    public List<T> values(final int state) {
        final List<T> list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
        Collections.reverse(list);
        return list;
    }

    /**
     * This method provides a "snapshot" in time of the bag items.  It
     * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
     * on items in the list, or understand the concurrency implications of
     * modifying items, before performing any action on them.
     *
     * @return a possibly empty list of (all) bag items
     */
    public List<T> values() {
        return (List<T>) sharedList.clone();
    }

    /**
     * 获得sourceList。
     *
     * @return a possibly empty list of (all) bag items
     */
    public List<T> sourceList() {
        return sharedList;
    }

    /**
     * reserve是一种标记删除方式。通过设置reserve来标记不可使用。
     *
     * @param bagEntry the item to reserve
     * @return true if the item was able to be reserved, false otherwise
     */
    public boolean reserve(int state, final T bagEntry) {
        return bagEntry.compareAndSet(state, IConcurrentBagEntry.STATE_RESERVED);
    }


    /**
     *  获得等待线程技术。
     *
     * @return the number of threads waiting for items from the bag
     */
    public int getWaitingThreadCount() {
        return waiters.get();
    }

    /**
     * 获得指定状态的对象。
     *
     * @param state the state of the items to count
     * @return a count of how many items in the bag are in the specified state
     */
    public int getCount(final int state) {
        int count = 0;
        for (IConcurrentBagEntry e : sharedList) {
            if (e.getState() == state) {
                count++;
            }
        }
        return count;
    }

    /**
     * Get the total number of items in the bag.
     *
     * @return the number of items in the bag
     */
    public int size() {
        return sharedList.size();
    }

    public void dumpState() {
        sharedList.forEach(entry -> logger.info(entry.toString()));
    }


    public interface IConcurrentBagEntry {

        /**
         * 删除状态。
         */
        int STATE_REMOVED = -2;

        /**
         * 标记删除状态
         */
        int STATE_RESERVED = -1;

        /**
         * 初始状态，此状态不可用
         */
        int STATE_INIT = 0;

        /**
         * 验证中状态，此状态不可用。
         */
        int STATE_AUTH = 1;
        /**
         * 正常状态。
         */
        int STATE_NORMAL = 2;

        /**
         * 使用中。。。
         */
        int STATE_USING = 3;

        /**
         * 比较后赋值，如果比对参数错，则不赋值。
         *
         * @param expectState
         * @param newState
         * @return
         */
        boolean compareAndSet(int expectState, int newState);

        /**
         * 获得当前状态。
         *
         * @return
         */
        int getState();

        /**
         * 设置当前状态。
         *
         * @param newState
         */
        void setState(int newState);
    }

    public interface IBagStateListener {
        void addBagItem(int waiting);
    }
}
