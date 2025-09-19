package ca.spottedleaf.concurrentutil.map;

import ca.spottedleaf.concurrentutil.util.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.HashUtil;
import ca.spottedleaf.concurrentutil.util.IntegerUtil;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

/**
 * Concurrent hashtable implementation supporting mapping arbitrary {@code long} values onto {@code long} values with
 * support for multiple writer and multiple reader threads.
 *
 * <h2>Happens-before relationship</h2>
 * <p>
 * As with {@link java.util.concurrent.ConcurrentMap}, there is a happens-before relationship between actions in one thread
 * prior to writing to the map and access to the results of those actions in another thread.
 * </p>
 *
 * <h2>Atomicity of functional methods</h2>
 * <p>
 * Functional methods are functions declared in this class which possibly perform a write (remove, replace, or modify)
 * to an entry in this map as a result of invoking a function on an input parameter. For example,
 * {@link #removeIf(long, LongPredicate)} is an example of functional a method. Functional methods will be performed atomically,
 * that is, the input parameter is guaranteed to only be invoked at most once per function call. The consequence of this
 * behavior however is that a critical lock for a bin entry is held, which means that if the input parameter invocation
 * makes additional calls to write into this hash table that the result is undefined and deadlock-prone.
 * </p>
 *
 * @see java.util.concurrent.ConcurrentMap
 */
public class ConcurrentLong2LongChainedHashTable implements Iterable<ConcurrentLong2LongChainedHashTable.TableEntry> {

    private static final TableEntry RESIZE_NODE = new TableEntry(0L, 0L);

    protected static final int DEFAULT_CAPACITY = 16;
    protected static final float DEFAULT_LOAD_FACTOR = 0.75f;
    protected static final int MAXIMUM_CAPACITY = Integer.MIN_VALUE >>> 1;

    protected final AtomicLong size = new AtomicLong();
    protected final float loadFactor;

    protected volatile TableEntry[] table;
    protected volatile TableEntry[] nextTable;

    protected static final int THRESHOLD_NO_RESIZE = -1;
    protected static final int THRESHOLD_RESIZING  = -2;
    protected volatile int threshold;
    protected static final VarHandle THRESHOLD_HANDLE = ConcurrentUtil.getVarHandle(ConcurrentLong2LongChainedHashTable.class, "threshold", int.class);

    protected final int getThresholdAcquire() {
        return (int)THRESHOLD_HANDLE.getAcquire(this);
    }

    protected final int getThresholdVolatile() {
        return (int)THRESHOLD_HANDLE.getVolatile(this);
    }

    protected final void setThresholdPlain(final int threshold) {
        THRESHOLD_HANDLE.set(this, threshold);
    }

    protected final void setThresholdRelease(final int threshold) {
        THRESHOLD_HANDLE.setRelease(this, threshold);
    }

    protected final void setThresholdVolatile(final int threshold) {
        THRESHOLD_HANDLE.setVolatile(this, threshold);
    }

    protected final int compareExchangeThresholdVolatile(final int expect, final int update) {
        return (int)THRESHOLD_HANDLE.compareAndExchange(this, expect, update);
    }

    public ConcurrentLong2LongChainedHashTable() {
        this(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    protected static int getTargetThreshold(final int capacity, final float loadFactor) {
        final double ret = (double)capacity * (double)loadFactor;
        if (Double.isInfinite(ret) || ret >= ((double)Integer.MAX_VALUE - 1)) {
            return THRESHOLD_NO_RESIZE;
        }

        return (int)Math.ceil(ret);
    }

    protected static int getCapacityFor(final int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Invalid capacity: " + capacity);
        }
        if (capacity >= MAXIMUM_CAPACITY) {
            return MAXIMUM_CAPACITY;
        }
        return IntegerUtil.roundCeilLog2(capacity);
    }

    protected ConcurrentLong2LongChainedHashTable(final int capacity, final float loadFactor) {
        final int tableSize = getCapacityFor(capacity);

        if (loadFactor <= 0.0 || !Float.isFinite(loadFactor)) {
            throw new IllegalArgumentException("Invalid load factor: " + loadFactor);
        }

        if (tableSize == MAXIMUM_CAPACITY) {
            this.setThresholdPlain(THRESHOLD_NO_RESIZE);
        } else {
            this.setThresholdPlain(getTargetThreshold(tableSize, loadFactor));
        }

        this.loadFactor = loadFactor;
        this.nextTable = this.table = new TableEntry[tableSize];
    }

    public static ConcurrentLong2LongChainedHashTable createWithCapacity(final int capacity) {
        return createWithCapacity(capacity, DEFAULT_LOAD_FACTOR);
    }

    public static ConcurrentLong2LongChainedHashTable createWithCapacity(final int capacity, final float loadFactor) {
        return new ConcurrentLong2LongChainedHashTable(capacity, loadFactor);
    }

    public static ConcurrentLong2LongChainedHashTable createWithExpected(final int expected) {
        return createWithExpected(expected, DEFAULT_LOAD_FACTOR);
    }

    public static ConcurrentLong2LongChainedHashTable createWithExpected(final int expected, final float loadFactor) {
        double capacity = Math.ceil((double)expected / (double)loadFactor);
        if (!Double.isFinite(capacity)) {
            throw new IllegalArgumentException("Invalid load factor");
        }
        if (capacity > (double)Integer.MAX_VALUE) {
            capacity = (double)Integer.MAX_VALUE;
        }

        return createWithCapacity((int)capacity, loadFactor);
    }

    /** must be deterministic given a key */
    protected static int getHash(final long key) {
        return (int)HashUtil.mix(key);
    }

    /**
     * Returns the load factor associated with this map.
     */
    public final float getLoadFactor() {
        return this.loadFactor;
    }

    protected static TableEntry getAtIndexAcquire(final TableEntry[] table, final int index) {
        return (TableEntry)TableEntry.TABLE_ENTRY_ARRAY_HANDLE.getVolatile(table, index);
    }

    protected static void setAtIndexRelease(final TableEntry[] table, final int index, final TableEntry value) {
        TableEntry.TABLE_ENTRY_ARRAY_HANDLE.setRelease(table, index, value);
    }

    protected static void setAtIndexVolatile(final TableEntry[] table, final int index, final TableEntry value) {
        TableEntry.TABLE_ENTRY_ARRAY_HANDLE.setVolatile(table, index, value);
    }

    protected static TableEntry compareAndExchangeAtIndexVolatile(final TableEntry[] table, final int index,
                                                                  final TableEntry expect, final TableEntry update) {
        return (TableEntry)TableEntry.TABLE_ENTRY_ARRAY_HANDLE.compareAndExchange(table, index, expect, update);
    }

    protected TableEntry[] fetchNewTable(final TableEntry[] expectedCurr) {
        final TableEntry[] candidate = this.nextTable;
        final TableEntry[] current = this.table;
        // Note: We fetch a new table once RESIZE_NODE is encountered in the expectedCurr table.
        //       The resize logic guarantees that the RESIZE_NODE is only written to a bin once
        //       the chain is fully moved to the next table. Provided that we actually fetch the next table,
        //       we can guarantee that we will see the chain in the next table. However, we may not end up fetching
        //       the next table, but rather the resize after that - which will not guarantee that we see the correct
        //       chain. We catch this race condition by checking if the current table is the same as the expected table.
        //       If the current table is not the expected table, then we just use that - as it is guaranteed to either
        //       contain the full chain or be referenced to the next table.
        // Note: the read order of next and current table is important, do not re-order.
        return expectedCurr == current ? candidate : current;
    }

    /**
     * Returns the possible node associated with the key, or {@code null} if there is no such node. The node
     * returned may have a {@code null} {@link TableEntry#value}, in which case the node is a placeholder for
     * a compute/computeIfAbsent call. The placeholder node should not be considered mapped in order to preserve
     * happens-before relationships between writes and reads in the map.
     */
    protected final TableEntry getNode(final long key) {
        final int hash = getHash(key);

        TableEntry[] table = this.table;
        for (;;) {
            TableEntry node = getAtIndexAcquire(table, hash & (table.length - 1));

            if (node == null) {
                // node == null
                return node;
            }

            if (node == RESIZE_NODE) {
                table = this.fetchNewTable(table);
                continue;
            }

            for (; node != null; node = node.getNextVolatile()) {
                if (node.key == key) {
                    return node;
                }
            }

            // node == null
            return node;
        }
    }


    /**
     * Returns the currently mapped value associated with the specified key, or {@code 0L} if there is none.
     *
     * @param key Specified key
     */
    public long get(final long key) {
        final TableEntry node = this.getNode(key);
        return node == null ? 0L : node.getValueVolatile();
    }

    /**
     * Returns the currently mapped value associated with the specified key, or the specified default value if there is none.
     *
     * @param key Specified key
     * @param defaultValue Specified default value
     */
    public long getOrDefault(final long key, final long defaultValue) {
        final TableEntry node = this.getNode(key);
        return node == null ? defaultValue : node.getValueVolatile();
    }

    /**
     * Returns whether the specified key is mapped to some value.
     * @param key Specified key
     */
    public boolean containsKey(final long key) {
        return this.getNode(key) != null;
    }


    /**
     * Returns whether the specified value has a key mapped to it.
     * @param value Specified value
     */
    public boolean containsValue(final long value) {
        final NodeIterator iterator = new NodeIterator(this);

        TableEntry node;
        while ((node = iterator.findNext()) != null) {
            // need to use acquire here to ensure the happens-before relationship
            if (node.getValueAcquire() == value) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns the number of mappings in this map.
     */
    public int size() {
        final long ret = this.size.get();

        if (ret < 0L) {
            return 0;
        }
        if (ret > (long)Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return (int)ret;
    }

    /**
     * Returns whether this map has no mappings.
     */
    public boolean isEmpty() {
        return this.size.get() <= 0L;
    }

    /**
     * Adds count to size and checks threshold for resizing
     */
    protected final void addSize(final long count) {
        final long sum = this.size.addAndGet(count);

        final int threshold = this.getThresholdAcquire();

        if (threshold < 0L) {
            // resizing or no resizing allowed, in either cases we do not need to do anything
            return;
        }

        if (sum < (long)threshold) {
            return;
        }

        if (threshold != this.compareExchangeThresholdVolatile(threshold, THRESHOLD_RESIZING)) {
            // some other thread resized
            return;
        }

        // create new table
        this.resize(sum);
    }

    /**
     * Resizes table, only invoke for the thread which has successfully updated threshold to {@link #THRESHOLD_RESIZING}
     * @param sum Estimate of current mapping count, must be >= old threshold
     */
    private void resize(final long sum) {
        int capacity;

        // add 1.0, as sum may equal threshold (in which case, sum / loadFactor = current capacity)
        // adding 1.0 should at least raise the size by a factor of two due to usage of roundCeilLog2
        final double targetD = ((double)sum / (double)this.loadFactor) + 1.0;
        if (targetD >= (double)MAXIMUM_CAPACITY) {
            capacity = MAXIMUM_CAPACITY;
        } else {
            capacity = (int)Math.ceil(targetD);
            capacity = IntegerUtil.roundCeilLog2(capacity);
            capacity = Math.min(capacity, MAXIMUM_CAPACITY);
        }

        // create new table data

        final TableEntry[] newTable = new TableEntry[capacity];

        // transfer nodes from old table

        // does not need to be volatile read, just plain
        final TableEntry[] oldTable = this.table;
        this.nextTable = newTable;

        // when resizing, the old entries at bin i (where i = hash % oldTable.length) are assigned to
        // bin k in the new table (where k = hash % newTable.length)
        // since both table lengths are powers of two (specifically, newTable is a multiple of oldTable),
        // the possible number of locations in the new table to assign any given i is newTable.length/oldTable.length

        // we can build the new linked nodes for the new table by using a work array sized to newTable.length/oldTable.length
        // which holds the _last_ entry in the chain per bin

        final int capOldShift = IntegerUtil.floorLog2(oldTable.length);
        final int capDiffShift = IntegerUtil.floorLog2(capacity) - capOldShift;

        if (capDiffShift == 0) {
            throw new IllegalStateException("Resizing to same size");
        }

        final TableEntry[] work = new TableEntry[1 << capDiffShift]; // typically, capDiffShift = 1

        for (int i = 0, len = oldTable.length; i < len; ++i) {
            TableEntry binNode = getAtIndexAcquire(oldTable, i);

            for (;;) {
                if (binNode == null) {
                    // just need to replace the bin node, do not need to move anything
                    if (null == (binNode = compareAndExchangeAtIndexVolatile(oldTable, i, null, RESIZE_NODE))) {
                        break;
                    } // else: binNode != null, fall through
                }

                // need write lock to block other writers
                synchronized (binNode) {
                    if (binNode != (binNode = getAtIndexAcquire(oldTable, i))) {
                        continue;
                    }

                    // an important detail of resizing is that we do not need to be concerned with synchronisation on
                    // writes to the new table, as no access to any nodes on bin i on oldTable will occur until a thread
                    // sees the resizeNode
                    // specifically, as long as the resizeNode is release written there are no cases where another thread
                    // will see our writes to the new table

                    TableEntry next = binNode.getNextPlain();

                    if (next == null) {
                        // simple case: do not use work array

                        // do not need to create new node, readers only need to see the state of the map at the
                        // beginning of a call, so any additions onto _next_ don't really matter
                        // additionally, the old node is replaced so that writers automatically forward to the new table,
                        // which resolves any issues
                        newTable[getHash(binNode.key) & (capacity - 1)] = binNode;
                    } else {
                        // reset for next usage
                        Arrays.fill(work, null);

                        for (TableEntry curr = binNode; curr != null; curr = curr.getNextPlain()) {
                            final int newTableIdx = getHash(curr.key) & (capacity - 1);
                            final int workIdx = newTableIdx >>> capOldShift;

                            final TableEntry replace = new TableEntry(curr.key, curr.getValuePlain());

                            final TableEntry workNode = work[workIdx];
                            work[workIdx] = replace;

                            if (workNode == null) {
                                newTable[newTableIdx] = replace;
                                continue;
                            } else {
                                workNode.setNextPlain(replace);
                                continue;
                            }
                        }
                    }

                    setAtIndexRelease(oldTable, i, RESIZE_NODE);
                    break;
                }
            }
        }

        // calculate new threshold
        final int newThreshold;
        if (capacity == MAXIMUM_CAPACITY) {
            newThreshold = THRESHOLD_NO_RESIZE;
        } else {
            newThreshold = getTargetThreshold(capacity, loadFactor);
        }

        this.table = newTable;
        // finish resize operation by releasing hold on threshold
        this.setThresholdVolatile(newThreshold);
    }

    /**
     * Subtracts count from size
     */
    protected final void subSize(final long count) {
        this.size.getAndAdd(-count);
    }

    /**
     * Atomically updates the value associated with {@code key} to {@code value}, or inserts a new mapping with {@code key}
     * mapped to {@code value}.
     * @param key Specified key
     * @param value Specified value
     * @return Old value previously associated with key, or {@code 0L} if none.
     */
    public long put(final long key, final long value) {
        final int hash = getHash(key);

        TableEntry[] table = this.table;
        table_loop:
        for (;;) {
            final int index = hash & (table.length - 1);

            TableEntry node = getAtIndexAcquire(table, index);
            node_loop:
            for (;;) {
                if (node == null) {
                    if (null == (node = compareAndExchangeAtIndexVolatile(table, index, null, new TableEntry(key, value)))) {
                        // successfully inserted
                        this.addSize(1L);
                        return 0L;
                    } // else: node != null, fall through
                }

                if (node == RESIZE_NODE) {
                    table = this.fetchNewTable(table);
                    continue table_loop;
                }

                synchronized (node) {
                    if (node != (node = getAtIndexAcquire(table, index))) {
                        continue node_loop;
                    }
                    // plain reads are fine during synchronised access, as we are the only writer
                    TableEntry prev = null;
                    for (; node != null; prev = node, node = node.getNextPlain()) {
                        if (node.key == key) {
                            final long ret = node.getValuePlain();
                            node.setValueVolatile(value);
                            return ret;
                        }
                    }

                    // volatile ordering ensured by addSize(), but we need release here
                    // to ensure proper ordering with reads and other writes
                    prev.setNextRelease(new TableEntry(key, value));
                }

                this.addSize(1L);
                return 0L;
            }
        }
    }

    /**
     * Atomically inserts a new mapping with {@code key} mapped to {@code value} if and only if {@code key} is not
     * currently mapped to some value.
     * @param key Specified key
     * @param value Specified value
     * @return Value currently associated with key, or {@code 0L} if none and {@code value} was associated.
     */
    public long putIfAbsent(final long key, final long value) {
        final int hash = getHash(key);

        TableEntry[] table = this.table;
        table_loop:
        for (;;) {
            final int index = hash & (table.length - 1);

            TableEntry node = getAtIndexAcquire(table, index);
            node_loop:
            for (;;) {
                if (node == null) {
                    if (null == (node = compareAndExchangeAtIndexVolatile(table, index, null, new TableEntry(key, value)))) {
                        // successfully inserted
                        this.addSize(1L);
                        return 0L;
                    } // else: node != null, fall through
                }

                if (node == RESIZE_NODE) {
                    table = this.fetchNewTable(table);
                    continue table_loop;
                }

                // optimise ifAbsent calls: check if first node is key before attempting lock acquire
                if (node.key == key) {
                    return node.getValueVolatile();
                }

                synchronized (node) {
                    if (node != (node = getAtIndexAcquire(table, index))) {
                        continue node_loop;
                    }
                    // plain reads are fine during synchronised access, as we are the only writer
                    TableEntry prev = null;
                    for (; node != null; prev = node, node = node.getNextPlain()) {
                        if (node.key == key) {
                            return node.getValuePlain();
                        }
                    }

                    // volatile ordering ensured by addSize(), but we need release here
                    // to ensure proper ordering with reads and other writes
                    prev.setNextRelease(new TableEntry(key, value));
                }

                this.addSize(1L);
                return 0L;
            }
        }
    }

    /**
     * Atomically updates the value associated with {@code key} to {@code value}, or does nothing if {@code key} is not
     * associated with a value.
     * @param key Specified key
     * @param value Specified value
     * @return Old value previously associated with key, or {@code 0L} if none.
     */
    public long replace(final long key, final long value) {
        final int hash = getHash(key);

        TableEntry[] table = this.table;
        table_loop:
        for (;;) {
            final int index = hash & (table.length - 1);

            TableEntry node = getAtIndexAcquire(table, index);
            node_loop:
            for (;;) {
                if (node == null) {
                    return 0L;
                }

                if (node == RESIZE_NODE) {
                    table = this.fetchNewTable(table);
                    continue table_loop;
                }

                synchronized (node) {
                    if (node != (node = getAtIndexAcquire(table, index))) {
                        continue node_loop;
                    }

                    // plain reads are fine during synchronised access, as we are the only writer
                    for (; node != null; node = node.getNextPlain()) {
                        if (node.key == key) {
                            final long ret = node.getValuePlain();
                            node.setValueVolatile(value);
                            return ret;
                        }
                    }
                }

                return 0L;
            }
        }
    }

    /**
     * Atomically updates the value associated with {@code key} to {@code update} if the currently associated
     * value is reference equal to {@code expect}, otherwise does nothing.
     * @param key Specified key
     * @param expect Expected value to check current mapped value with
     * @param update Update value to replace mapped value with
     * @return If the currently mapped value is not reference equal to {@code expect}, then returns the currently mapped
     *         value. If the key is not mapped to any value, then returns {@code 0L}. If neither of the two cases are
     *         true, then returns {@code expect}.
     */
    public long replace(final long key, final long expect, final long update) {
        final int hash = getHash(key);

        TableEntry[] table = this.table;
        table_loop:
        for (;;) {
            final int index = hash & (table.length - 1);

            TableEntry node = getAtIndexAcquire(table, index);
            node_loop:
            for (;;) {
                if (node == null) {
                    return 0L;
                }

                if (node == RESIZE_NODE) {
                    table = this.fetchNewTable(table);
                    continue table_loop;
                }

                synchronized (node) {
                    if (node != (node = getAtIndexAcquire(table, index))) {
                        continue node_loop;
                    }

                    // plain reads are fine during synchronised access, as we are the only writer
                    for (; node != null; node = node.getNextPlain()) {
                        if (node.key == key) {
                            final long ret = node.getValuePlain();

                            if (ret != expect) {
                                return ret;
                            }

                            node.setValueVolatile(update);
                            return ret;
                        }
                    }
                }

                return 0L;
            }
        }
    }

    /**
     * Atomically removes the mapping for the specified key and returns the value it was associated with. If the key
     * is not mapped to a value, then does nothing and returns {@code 0L}.
     * @param key Specified key
     * @return Old value previously associated with key, or {@code 0L} if none.
     */
    public long remove(final long key) {
        final int hash = getHash(key);

        TableEntry[] table = this.table;
        table_loop:
        for (;;) {
            final int index = hash & (table.length - 1);

            TableEntry node = getAtIndexAcquire(table, index);
            node_loop:
            for (;;) {
                if (node == null) {
                    return 0L;
                }

                if (node == RESIZE_NODE) {
                    table = this.fetchNewTable(table);
                    continue table_loop;
                }

                boolean removed = false;
                long ret = 0L;

                synchronized (node) {
                    if (node != (node = getAtIndexAcquire(table, index))) {
                        continue node_loop;
                    }

                    TableEntry prev = null;

                    // plain reads are fine during synchronised access, as we are the only writer
                    for (; node != null; prev = node, node = node.getNextPlain()) {
                        if (node.key == key) {
                            ret = node.getValuePlain();
                            removed = true;

                            // volatile ordering ensured by addSize(), but we need release here
                            // to ensure proper ordering with reads and other writes
                            if (prev == null) {
                                setAtIndexRelease(table, index, node.getNextPlain());
                            } else {
                                prev.setNextRelease(node.getNextPlain());
                            }

                            break;
                        }
                    }
                }

                if (removed) {
                    this.subSize(1L);
                }

                return ret;
            }
        }
    }

    /**
     * Atomically removes the mapping for the specified key if it is mapped to {@code expect} and returns {@code expect}. If the key
     * is not mapped to a value, then does nothing and returns {@code 0L}. If the key is mapped to a value that is not
     * equal to {@code expect}, then returns that value.
     * @param key Specified key
     * @param expect Specified expected value
     * @return The specified expected value if the key was mapped to {@code expect}. If
     *         the key is not mapped to any value, then returns {@code 0L}. If neither of those cases are true,
     *         then returns the current (possibly zero) mapped value for key.
     */
    public long remove(final long key, final long expect) {
        final int hash = getHash(key);

        TableEntry[] table = this.table;
        table_loop:
        for (;;) {
            final int index = hash & (table.length - 1);

            TableEntry node = getAtIndexAcquire(table, index);
            node_loop:
            for (;;) {
                if (node == null) {
                    return 0L;
                }

                if (node == RESIZE_NODE) {
                    table = this.fetchNewTable(table);
                    continue table_loop;
                }

                boolean removed = false;
                long ret = 0L;

                synchronized (node) {
                    if (node != (node = getAtIndexAcquire(table, index))) {
                        continue node_loop;
                    }

                    TableEntry prev = null;

                    // plain reads are fine during synchronised access, as we are the only writer
                    for (; node != null; prev = node, node = node.getNextPlain()) {
                        if (node.key == key) {
                            ret = node.getValuePlain();
                            if (ret == expect) {
                                removed = true;

                                // volatile ordering ensured by addSize(), but we need release here
                                // to ensure proper ordering with reads and other writes
                                if (prev == null) {
                                    setAtIndexRelease(table, index, node.getNextPlain());
                                } else {
                                    prev.setNextRelease(node.getNextPlain());
                                }
                            }
                            break;
                        }
                    }
                }

                if (removed) {
                    this.subSize(1L);
                }

                return ret;
            }
        }
    }

    /**
     * Atomically removes the mapping for the specified key the predicate returns true for its currently mapped value. If the key
     * is not mapped to a value, then does nothing and returns {@code 0L}.
     *
     * <p>
     * This function is a "functional methods" as defined by {@link ConcurrentLong2LongChainedHashTable}.
     * </p>
     *
     * @param key Specified key
     * @param predicate Specified predicate
     * @throws NullPointerException If predicate is null
     * @return The specified expected value if the key was mapped to {@code expect}. If
     *         the key is not mapped to any value, then returns {@code 0L}. If neither of those cases are true,
     *         then returns the current (possibly zero) mapped value for key.
     */
    public long removeIf(final long key, final LongPredicate predicate) {
        Objects.requireNonNull(predicate, "Predicate may not be null");

        final int hash = getHash(key);

        TableEntry[] table = this.table;
        table_loop:
        for (;;) {
            final int index = hash & (table.length - 1);

            TableEntry node = getAtIndexAcquire(table, index);
            node_loop:
            for (;;) {
                if (node == null) {
                    return 0L;
                }

                if (node == RESIZE_NODE) {
                    table = this.fetchNewTable(table);
                    continue table_loop;
                }

                boolean removed = false;
                long ret = 0L;

                synchronized (node) {
                    if (node != (node = getAtIndexAcquire(table, index))) {
                        continue node_loop;
                    }

                    TableEntry prev = null;

                    // plain reads are fine during synchronised access, as we are the only writer
                    for (; node != null; prev = node, node = node.getNextPlain()) {
                        if (node.key == key) {
                            ret = node.getValuePlain();
                            if (predicate.test(ret)) {
                                removed = true;

                                // volatile ordering ensured by addSize(), but we need release here
                                // to ensure proper ordering with reads and other writes
                                if (prev == null) {
                                    setAtIndexRelease(table, index, node.getNextPlain());
                                } else {
                                    prev.setNextRelease(node.getNextPlain());
                                }
                            }
                            break;
                        }
                    }
                }

                if (removed) {
                    this.subSize(1L);
                }

                return ret;
            }
        }
    }

    /**
     * Atomically inserts a mapping for the specified key with the provided default value if the key is not mapped or
     * updates the current value by adding the specified increment.
     * @param key Specified key
     * @param increment Specified increment
     * @param defaultValue Specified default value
     * @return The newly mapped value
     */
    public long addTo(final long key, final long increment, final long defaultValue) {
        final int hash = getHash(key);

        TableEntry[] table = this.table;
        table_loop:
        for (;;) {
            final int index = hash & (table.length - 1);

            TableEntry node = getAtIndexAcquire(table, index);
            node_loop:
            for (;;) {
                if (node == null) {
                    if (null == (node = compareAndExchangeAtIndexVolatile(table, index, null, new TableEntry(key, defaultValue)))) {
                        // successfully inserted
                        this.addSize(1L);
                        return defaultValue;
                    } // else: node != null, fall through
                }

                if (node == RESIZE_NODE) {
                    table = this.fetchNewTable(table);
                    continue table_loop;
                }

                synchronized (node) {
                    if (node != (node = getAtIndexAcquire(table, index))) {
                        continue node_loop;
                    }
                    // plain reads are fine during synchronised access, as we are the only writer
                    TableEntry prev = null;
                    for (; node != null; prev = node, node = node.getNextPlain()) {
                        if (node.key == key) {
                            final long ret = node.getValuePlain() + increment;
                            node.setValueVolatile(ret);
                            return ret;
                        }
                    }

                    // volatile ordering ensured by addSize(), but we need release here
                    // to ensure proper ordering with reads and other writes
                    prev.setNextRelease(new TableEntry(key, defaultValue));
                }

                this.addSize(1L);
                return defaultValue;
            }
        }
    }

    /**
     * Atomically decrements the mapping by the specified decrement and then removes the mapping if it is below or equal to
     * the specified threshold. If the key is not mapped, then an exception is thrown.
     * @param key Specified key
     * @param decrement Specified decrement
     * @param threshold Specified threshold
     * @throws IllegalStateException If there exists no mapping for the key
     * @return The previous value decremented by the specified decrement
     */
    public long decFrom(final long key, final long decrement, final long threshold) {
        final int hash = getHash(key);

        TableEntry[] table = this.table;
        table_loop:
        for (;;) {
            final int index = hash & (table.length - 1);

            TableEntry node = getAtIndexAcquire(table, index);
            node_loop:
            for (;;) {
                if (node == null) {
                    throw new IllegalStateException();
                }

                if (node == RESIZE_NODE) {
                    table = this.fetchNewTable(table);
                    continue table_loop;
                }

                boolean removed = false;
                boolean found = false;
                long ret = 0L;

                synchronized (node) {
                    if (node != (node = getAtIndexAcquire(table, index))) {
                        continue node_loop;
                    }

                    TableEntry prev = null;

                    // plain reads are fine during synchronised access, as we are the only writer
                    for (; node != null; prev = node, node = node.getNextPlain()) {
                        if (node.key == key) {
                            found = true;
                            ret = node.getValuePlain() - decrement;

                            if (ret <= threshold) {
                                removed = true;

                                // volatile ordering ensured by addSize(), but we need release here
                                // to ensure proper ordering with reads and other writes
                                if (prev == null) {
                                    setAtIndexRelease(table, index, node.getNextPlain());
                                } else {
                                    prev.setNextRelease(node.getNextPlain());
                                }
                            } else {
                                // note: cannot set if removing, otherwise the operation is not atomic!
                                node.setValueRelease(ret);
                            }
                            break;
                        }
                    }
                }

                if (!found) {
                    throw new IllegalStateException();
                }

                if (removed) {
                    this.subSize(1L);
                }

                return ret;
            }
        }
    }

    /**
     * Removes at least all entries currently mapped at the beginning of this call. May not remove entries added during
     * this call. As a result, only if this map is not modified during the call, that all entries will be removed by
     * the end of the call.
     *
     * <p>
     * This function is not atomic.
     * </p>
     */
    public void clear() {
        // it is possible to optimise this to directly interact with the table,
        // but we do need to be careful when interacting with resized tables,
        // and the NodeIterator already does this logic
        final NodeIterator nodeIterator = new NodeIterator(this);

        TableEntry node;
        while ((node = nodeIterator.findNext()) != null) {
            this.remove(node.key);
        }
    }

    /**
     * Returns an iterator over the entries in this map. The iterator is only guaranteed to see entries that were
     * added before the beginning of this call, but it may see entries added during.
     */
    public Iterator<TableEntry> entryIterator() {
        return new EntryIterator(this);
    }

    @Override
    public final Iterator<TableEntry> iterator() {
        return this.entryIterator();
    }

    /**
     * Returns an iterator over the keys in this map. The iterator is only guaranteed to see keys that were
     * added before the beginning of this call, but it may see keys added during.
     */
    public PrimitiveIterator.OfLong keyIterator() {
        return new KeyIterator(this);
    }

    /**
     * Returns an iterator over the values in this map. The iterator is only guaranteed to see values that were
     * added before the beginning of this call, but it may see values added during.
     */
    public PrimitiveIterator.OfLong valueIterator() {
        return new ValueIterator(this);
    }

    protected static final class EntryIterator extends BaseIteratorImpl<TableEntry> {

        public EntryIterator(final ConcurrentLong2LongChainedHashTable map) {
            super(map);
        }

        @Override
        public TableEntry next() throws NoSuchElementException {
            return this.nextNode();
        }

        @Override
        public void forEachRemaining(final Consumer<? super TableEntry> action) {
            Objects.requireNonNull(action, "Action may not be null");
            while (this.hasNext()) {
                action.accept(this.next());
            }
        }
    }

    protected static final class KeyIterator extends BaseLongIteratorImpl {

        public KeyIterator(final ConcurrentLong2LongChainedHashTable map) {
            super(map);
        }

        @Override
        public long nextLong() {
            return this.nextNode().key;
        }

        @Override
        public void forEachRemaining(final LongConsumer action) {
            Objects.requireNonNull(action, "Action may not be null");
            while (this.hasNext()) {
                action.accept(this.nextLong());
            }
        }
    }

    protected static final class ValueIterator extends BaseLongIteratorImpl {

        public ValueIterator(final ConcurrentLong2LongChainedHashTable map) {
            super(map);
        }

        @Override
        public long nextLong() throws NoSuchElementException {
            return this.nextNode().getValueVolatile();
        }

        @Override
        public void forEachRemaining(final LongConsumer action) {
            Objects.requireNonNull(action, "Action may not be null");
            while (this.hasNext()) {
                action.accept(this.next());
            }
        }
    }

    protected static abstract class BaseLongIteratorImpl extends BaseIteratorImpl<Long> implements PrimitiveIterator.OfLong {

        protected BaseLongIteratorImpl(ConcurrentLong2LongChainedHashTable map) {
            super(map);
        }

        @Override
        public final Long next() {
            return Long.valueOf(this.nextLong());
        }

        @Override
        public final void forEachRemaining(final Consumer<? super Long> action) {
            Objects.requireNonNull(action, "Action may not be null");

            if (action instanceof LongConsumer longConsumer) {
                this.forEachRemaining(longConsumer);
                return;
            }

            while (this.hasNext()) {
                action.accept(this.next());
            }
        }

        @Override
        public abstract long nextLong() throws NoSuchElementException;

        // overwritten by subclasses to avoid indirection on hasNext() and next()
        @Override
        public abstract void forEachRemaining(final LongConsumer action);
    }

    protected static abstract class BaseIteratorImpl<T> extends NodeIterator implements Iterator<T> {

        protected final ConcurrentLong2LongChainedHashTable map;
        protected TableEntry lastReturned;
        protected TableEntry nextToReturn;

        protected BaseIteratorImpl(final ConcurrentLong2LongChainedHashTable map) {
            super(map);
            this.map = map;
        }

        @Override
        public final boolean hasNext() {
            if (this.nextToReturn != null) {
                return true;
            }

            return (this.nextToReturn = this.findNext()) != null;
        }

        protected final TableEntry nextNode() throws NoSuchElementException {
            TableEntry ret = this.nextToReturn;
            if (ret != null) {
                this.lastReturned = ret;
                this.nextToReturn = null;
                return ret;
            }
            ret = this.findNext();
            if (ret != null) {
                this.lastReturned = ret;
                return ret;
            }
            throw new NoSuchElementException();
        }

        @Override
        public final void remove() {
            final TableEntry lastReturned = this.lastReturned;
            if (lastReturned == null) {
                throw new NoSuchElementException();
            }
            this.lastReturned = null;
            this.map.remove(lastReturned.key);
        }

        @Override
        public abstract T next() throws NoSuchElementException;

        // overwritten by subclasses to avoid indirection on hasNext() and next()
        @Override
        public abstract void forEachRemaining(final Consumer<? super T> action);
    }

    protected static class NodeIterator {

        protected ConcurrentLong2LongChainedHashTable map;
        protected TableEntry[] currentTable;
        protected ResizeChain resizeChain;
        protected TableEntry last;
        protected int nextBin;
        protected int increment;

        protected NodeIterator(final ConcurrentLong2LongChainedHashTable map) {
            this.map = map;
            this.currentTable = map.table;
            this.increment = 1;
        }

        private TableEntry[] pullResizeChain(final int index) {
            final ResizeChain resizeChain = this.resizeChain;
            if (resizeChain == null) {
                this.currentTable = null;
                return null;
            }

            final ResizeChain prevChain = resizeChain.prev;
            this.resizeChain = prevChain;
            if (prevChain == null) {
                this.currentTable = null;
                return null;
            }

            final TableEntry[] newTable = prevChain.table;

            // we recover the original index by modding by the new table length, as the increments applied to the index
            // are a multiple of the new table's length
            int newIdx = index & (newTable.length - 1);

            // the increment is always the previous table's length
            final ResizeChain nextPrevChain = prevChain.prev;
            final int increment;
            if (nextPrevChain == null) {
                increment = 1;
            } else {
                increment = nextPrevChain.table.length;
            }

            // done with the upper table, so we can skip the resize node
            newIdx += increment;

            this.increment = increment;
            this.nextBin = newIdx;
            this.currentTable = newTable;

            return newTable;
        }

        private TableEntry[] pushResizeChain(final TableEntry[] table) {
            final ResizeChain chain = this.resizeChain;

            if (chain == null) {
                final TableEntry[] nextTable = this.map.fetchNewTable(table);

                final ResizeChain oldChain = new ResizeChain(table, null, null);
                final ResizeChain currChain = new ResizeChain(nextTable, oldChain, null);
                oldChain.next = currChain;

                this.increment = table.length;
                this.resizeChain = currChain;
                this.currentTable = nextTable;

                return nextTable;
            } else {
                ResizeChain currChain = chain.next;
                if (currChain == null) {
                    final TableEntry[] ret = this.map.fetchNewTable(table);
                    currChain = new ResizeChain(ret, chain, null);
                    chain.next = currChain;

                    this.increment = table.length;
                    this.resizeChain = currChain;
                    this.currentTable = ret;

                    return ret;
                } else {
                    this.increment = table.length;
                    this.resizeChain = currChain;
                    return this.currentTable = currChain.table;
                }
            }
        }

        protected final TableEntry findNext() {
            for (;;) {
                final TableEntry last = this.last;
                if (last != null) {
                    final TableEntry next = last.getNextVolatile();
                    if (next != null) {
                        this.last = next;
                        return next;
                    }
                }

                TableEntry[] table = this.currentTable;

                if (table == null) {
                    return null;
                }

                int idx = this.nextBin;
                int increment = this.increment;
                for (;;) {
                    if (idx >= table.length) {
                        table = this.pullResizeChain(idx);
                        idx = this.nextBin;
                        increment = this.increment;
                        if (table != null) {
                            continue;
                        } else {
                            this.last = null;
                            return null;
                        }
                    }

                    final TableEntry entry = getAtIndexAcquire(table, idx);
                    if (entry == null) {
                        idx += increment;
                        continue;
                    }

                    if (entry == RESIZE_NODE) {
                        // push onto resize chain
                        table = this.pushResizeChain(table);
                        increment = this.increment;
                        continue;
                    }

                    this.last = entry;
                    this.nextBin = idx + increment;
                    return entry;
                }
            }
        }

        protected static final class ResizeChain {

            public final TableEntry[] table;
            public final ResizeChain prev;
            public ResizeChain next;

            public ResizeChain(final TableEntry[] table, final ResizeChain prev, final ResizeChain next) {
                this.table = table;
                this.prev = prev;
                this.next = next;
            }
        }
    }

    public static final class TableEntry {

        private static final VarHandle TABLE_ENTRY_ARRAY_HANDLE = ConcurrentUtil.getArrayHandle(ConcurrentLong2LongChainedHashTable.TableEntry[].class);

        private final long key;

        private volatile long value;
        private static final VarHandle VALUE_HANDLE = ConcurrentUtil.getVarHandle(ConcurrentLong2LongChainedHashTable.TableEntry.class, "value", long.class);

        private long getValuePlain() {
            return (long)VALUE_HANDLE.get(this);
        }

        private long getValueAcquire() {
            return (long)VALUE_HANDLE.getAcquire(this);
        }

        private long getValueVolatile() {
            return (long)VALUE_HANDLE.getVolatile(this);
        }

        private void setValuePlain(final long value) {
            VALUE_HANDLE.set(this, value);
        }

        private void setValueRelease(final long value) {
            VALUE_HANDLE.setRelease(this, value);
        }

        private void setValueVolatile(final long value) {
            VALUE_HANDLE.setVolatile(this, value);
        }

        private volatile ConcurrentLong2LongChainedHashTable.TableEntry next;
        private static final VarHandle NEXT_HANDLE = ConcurrentUtil.getVarHandle(ConcurrentLong2LongChainedHashTable.TableEntry.class, "next", ConcurrentLong2LongChainedHashTable.TableEntry.class);

        private ConcurrentLong2LongChainedHashTable.TableEntry getNextPlain() {
            return (ConcurrentLong2LongChainedHashTable.TableEntry)NEXT_HANDLE.get(this);
        }

        private ConcurrentLong2LongChainedHashTable.TableEntry getNextVolatile() {
            return (ConcurrentLong2LongChainedHashTable.TableEntry)NEXT_HANDLE.getVolatile(this);
        }

        private void setNextPlain(final ConcurrentLong2LongChainedHashTable.TableEntry next) {
            NEXT_HANDLE.set(this, next);
        }

        private void setNextRelease(final ConcurrentLong2LongChainedHashTable.TableEntry next) {
            NEXT_HANDLE.setRelease(this, next);
        }

        private void setNextVolatile(final ConcurrentLong2LongChainedHashTable.TableEntry next) {
            NEXT_HANDLE.setVolatile(this, next);
        }

        public TableEntry(final long key, final long value) {
            this.key = key;
            this.setValuePlain(value);
        }

        public long getKey() {
            return this.key;
        }

        public long getValue() {
            return this.getValueVolatile();
        }
    }
}
