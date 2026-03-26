package ca.spottedleaf.concurrentutil.numa;

import com.sun.jna.*;
import com.sun.jna.ptr.ByReference;
import com.sun.jna.ptr.IntByReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

public final class LinuxNuma extends OSNuma.PreCalculatedNuma {

    private static final Logger LOGGER = LoggerFactory.getLogger(LinuxNuma.class);

    private static final ThreadLocal<IntByReference> CURRENT_CORE_POINTER = ThreadLocal.withInitial(IntByReference::new);
    private static final boolean LIBRARIES_AVAILABLE;
    static {
        boolean librariesOk = false;
        try {
            LibNuma.init();
            LibC.init();
            librariesOk = true;
        } catch (final Throwable ignore) {}

        if (!librariesOk) {
            LIBRARIES_AVAILABLE = false;
            LOGGER.debug("Unable to link NUMA libraries for Linux NUMA");
        } else {
            final int numaAvail = LibNuma.numa_available();
            LIBRARIES_AVAILABLE = numaAvail >= 0;
            LOGGER.debug("libnuma numa_available: " + numaAvail);
        }
    }
    public static final LinuxNuma INSTANCE;
    static {
        if (!LIBRARIES_AVAILABLE) {
            INSTANCE = null;
        } else {
            final int totalNumaNodes = LibNuma.numa_max_node() + 1;

            final Pointer cpuMask = LibNuma.numa_allocate_cpumask();
            try {
                if (cpuMask == null) {
                    INSTANCE = null;
                    LOGGER.debug("Failed to create Linux NUMA CPU mapping: Failed to allocate cpu mask for libnuma");
                } else {
                    final int totalCpus = LibNuma.numa_num_possible_cpus();

                    int[] coreToNuma = new int[totalCpus];
                    Arrays.fill(coreToNuma, -1);
                    core_to_numa_setup:
                    for (int node = 0; node < totalNumaNodes; ++node) {
                        LibNuma.numa_bitmask_clearall(cpuMask);
                        final int res = LibNuma.numa_node_to_cpus(node, cpuMask);
                        if (res != 0) {
                            // failed
                            LOGGER.debug("Failed to create Linux NUMA CPU mapping: Failed libnuma numa_node_to_cpus(" + node + ", ...): " + res);
                            coreToNuma = null;
                            break;
                        }

                        for (int cpu = 0; cpu < totalCpus; ++cpu) {
                            final int bit = LibNuma.numa_bitmask_isbitset(cpuMask, cpu);
                            if (bit == 0) {
                                // not set
                                continue;
                            }
                            if (coreToNuma[cpu] != -1) {
                                LOGGER.debug("Failed to create Linux NUMA CPU mapping: Duplicate node mapping for core " + cpu + ": " + coreToNuma[cpu] + " and " + node);
                                coreToNuma = null;
                                break core_to_numa_setup;
                            }
                            // it is set, so mark it in the core mapping
                            coreToNuma[cpu] = node;
                        }
                    }
                    if (coreToNuma != null) {
                        for (int cpu = 0; cpu < coreToNuma.length; ++cpu) {
                            final int node = coreToNuma[cpu];
                            if (node == -1) {
                                LOGGER.debug("Failed to create Linux NUMA CPU mapping: No node mapping for core " + cpu);
                                coreToNuma = null;
                                break;
                            }
                        }
                    }

                    final int[][] costArray = new int[totalNumaNodes][totalNumaNodes];
                    for (int i = 0; i < totalNumaNodes; ++i) {
                        for (int j = 0; j < totalNumaNodes; ++j) {
                            final int dist = LibNuma.numa_distance(i, j);
                            // distance is 0 when it cannot be determined
                            // distances 1-9 are reserved and have no meaning
                            costArray[i][j] = dist < 10 ? OSNuma.NUMA_DISTANCE_CANNOT_DETERMINE : dist;
                        }
                    }

                    INSTANCE = coreToNuma == null ? null : new LinuxNuma(coreToNuma, costArray);
                }
            } finally {
                if (cpuMask != null) {
                    LibNuma.numa_bitmask_free(cpuMask);
                }
            }
        }
    }

    private LinuxNuma(final int[] coreToNuma, final int[][] costArray) {
        super(coreToNuma, costArray);
    }

    @Override
    public boolean isAvailable() {
        // always available _if_ instance is constructed
        return true;
    }

    private static int getCurrentCore0() {
        final IntByReference cpu = CURRENT_CORE_POINTER.get();
        final int res = LibC.getcpu(cpu, (IntByReference)null);
        if (res == 0) {
            return cpu.getValue();
        }
        // getting errno looks hard
        throw new IllegalStateException("getcpu failed: " + res);
    }

    @Override
    public int getCurrentCore() {
        return getCurrentCore0();
    }

    private static long[] getCurrentThreadAffinity0() {
        final int cpus = LibNuma.numa_num_possible_cpus();
        if (cpus < 0) {
            throw new IllegalStateException();
        }
        final CpuSet cpuSet = new CpuSet(cpus);

        final int res = LibC.sched_getaffinity(0, (int)cpuSet.sizeof(), cpuSet.getPointer());
        if (res != 0) {
            throw new IllegalStateException();
        }

        return cpuSet.toLongs();
    }

    @Override
    public long[] getCurrentThreadAffinity() {
        return getCurrentThreadAffinity0();
    }

    private static void setCurrentThreadAffinity0(final long[] to) {
        final CpuSet cpuSet = new CpuSet(to);

        // ignore res, since we don't have errno we don't know if this is a problem with our bitset
        // or if the process simply doesn't have perms to do this
        LibC.sched_setaffinity(0, (int)cpuSet.sizeof(), cpuSet.getPointer());
    }

    @Override
    public void setCurrentThreadAffinity(final long[] to) {
        setCurrentThreadAffinity0(to);
    }

    private static final class LibNuma {
        static {
            Native.register("numa");
        }

        public static void init() {}

        /* int numa_available(void); */
        public static native int numa_available();

        /* int numa_max_node(void); */
        public static native int numa_max_node();

        /* int numa_distance(int node1, int node2); */
        public static native int numa_distance(final int node1, final int node2);

        /* struct bitmask *numa_allocate_cpumask(); */
        public static native Pointer numa_allocate_cpumask();

        /* struct bitmask *numa_bitmask_clearall(struct bitmask *); */
        public static native Pointer numa_bitmask_clearall(final Pointer bitmask);

        /* int numa_bitmask_isbitset(const struct bitmask *, unsigned int); */
        public static native int numa_bitmask_isbitset(final Pointer bitmask, final int bit);

        /* int numa_node_to_cpus(int, struct bitmask *); */
        public static native int numa_node_to_cpus(final int node, final Pointer bitmask);

        /* void numa_bitmask_free(struct bitmask *); */
        public static native void numa_bitmask_free(final Pointer bitmask);

        /* int numa_num_possible_cpus(); */
        public static native int numa_num_possible_cpus();
    }

    private static final class LibC {
        static {
            Native.register(Platform.C_LIBRARY_NAME);
        }

        public static void init() {}

        /* int getcpu(unsigned int *_Nullable cpu, unsigned int *_Nullable node); */
        public static native int getcpu(final IntByReference cpu, final IntByReference node);

        /* int sched_setaffinity(pid_t pid, size_t cpusetsize, const cpu_set_t *mask); */
        public static native int sched_setaffinity(final int pid, final int cpusetsize, final Pointer cpuset);

        /* int sched_getaffinity(pid_t pid, size_t cpusetsize, cpu_set_t *mask) */
        public static native int sched_getaffinity(final int pid, final int cpusetsize, final Pointer cpuset);
    }

    private static final class CpuSet extends ByReference {

        private static int roundLongSizeBits(final int bits) {
            // convert to bytes
            final int bytes = (bits + (Byte.SIZE - 1)) / Byte.SIZE;
            return roundLongSizeBytes(bytes);
        }

        private static int roundLongSizeBytes(final int bytes) {
            // now convert to long
            final int longs = (bytes + (Native.LONG_SIZE - 1)) / Native.LONG_SIZE;

            // back to bytes
            return longs * Native.LONG_SIZE;
        }

        public CpuSet(final int bits) {
            super(roundLongSizeBits(bits));
        }

        public CpuSet(final long[] bitset) {
            super(roundLongSizeBytes(bitset.length * Long.BYTES));
            for (int i = 0; i < bitset.length; ++i) {
                this.getPointer().setLong((long)i << 3, bitset[i]);
            }
        }

        public long[] toLongs() {
            final long[] ret = new long[(int)((this.sizeof() + (Long.BYTES - 1)) / Long.BYTES)];

            for (int i = 0; i < ret.length; ++i) {
                ret[i] = this.getPointer().getLong((long)i << 3);
            }

            return ret;
        }

        public long sizeof() {
            return ((Memory)this.getPointer()).size();
        }
    }
}
