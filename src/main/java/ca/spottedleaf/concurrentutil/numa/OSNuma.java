package ca.spottedleaf.concurrentutil.numa;

import ca.spottedleaf.concurrentutil.util.FlatBitsetUtil;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public interface OSNuma {

    public static int NUMA_DISTANCE_CANNOT_DETERMINE = 255;

    public static OSNuma getNativeInstance() {
        final LinuxNuma linux = LinuxNuma.INSTANCE;
        if (linux != null && linux.isAvailable()) {
            return linux;
        }

        return NoOp.INSTANCE;
    }

    /**
     * Returns whether interaction with the OS for NUMA is available. If no interaction is available,
     * then the following applies:
     * <ul>
     *     <li>
     *         There exists only one NUMA node including all the available processors.
     *     </li>
     *     <li>
     *         Scheduling affinity methods will throw {@link UnsupportedOperationException}
     *     </li>
     * </ul>
     * @return Whether there exists native support to retrieve NUMA information and set scheduling affinity.
     */
    public boolean isAvailable();

    /**
     * Returns the distance between the specified NUMA nodes. The return values are defined by the ACPI SLIT table
     * present on the system.
     * @param n1 First node
     * @param n2 Second node
     * @return The distance between the NUMA nodes.
     * @see <a href="https://uefi.org/htmlspecs/ACPI_Spec_6_4_html/05_ACPI_Software_Programming_Model/ACPI_Software_Programming_Model.html#system-locality-information-table-slit">System Locality Information Table</a>
     */
    public int getNumaDistance(final int n1, final int n2);

    /**
     * Returns the total number of nodes on the system.
     * @return The total number of nodes on the system.
     */
    public int getTotalNumaNodes();

    /**
     * Returns an N by N array where {@code cost[i][j] = getNumaDistance(i,j)}
     * @return The node distance matrix.
     * @see <a href="https://uefi.org/htmlspecs/ACPI_Spec_6_4_html/05_ACPI_Software_Programming_Model/ACPI_Software_Programming_Model.html#system-locality-information-table-slit">System Locality Information Table</a>
     */
    public int[][] getNodeDistances();

    /**
     * Returns the NUMA node associated with the specified core. If the core is not valid, then returns {@code -1}.
     * @param coreId The specified core
     * @return The NUMA node for the core.
     */
    public int getNumaNode(final int coreId);

    /**
     * Returns the total number of cores on the system.
     * @return The total number of cores on the system.
     */
    public int getTotalCores();

    /**
     * Returns the cores associated with the specified NUMA node. If the node is not valid, then returns {@code null}.
     * <p>
     *     Note that the returned array is not a bitset. It is simply a list of core numbers.
     * </p>
     * @param numaId The specified NUMA node.
     * @return The cores associated with the specified NUMA node.
     */
    public int[] getCores(final int numaId);

    /**
     * Returns the current NUMA node that this thread is running on. If NUMA interaction is not available,
     * then {@code 0} will be returned.
     * @return The current NUMA node that this thread is running on
     */
    public int getCurrentNumaNode();

    /**
     * Returns the current core that this thread is running on. If NUMA interaction is not available,
     * then {@code 0} will be returned.
     * @return The current core this thread is running on
     * @see #isAvailable()
     */
    public int getCurrentCore();

    /**
     * Returns the current thread's affinity mask.
     * If NUMA interaction is not available, then this function will return an undefined result.
     * @return The current thread's affinity mask.
     * @see #isAvailable()
     */
    public long[] getCurrentThreadAffinity();

    /**
     * Attempts to adjust the current thread's affinity mask to the specified bitset.
     * If NUMA interaction is not available, then this function is a no-op.
     * @param to Specified new affinity bitmask.
     * @see #isAvailable()
     */
    public void setCurrentThreadAffinity(final long[] to);

    /**
     * Attempts to adjust the current thread's NUMA affinity mask to the specified bitset.
     * This is equivalent to invoking {@link #setCurrentThreadAffinity(long[])} with a bitmask specifying all
     * cores present in the specified NUMA nodes.
     * If NUMA interaction is not available, then this function is a no-op.
     * @param to Specified new NUMA affinity bitmask.
     * @see #isAvailable()
     */
    public default void setCurrentNumaAffinity(final long[] to) {
        this.setCurrentNumaAffinity(FlatBitsetUtil.bitsetToInts(to));
    }

    /**
     * Attempts to adjust the current thread's NUMA affinity mask to the specified list of numa nodes.
     * This is equivalent to invoking {@link #setCurrentThreadAffinity(long[])} with a bitmask specifying all
     * cores present in the specified NUMA nodes.
     * If NUMA interaction is not available, then this function is a no-op.
     * @param numaNodes Specified new NUMA nodes.
     * @see #isAvailable()
     */
    public default void setCurrentNumaAffinity(final int[] numaNodes) {
        final IntArrayList cores = new IntArrayList();
        for (final int node : numaNodes) {
            cores.addAll(IntArrayList.wrap(this.getCores(node)));
        }

        this.setCurrentThreadAffinity(FlatBitsetUtil.intsToBitset(cores.toIntArray()));
    }

    public static abstract class PreCalculatedNuma implements OSNuma {

        private final int[] coreToNuma;
        private final int[][] costArray;
        private final int[][] numaToCore;

        public PreCalculatedNuma(final int[] coreToNuma, final int[][] costArray) {
            this.coreToNuma = coreToNuma;
            this.costArray = costArray;

            final IntArrayList[] numaToCore = new IntArrayList[this.costArray.length];
            for (int i = 0; i < numaToCore.length; ++i) {
                numaToCore[i] = new IntArrayList();
            }

            for (int core = 0; core < coreToNuma.length; ++core) {
                numaToCore[coreToNuma[core]].add(core);
            }

            this.numaToCore = new int[this.costArray.length][];
            for (int i = 0; i < this.numaToCore.length; ++i) {
                this.numaToCore[i] = numaToCore[i].toIntArray();
            }
        }

        @Override
        public int getNumaDistance(final int n1, final int n2) {
            if (n1 < 0 || n1 >= this.costArray.length) {
                // cannot determine
                return NUMA_DISTANCE_CANNOT_DETERMINE;
            }
            final int[] distances = this.costArray[n1];
            if (n2 < 0 || n2 >= distances.length) {
                // cannot determine
                return NUMA_DISTANCE_CANNOT_DETERMINE;
            }
            return distances[n2];
        }

        @Override
        public int getTotalNumaNodes() {
            return this.costArray.length;
        }

        @Override
        public int[][] getNodeDistances() {
            final int[][] ret = new int[this.costArray.length][];
            for (int i = 0; i < ret.length; ++i) {
                ret[i] = this.costArray[i].clone();
            }

            return ret;
        }

        @Override
        public int getNumaNode(final int coreId) {
            if (coreId < 0 || coreId >= this.coreToNuma.length) {
                // cannot determine
                return -1;
            }
            return this.coreToNuma[coreId];
        }

        @Override
        public int getTotalCores() {
            return this.coreToNuma.length;
        }

        @Override
        public int[] getCores(final int numaId) {
            if (numaId < 0 || numaId >= this.numaToCore.length) {
                // cannot determine
                return null;
            }
            return this.numaToCore[numaId].clone();
        }

        @Override
        public int getCurrentNumaNode() {
            return this.coreToNuma[this.getCurrentCore()];
        }
    }

    public static final class NoOp extends PreCalculatedNuma {
        public static final NoOp INSTANCE = new NoOp();
        private final long[] currentThreadAffinity;

        private NoOp() {
            // ACPI SLIT table defines "10" as the relative distance
            super(new int[Runtime.getRuntime().availableProcessors()], new int[][] { new int[] { 10 } });
            this.currentThreadAffinity = FlatBitsetUtil.intsToBitset(this.getCores(0));
        }

        @Override
        public boolean isAvailable() {
            return false;
        }

        @Override
        public int getCurrentCore() {
            return 0;
        }

        @Override
        public long[] getCurrentThreadAffinity() {
            return this.currentThreadAffinity.clone();
        }

        @Override
        public void setCurrentThreadAffinity(final long[] to) {}
    }
}
