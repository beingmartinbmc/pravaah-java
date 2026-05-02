package io.github.beingmartinbmc.pravaah.internal.util;

/**
 * Small map / hash helpers shared by the row materialiser, schema validator,
 * and any other component that needs to size {@link java.util.HashMap}-backed
 * containers without rehashing.
 */
public final class Maps {

    /** {@link java.util.HashMap}'s default load factor. */
    public static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /** Minimum bucket count returned by {@link #mapCapacity(int)}. */
    public static final int MIN_MAP_CAPACITY = 4;

    private Maps() {}

    /**
     * Returns a {@link java.util.HashMap} initial capacity that holds {@code entries}
     * elements without triggering a resize at the default load factor.
     */
    public static int mapCapacity(int entries) {
        return Math.max(MIN_MAP_CAPACITY, (int) (entries / DEFAULT_LOAD_FACTOR) + 1);
    }
}
