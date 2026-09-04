/*
 * Copyright (c) 2025, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 */

package org.jlab.coda.emu.support;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Lightweight, reflection-based wrapper around OpenHFT's {@code net.openhft.affinity.AffinityLock}.
 *
 * <p>Callers can pin the current thread to a specific CPU (or let the library pick one) without
 * a hard compile-time or run-time dependency on the affinity library. If the library is not on
 * the classpath — or if the platform does not support setting affinity (e.g. macOS) — every call
 * is a no-op that returns {@code false}.</p>
 *
 * <p>Threads that pin themselves should call {@link #release()} in a {@code finally} block so
 * the underlying lock is released even if the thread exits by exception.
 *
 * <p>Example usage inside a thread's {@code run()}:
 * <pre>{@code
 * public void run() {
 *     boolean pinned = ThreadAffinity.tryPin(coreId);   // no-op on macOS or if lib missing
 *     try {
 *         // hot loop
 *     } finally {
 *         if (pinned) ThreadAffinity.release();
 *     }
 * }
 * }</pre>
 */
public final class ThreadAffinity {

    // Whether the underlying library is available. Resolved once at class init.
    private static final boolean AVAILABLE;
    private static final Method  ACQUIRE_LOCK;              // AffinityLock.acquireLock()
    private static final Method  ACQUIRE_CORE_LOCK;         // AffinityLock.acquireCore()
    private static final Method  ACQUIRE_ON_CPU_LOCK;       // AffinityLock.acquireLock(int)
    private static final Method  RELEASE_LOCK;              // AffinityLock.release()

    // Per-thread lock reference so release() can find the right AffinityLock to release.
    private static final ConcurrentMap<Thread, Object> LOCKS = new ConcurrentHashMap<>();

    static {
        boolean available = false;
        Method acquire = null, acquireCore = null, acquireOnCpu = null, release = null;
        try {
            Class<?> alClass = Class.forName("net.openhft.affinity.AffinityLock");
            acquire      = alClass.getMethod("acquireLock");
            acquireCore  = alClass.getMethod("acquireCore");
            acquireOnCpu = alClass.getMethod("acquireLock", int.class);
            release      = alClass.getMethod("release");
            available    = true;
        }
        catch (ClassNotFoundException | NoSuchMethodException e) {
            // Library not on classpath — every method below silently returns false.
        }
        AVAILABLE            = available;
        ACQUIRE_LOCK         = acquire;
        ACQUIRE_CORE_LOCK    = acquireCore;
        ACQUIRE_ON_CPU_LOCK  = acquireOnCpu;
        RELEASE_LOCK         = release;
    }

    private ThreadAffinity() { /* static only */ }

    /** @return true if the OpenHFT affinity library is on the classpath. */
    public static boolean isAvailable() { return AVAILABLE; }

    /**
     * Pin the current thread to an OS-chosen CPU (an unused one, respecting other AffinityLocks).
     * @return true on success, false if the library is missing or the platform is unsupported.
     */
    public static boolean tryPin() {
        if (!AVAILABLE) return false;
        try {
            Object lock = ACQUIRE_LOCK.invoke(null);
            if (lock == null) return false;
            LOCKS.put(Thread.currentThread(), lock);
            return true;
        }
        catch (ReflectiveOperationException e) {
            return false;
        }
    }

    /**
     * Pin the current thread to a whole physical core (avoids hyper-thread pair contention).
     * @return true on success, false if the library is missing or the platform is unsupported.
     */
    public static boolean tryPinCore() {
        if (!AVAILABLE) return false;
        try {
            Object lock = ACQUIRE_CORE_LOCK.invoke(null);
            if (lock == null) return false;
            LOCKS.put(Thread.currentThread(), lock);
            return true;
        }
        catch (ReflectiveOperationException e) {
            return false;
        }
    }

    /**
     * Pin the current thread to the given CPU id.
     * @param cpuId zero-based logical CPU id (as reported by {@code /proc/cpuinfo}).
     * @return true on success, false if the library is missing, platform is unsupported,
     *         or the requested CPU is already reserved.
     */
    public static boolean tryPin(int cpuId) {
        if (!AVAILABLE) return false;
        try {
            Object lock = ACQUIRE_ON_CPU_LOCK.invoke(null, cpuId);
            if (lock == null) return false;
            LOCKS.put(Thread.currentThread(), lock);
            return true;
        }
        catch (ReflectiveOperationException e) {
            return false;
        }
    }

    /**
     * Release the affinity lock held by the current thread, if any. Safe to call unconditionally
     * in a {@code finally} block.
     */
    public static void release() {
        Object lock = LOCKS.remove(Thread.currentThread());
        if (lock == null || RELEASE_LOCK == null) return;
        try {
            RELEASE_LOCK.invoke(lock);
        }
        catch (ReflectiveOperationException ignored) { /* best-effort */ }
    }
}
