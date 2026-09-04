# VG Updates — 2026-09-03

Performance / idle-allocation / diagnostics work on the streaming aggregator
pipeline. All changes compile against Java 17 (`./gradlew compileJava` is green).
The hot-path `WaitStrategy` was intentionally left unchanged
(`SpinCountBackoffWaitStrategy(10000, new LiteBlockingWaitStrategy())`) — the
optimizations below reduce **allocations**, not scheduling.

---

## 1. Diagnostic-print cleanup

`java/org/jlab/coda/emu/modules/StreamAggregator.java`

Commented out the `"Agg mod: … wait"` and `"Agg mod: … got"` prints that fire
on the hot event path or per handshake step:

| Line | Was | Reason |
|------|-----|--------|
| 425  | `System.out.println("  Agg mod: wait ch" + …)` | fires **per event** in `eventToOutputRing` |
| 427  | `System.out.println("  Agg mod: got item for " + …)` | fires **per event** in `eventToOutputRing` |
| 471  | `System.out.println("  Agg mod: getAllControlEvents wait for seq " + …)` | per channel at prestart/go/end |
| 474  | `System.out.println("  Agg mod: getAllControlEvents got seq " + …)` | per channel at prestart/go/end |
| 904  | `System.out.println("  Agg mod: findEnd, chan " + …)` | fires once at END, but was noisy |

**Kept active** (transition markers, not hot):
- L1042 `Agg mod: got all PRESTART events`
- L1056 `Agg mod: got all END events`
- L1082 `Agg mod: got all GO events`
- L1299 `Agg mod: sorter got user event …`

The `if (debug) System.out.println(...)` prints at L1034 and L1076 were already
gated and left untouched.

---

## 2. Empty-frame `PayloadBuffer` pooling

`java/org/jlab/coda/emu/modules/StreamAggregator.java`

**Problem:** `sendEmptyFrameToTimeSliceRing` was calling
`emptyFrameBuffer.clone()` on every synthesized empty frame.
`PayloadBuffer.clone()` deep-copies the `ByteBuffer`
(`PayloadBuffer.java:157`: `ByteBuffer.allocate(buf.capacity()).order(order);`),
so a 116-frame gap burst produced **116 fresh `PayloadBuffer`s + 116 fresh
`byte[]`s** on the sorter's hot path.

**Fix:** Pre-clone a pool of empty-frame `PayloadBuffer`s at prestart, one pool
per build thread, and cycle a cursor over the pool at emit time.
`Evio.updateEmptyFrameBuffer` does absolute writes at fixed offsets, so
reusing the same `ByteBuffer` is safe.

### 2.1 New fields

```java
//-------------------------------------------
// Empty-frame PayloadBuffer pool (one pool per build thread).
// Sized to sorterRingSize so a slot is never reused while a build
// thread still references the previous occupant …
//-------------------------------------------
private PayloadBuffer[][] emptyFramePool;
private int[] emptyFrameCursor;
```

### 2.2 Pool initialization (in `prestart`, right after the sorter rings are built)

```java
//------------------------------------------------
// Pre-clone empty-frame PayloadBuffers, one pool per build thread.
// Size = 2 * sorterRingSize to safely cover the full pipeline depth:
// a pool slot's ByteBuffer may still be referenced by the output
// ring / writer thread after the build thread has released the
// sorter-ring slot. 2x sorterRingSize (kept a power of 2 so the
// cursor can be masked with & (size-1)) leaves plenty of headroom
// above sorterRingSize + outputRingSize.
//------------------------------------------------
int emptyPoolSize = sorterRingSize * 2;
emptyFramePool   = new PayloadBuffer[buildingThreadCount][emptyPoolSize];
emptyFrameCursor = new int[buildingThreadCount];
for (int j = 0; j < buildingThreadCount; j++) {
    for (int k = 0; k < emptyPoolSize; k++) {
        emptyFramePool[j][k] = (PayloadBuffer) emptyFrameBuffer.clone();
    }
}
```

### 2.3 Rewritten `sendEmptyFrameToTimeSliceRing` (allocation-free)

```java
private void sendEmptyFrameToTimeSliceRing(long skippedFrame, long timestamp, int btIndex)
        throws InterruptedException {

    // Pooled empty-frame PayloadBuffer. Pool size == sorterRingSize (power of 2)
    // guarantees the slot we hand out is not still owned by the build thread:
    // the sorter cannot claim more sorter-ring slots than the build thread
    // has released via buildSequenceIn, so cycling the pool at the same rate
    // is safe. Reuses avoid the per-skip byte[] + PayloadBuffer allocations
    // that clone() would perform.
    int cursor = emptyFrameCursor[btIndex];
    PayloadBuffer emptyBuffer = emptyFramePool[btIndex][cursor];
    emptyFrameCursor[btIndex] = (cursor + 1) & (emptyFramePool[btIndex].length - 1);

    try {
        Evio.updateEmptyFrameBuffer(skippedFrame, timestamp, inputChannelCount, emptyBuffer);
    }
    catch (EmuException e) {/*never happen*/}

    // Important for Build Thread cause this is where it gets frame #
    emptyBuffer.setTimeFrame(skippedFrame);

    getSequences[btIndex] = sorterRingBuffers[btIndex].nextIntr(1);
    TimeSliceBankItem item = sorterRingBuffers[btIndex].get(getSequences[btIndex]);
    item.setBuf(emptyBuffer);
    sorterRingBuffers[btIndex].publish(getSequences[btIndex]);
}
```

### 2.4 Sizing rationale

Buffers can be simultaneously in flight in:

- the sorter ring (up to `sorterRingSize = 4096`)
- the output ring (~`outputRingSize`, typically ≤ 256)
- the output writer thread's local reference (1)

Pool size `= 2 × sorterRingSize = 8192` gives plenty of headroom above
`sorterRingSize + outputRingSize + slack`. Memory per pool slot ≈ 200 B,
so total memory is ~3 MB per build thread — negligible.

---

## 3. Status-reporter caching + skip-when-unchanged

### 3.1 Cached `Object[11]` — `java/org/jlab/coda/emu/modules/ModuleAdapter.java`

Replaced per-call allocation with a reused field:

```java
/** Cached array reused across getStatistics() calls to avoid per-call allocation.
 *  Autoboxing of the values it holds is unavoidable without an API change. */
private final Object[] statsCache = new Object[11];

/** {@inheritDoc} */
synchronized public Object[] getStatistics() {
    // … populate statsCache[0..10] as before …
    return statsCache;
}
```

The 8 auto-boxings per call (`Long`, `Integer`, `Float`) remain — removing
them requires changing the `getStatistics()` API (a follow-up).

### 3.2 Skip-when-unchanged — `java/org/jlab/coda/emu/Emu.java`

`cMsgPayloadItem` is **immutable** (constructors + getters only; no setters
exposed) so we cannot update items in place. Instead, skip the whole
`sendStatusMessage()` when no counter or state has moved. A keepalive still
fires every `KEEPALIVE_MULTIPLIER = 5` periods (~10 s at 2 s cadence) so run
control never sees us as silent.

**New fields inside `StatusReportingThread`:**

```java
//--------------------------------------------------------------
// "Skip when unchanged" — avoids allocating ~18 cMsgPayloadItem
// objects per report during idle periods when no data is flowing.
// The full report is still sent whenever a counter changes OR at
// least once every KEEPALIVE_MULTIPLIER periods so run control
// never sees us as silent.
//--------------------------------------------------------------
private long prevEventCount = -1L;
private long prevWordCount  = -1L;
private long prevFrameCount = -1L;
private CODAStateIF prevState = null;
private int  skippedReports = 0;
private static final int KEEPALIVE_MULTIPLIER = 5;  // ~10 s at 2 s period
```

**New skip check in `sendStatusMessage()`**, inserted just before the
`try { reportMsg.addPayloadItem(new cMsgPayloadItem(…state…)); … }`:

```java
// Skip the whole report (and its ~18 payload-item allocations)
// if no counter has moved and state is unchanged, so long as we
// still send at least one keepalive per KEEPALIVE_MULTIPLIER
// periods so run control does not assume we're dead.
CODAStateIF curState = state();
if (eventCount == prevEventCount &&
    wordCount  == prevWordCount  &&
    frameCount == prevFrameCount &&
    curState   == prevState      &&
    skippedReports < KEEPALIVE_MULTIPLIER) {
    skippedReports++;
    return;
}
skippedReports = 0;
prevEventCount = eventCount;
prevWordCount  = wordCount;
prevFrameCount = frameCount;
prevState      = curState;
```

**Effect:** during idle, 4 of every 5 reports are fully suppressed. When
data is flowing, behavior is unchanged.

---

## 4. Programmatic thread affinity

Optional CPU pinning via a lightweight reflection wrapper around OpenHFT's
`net.openhft.affinity.AffinityLock`. No hard compile-time or run-time
dependency: if the jar is missing, every call is a silent no-op. macOS is
inherently unsupported by the underlying library — code degrades to no-op
there as well, so nothing breaks on dev machines.

### 4.1 New helper — `java/org/jlab/coda/emu/support/ThreadAffinity.java`

```java
package org.jlab.coda.emu.support;

/** … see file for full javadoc … */
public final class ThreadAffinity {
    public static boolean isAvailable();
    public static boolean tryPin();               // OpenHFT picks a free CPU
    public static boolean tryPinCore();           // whole physical core
    public static boolean tryPin(int cpuId);      // specific CPU id
    public static void    release();              // safe to call unconditionally
}
```

The class discovers `AffinityLock` reflectively at class-init:

```java
static {
    boolean available = false;
    …
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
    …
}
```

Per-thread lock references are stored in a `ConcurrentHashMap<Thread, Object>`
so `release()` can find the right lock without the caller having to keep the
reference.

### 4.2 `StreamAggregator.java` — module-level config + sorter & build threads

**Config parsing (in constructor):**

```java
//--------------------------------------------------------------------
// CPU affinity. Accepts:
//   "off"  / absent → no pinning (default)
//   "auto"          → let OpenHFT pick a free CPU per thread
//   "core"          → pin each thread to a whole physical core
//   "3,5,7,9"       → explicit CPU-id list, one per thread in start order
//
// Requires the OpenHFT affinity jar (net.openhft:affinity) on the classpath;
// otherwise the setting is silently ignored. See ThreadAffinity.java.
//--------------------------------------------------------------------
affinityMode = "off";
affinityCpus = null;
String affStr = attributeMap.get("affinity");
if (affStr != null && !affStr.isEmpty()) {
    String s = affStr.trim().toLowerCase();
    if (s.equals("auto") || s.equals("core") || s.equals("off")) {
        affinityMode = s;
    }
    else {
        // Parse explicit CPU-id list
        String[] parts = s.split(",");
        int[] cpus = new int[parts.length];
        boolean ok = true;
        for (int i = 0; i < parts.length; i++) {
            try { cpus[i] = Integer.parseInt(parts[i].trim()); }
            catch (NumberFormatException e) { ok = false; break; }
        }
        if (ok) {
            affinityMode = "list";
            affinityCpus = cpus;
        }
    }
}
if (!"off".equals(affinityMode)) {
    logger.info("  Agg mod: CPU affinity = " + affinityMode + …);
}
```

**Fields + helper:**

```java
private String affinityMode;
private int[]  affinityCpus;
private final java.util.concurrent.atomic.AtomicInteger affinityCursor =
        new java.util.concurrent.atomic.AtomicInteger(0);

private boolean pinCurrentThreadIfConfigured() {
    switch (affinityMode) {
        case "auto": return org.jlab.coda.emu.support.ThreadAffinity.tryPin();
        case "core": return org.jlab.coda.emu.support.ThreadAffinity.tryPinCore();
        case "list":
            int idx = affinityCursor.getAndIncrement();
            if (idx < affinityCpus.length) {
                return org.jlab.coda.emu.support.ThreadAffinity.tryPin(affinityCpus[idx]);
            }
            return false;
        default: return false;
    }
}
```

**Sorter thread — top of `run()`:**

```java
public void run() {

    boolean affinityPinned = pinCurrentThreadIfConfigured();
    try {

    // … existing sorter body, including its own try/catch …

    } // end outer try for affinity scope
    finally {
        if (affinityPinned) org.jlab.coda.emu.support.ThreadAffinity.release();
    }
}
```

**Build thread — piggy-backed on the existing `finally`:**

```java
public void run() {

    boolean affinityPinned = pinCurrentThreadIfConfigured();
    try {
        // … existing body …
    }
    catch (…) { … }
    finally {
        // existing input-channel drain code
        …
        // Release CPU affinity, if any was acquired at the top of run().
        if (affinityPinned) org.jlab.coda.emu.support.ThreadAffinity.release();
    }

    if (debug) System.out.println("  Agg mod: Building thread is ending");
}
```

### 4.3 `DataChannelImplTcpStream.java` — reader-thread pinning

**Config parsing (in constructor, after `recvBuf` handling):**

```java
//--------------------------------------------------------------
// CPU affinity for the socket-reader thread.
//   "off"  / absent  → no pinning (default)
//   "auto"           → OpenHFT picks a free CPU
//   "core"           → whole physical core
//   "<int>"          → pin to that CPU id
// Requires OpenHFT affinity jar on the classpath, otherwise no-op.
//--------------------------------------------------------------
readerAffinityMode = "off";
readerAffinityCpu  = -1;
String affStr = attributeMap.get("affinity");
if (affStr != null && !affStr.isEmpty()) {
    String s = affStr.trim().toLowerCase();
    if (s.equals("auto") || s.equals("core") || s.equals("off")) {
        readerAffinityMode = s;
    }
    else {
        try {
            readerAffinityCpu  = Integer.parseInt(s);
            readerAffinityMode = "cpu";
        }
        catch (NumberFormatException e) { /* leave mode off */ }
    }
}
```

**New fields:**

```java
private String readerAffinityMode = "off";
private int    readerAffinityCpu  = -1;
```

**`DataInputHelper.run()` — pin at start, release in `finally`:**

```java
public void run() {
    latch.countDown();

    // Pin this thread to a CPU if configured; released in finally below.
    boolean affinityPinned = false;
    switch (readerAffinityMode) {
        case "auto": affinityPinned = org.jlab.coda.emu.support.ThreadAffinity.tryPin(); break;
        case "core": affinityPinned = org.jlab.coda.emu.support.ThreadAffinity.tryPinCore(); break;
        case "cpu":  affinityPinned = org.jlab.coda.emu.support.ThreadAffinity.tryPin(readerAffinityCpu); break;
        default: break;
    }

    // … existing reader body …

    try { … }
    catch (Exception e) {
        if (haveInputEndEvent) {
            if (affinityPinned) org.jlab.coda.emu.support.ThreadAffinity.release();
            return;
        }
        …
    }
    finally {
        if (affinityPinned) org.jlab.coda.emu.support.ThreadAffinity.release();
    }
}
```

### 4.4 Enabling affinity at runtime

Drop the OpenHFT affinity jar into `java/jars/`. Gradle picks it up
automatically via the existing
`implementation(files("java/jars").asFileTree.matching { include("*.jar") })`
line in `build.gradle.kts`:

```bash
curl -L -o java/jars/affinity-3.23ea1.jar \
  https://repo1.maven.org/maven2/net/openhft/affinity/3.23ea1/affinity-3.23ea1.jar
```

Then set `affinity="auto"` (or `"core"`, or an explicit CPU list) on the
sagg/pagg module and on the reader channels in `jcedit`.

On startup, look for:

```
  Agg mod: CPU affinity = auto
      DataChannel TcpStream: reader affinity = auto
```

On Linux without isolated cores, `AffinityLock.acquireLock()` still claims a
CPU but the kernel can schedule other work on it. For true isolation, add
`isolcpus=…` and `nohz_full=…` on the kernel command line and pass the same
CPU ids to `affinity=…`.

---

## Files changed

```
 java/org/jlab/coda/emu/Emu.java                                        (+  status-reporter skip)
 java/org/jlab/coda/emu/modules/ModuleAdapter.java                      (+  cached statsCache)
 java/org/jlab/coda/emu/modules/StreamAggregator.java                   (+  empty-frame pool
                                                                            + affinity fields/helper
                                                                            + pin/release in sorter & build run()
                                                                            + commented hot-path prints)
 java/org/jlab/coda/emu/support/ThreadAffinity.java                     (new)
 java/org/jlab/coda/emu/support/transport/DataChannelImplTcpStream.java (+  reader affinity)
```

Build check: `./gradlew --console=plain compileJava` → BUILD SUCCESSFUL.

## Not done in this session (intentional)

- `WaitStrategy` **not** changed — a brief experiment with pure
  `LiteBlockingWaitStrategy` was reverted because it adds 1–10 µs park/unpark
  per event. Original `SpinCountBackoffWaitStrategy(10000, LiteBlockingWaitStrategy)`
  is retained on all sorter / build rings.
- No change to `ByteBufferSupply`'s WaitStrategy either.
- The `sockChannel.read(wordCmdBuf)` short-read bug for `direct=true` (see
  earlier review) is **not** fixed; currently latent because the code path
  uses `direct=false` (`DataInputStream.readLong()` which loops correctly),
  but should be addressed if `direct` is ever enabled.
- The NPE at `StreamAggregator:1914` (~± a few lines shift due to earlier
  debug prints in the compiled binary) was investigated but not conclusively
  root-caused from source alone. A JDK ≥ 14 with
  `-XX:+ShowCodeDetailsInExceptionMessages` will name the failing reference in
  the next reproduction.

## Suggested validation

1. Rebuild JAR and deploy.
2. Short run with a JFR profile recording:
   ```
   -XX:StartFlightRecording=filename=idle.jfr,duration=30m,settings=profile
   ```
   Expect: idle allocation rate for `PayloadBuffer`, `Object[]`, and
   `cMsgPayloadItem` drops by > 90 % vs. baseline.
3. Reproduce the 116-frame gap scenario. Expect: `sendEmptyFrameToTimeSliceRing`
   no longer appears among top allocators in JFR.
4. If affinity jar is installed and `affinity="auto"` is set: `top -H -p <pid>`
   should show sorter / build / reader threads pinned to distinct CPUs.
