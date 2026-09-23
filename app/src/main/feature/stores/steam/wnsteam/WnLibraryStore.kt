package com.winlator.cmod.feature.stores.steam.wnsteam

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import org.json.JSONArray
import org.json.JSONObject
import timber.log.Timber
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Kotlin facade over the native [WnSteamSession]'s library store.
 *
 * Native observer callbacks are coalesced and then resolved through a
 * revision-based delta API. The initial refresh is a full baseline; subsequent
 * refreshes transfer and parse only changed apps/packages.
 */
class WnLibraryStore(private val session: WnSteamSession) {

    private val _snapshots =
        MutableSharedFlow<WnLibrarySnapshot>(
            replay = 1,
            extraBufferCapacity = 1,
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
        )

    /** Compatibility flow for callers that still need complete snapshots. */
    val snapshots: SharedFlow<WnLibrarySnapshot> = _snapshots.asSharedFlow()

    private val updateChannel = Channel<WnLibraryDelta>(Channel.UNLIMITED)

    /**
     * Reliable single-consumer stream of native deltas. Unlike [snapshots], this
     * never rebuilds or traverses the whole library after the initial baseline.
     */
    val updates: Flow<WnLibraryDelta> = updateChannel.receiveAsFlow()

    private val refreshScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val refreshScheduled = AtomicBoolean(false)
    private val refreshLock = Any()
    private val scheduleLock = Any()
    private val packagesById = LinkedHashMap<Int, WnOwnedPackage>()
    private val ownedAppsById = LinkedHashMap<Int, WnOwnedApp>()
    private var refreshJob: Job? = null
    private var lastRevision = 0L
    private var allAppsCount = 0
    private var ownedAppsCount = 0
    private var initialSnapshotEmitted = false

    @Volatile
    private var observing = false

    private val nativeObserver = WnLibraryObserver { scheduleRefresh() }

    /** Last merged snapshot, built on demand for synchronous compatibility callers. */
    val current: WnLibrarySnapshot
        get() =
            synchronized(refreshLock) {
                snapshotLocked()
            }

    /** Wire the native observer and acquire the initial full baseline. */
    fun startObserving() {
        synchronized(scheduleLock) {
            observing = true
        }
        session.setLibraryObserver(nativeObserver)
        refresh()
    }

    fun stopObserving() {
        val pending =
            synchronized(scheduleLock) {
                observing = false
                refreshScheduled.set(false)
                refreshJob.also { refreshJob = null }
            }
        pending?.cancel()
        session.setLibraryObserver(null)
        synchronized(refreshLock) {
            // Barrier: wait for a refresh already inside JNI before session teardown.
        }
    }

    private fun scheduleRefresh() {
        synchronized(scheduleLock) {
            if (!observing) return
            if (!refreshScheduled.compareAndSet(false, true)) return
            refreshJob =
                refreshScope.launch {
                    val thisJob = coroutineContext[Job]
                    try {
                        delay(250L)
                        if (observing) refresh()
                    } finally {
                        synchronized(scheduleLock) {
                            if (refreshJob === thisJob) {
                                refreshScheduled.set(false)
                                refreshJob = null
                            }
                        }
                    }
                }
        }
    }

    /** Fetch and merge only entities changed after [lastRevision]. */
    fun refresh() {
        synchronized(refreshLock) {
            if (!observing) return
            val json = session.getLibraryDeltaJson(lastRevision)
            val delta =
                runCatching { parseDelta(json) }
                    .onFailure {
                        Timber
                            .tag(TAG)
                            .w(
                                it,
                                "delta parse failed at revision=%d; json=%s",
                                lastRevision,
                                json.take(200),
                            )
                    }.getOrNull()
                    ?: return

            if (delta.revision < lastRevision) {
                Timber.tag(TAG).w(
                    "native library revision moved backwards (%d -> %d); rebuilding baseline",
                    lastRevision,
                    delta.revision,
                )
                packagesById.clear()
                ownedAppsById.clear()
                lastRevision = 0L
                return
            }

            val isInitial = !initialSnapshotEmitted
            if (!isInitial &&
                delta.revision == lastRevision &&
                delta.packages.isEmpty() &&
                delta.ownedApps.isEmpty() &&
                delta.removedOwnedAppIds.isEmpty()
            ) {
                return
            }

            delta.packages.forEach { packagesById[it.id] = it }
            delta.removedOwnedAppIds.forEach { ownedAppsById.remove(it) }
            delta.ownedApps.forEach { ownedAppsById[it.id] = it }
            allAppsCount = delta.allAppsCount
            ownedAppsCount = delta.ownedAppsCount
            lastRevision = delta.revision

            if (!updateChannel.trySend(delta).isSuccess) {
                Timber.tag(TAG).w("failed to enqueue library delta revision=%d", delta.revision)
            }

            // Preserve the old snapshot API without paying O(N) on every native
            // callback when nobody consumes it. Always publish the initial baseline
            // so a late snapshot subscriber still receives replay=1.
            if (isInitial || _snapshots.subscriptionCount.value > 0) {
                _snapshots.tryEmit(snapshotLocked())
                initialSnapshotEmitted = true
            }
        }
    }

    private fun snapshotLocked(): WnLibrarySnapshot =
        WnLibrarySnapshot(
            packages = packagesById.values.toList(),
            ownedApps = ownedAppsById.values.toList(),
            allAppsCount = allAppsCount,
            ownedAppsCount = ownedAppsCount,
        )

    companion object {
        private const val TAG = "WnLibraryStore"

        @JvmStatic
        fun parseDelta(json: String): WnLibraryDelta {
            if (json.isBlank() || json == "{}") {
                return WnLibraryDelta(0L, emptyList(), emptyList(), emptyList(), 0, 0)
            }
            val root = JSONObject(json)
            return WnLibraryDelta(
                revision = root.optLong("revision", 0L),
                packages = parsePackages(root.optJSONArray("packages")),
                ownedApps = parseOwnedApps(root.optJSONArray("owned_apps")),
                removedOwnedAppIds = root.optJSONArray("removed_owned_app_ids").toIntList(),
                allAppsCount = root.optInt("all_apps_count"),
                ownedAppsCount = root.optInt("owned_apps_count"),
            )
        }

        @JvmStatic
        fun parseSnapshot(json: String): WnLibrarySnapshot {
            if (json.isBlank() || json == "{}") return WnLibrarySnapshot.EMPTY
            val root = JSONObject(json)
            return WnLibrarySnapshot(
                packages = parsePackages(root.optJSONArray("packages")),
                ownedApps = parseOwnedApps(root.optJSONArray("owned_apps")),
                allAppsCount = root.optInt("all_apps_count"),
                ownedAppsCount = root.optInt("owned_apps_count"),
            )
        }

        private fun parsePackages(arr: JSONArray?): List<WnOwnedPackage> {
            if (arr == null) return emptyList()
            return List(arr.length()) { i ->
                val o = arr.getJSONObject(i)
                WnOwnedPackage(
                    id = o.getInt("id"),
                    licenseFlags = o.optInt("flags"),
                    licenseType = o.optInt("license_type"),
                    changeNumber = o.optInt("change_number"),
                    accessToken = o.optString("access_token", "0"),
                )
            }
        }

        private fun parseOwnedApps(arr: JSONArray?): List<WnOwnedApp> {
            if (arr == null) return emptyList()
            return List(arr.length()) { i ->
                val o = arr.getJSONObject(i)
                WnOwnedApp(
                    id = o.getInt("id"),
                    name = o.optString("name"),
                    type = o.optString("type"),
                    sortAs = o.optString("sort_as"),
                    osList = o.optString("os_list"),
                    parentAppId = o.optInt("parent"),
                    changeNumber = o.optInt("change_number"),
                    accessToken = o.optString("access_token", "0"),
                    dlcAppIds = o.optJSONArray("dlc").toIntList(),
                    sourcePackageIds = o.optJSONArray("src_packages").toIntList(),
                    buildId = o.optInt("build_id", 0),
                )
            }
        }

        private fun JSONArray?.toIntList(): List<Int> {
            if (this == null) return emptyList()
            return List(length()) { i -> getInt(i) }
        }
    }
}
