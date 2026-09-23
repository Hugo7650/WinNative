package com.winlator.cmod.feature.stores.steam.wnsteam

/**
 * Mirror of the native `OwnedPackage` struct — one of the user's Steam
 * licenses (subscription/package). [accessToken] is encoded as decimal
 * string since it's a uint64 and JSON has no native unsigned 64-bit type.
 */
data class WnOwnedPackage(
    val id: Int,
    val licenseFlags: Int,
    val licenseType: Int,
    val changeNumber: Int,
    val accessToken: String,
    val picsFetched: Boolean = false,
)

/**
 * Mirror of the native `OwnedApp` struct. Only apps with at least one
 * source package are included in [WnLibrarySnapshot.ownedApps]; parent
 * stubs (apps known only because the user owns DLC of them) are
 * excluded.
 */
data class WnOwnedApp(
    val id: Int,
    val name: String,
    val type: String,
    val sortAs: String,
    val osList: String,
    val parentAppId: Int,
    val changeNumber: Int,
    val accessToken: String,
    val dlcAppIds: List<Int>,
    val sourcePackageIds: List<Int>,
    val buildId: Int = 0,
    val picsFetched: Boolean = false,
)

data class WnLibrarySyncProgress(
    val fetchedPackages: Int = 0,
    val totalPackages: Int = 0,
    val fetchedOwnedApps: Int = 0,
    val totalOwnedApps: Int = 0,
) {
    val packageDiscoveryComplete: Boolean
        get() = totalPackages > 0 && fetchedPackages >= totalPackages

    val fraction: Float
        get() =
            when {
                totalPackages > 0 && !packageDiscoveryComplete ->
                    fetchedPackages.toFloat() / totalPackages.toFloat()
                totalOwnedApps > 0 ->
                    fetchedOwnedApps.toFloat() / totalOwnedApps.toFloat()
                packageDiscoveryComplete -> 1f
                else -> 0f
            }.coerceIn(0f, 1f)

    companion object {
        val EMPTY = WnLibrarySyncProgress()
    }
}

/** Incremental native-library update since a caller-provided revision. */
data class WnLibraryDelta(
    val revision: Long,
    val packages: List<WnOwnedPackage>,
    val ownedApps: List<WnOwnedApp>,
    val removedOwnedAppIds: List<Int>,
    val allAppsCount: Int,
    val ownedAppsCount: Int,
)

/** Full snapshot of the native library store. */
data class WnLibrarySnapshot(
    val packages: List<WnOwnedPackage>,
    val ownedApps: List<WnOwnedApp>,
    val allAppsCount: Int,
    val ownedAppsCount: Int,
) {
    companion object {
        val EMPTY = WnLibrarySnapshot(emptyList(), emptyList(), 0, 0)
    }
}
