use crate::pb::cmsg_client_license_list::CMsgClientLicenseList;
use crate::pb::cmsg_client_pics::{
    CMsgClientPICSAccessTokenResponse, CMsgClientPICSProductInfoResponse, PicsAppInfoReq,
    PicsPackageInfoReq,
};
use crate::vdf::{self, KVNode};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OwnedPackage {
    pub package_id: u32,
    pub access_token: u64,
    pub change_number: i32,
    pub license_flags: u32,
    pub license_type: u32,
    pub pics_fetched: bool,
    pub app_ids: Vec<u32>,
    pub depot_ids: Vec<u32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OwnedApp {
    pub app_id: u32,
    pub change_number: u32,
    pub name: String,
    pub sort_as: String,
    pub app_type: String,
    pub os_list: String,
    pub parent_app_id: u32,
    pub dlc_app_ids: Vec<u32>,
    pub build_id: u32,
    pub source_package_ids: Vec<u32>,
    pub pics_fetched: bool,
    pub missing_token: bool,
    pub access_token: u64,
}

type SnapshotObserver = Arc<dyn Fn() + Send + Sync + 'static>;

#[derive(Default)]
struct LibraryRevisionChange {
    package_ids: Vec<u32>,
    app_ids: Vec<u32>,
}

#[derive(Default)]
struct LibraryRevisionState {
    revision: u64,
    changes: Vec<LibraryRevisionChange>,
    owned_apps_count: usize,
}

#[derive(Default)]
pub struct WnLibraryStore {
    packages: Mutex<HashMap<u32, OwnedPackage>>,
    apps: Mutex<HashMap<u32, OwnedApp>>,
    revisions: Mutex<LibraryRevisionState>,
    observer: Mutex<Option<SnapshotObserver>>,
}

impl WnLibraryStore {
    pub fn ingest_license_list(&self, msg: &CMsgClientLicenseList) {
        let changed_packages = msg
            .licenses
            .iter()
            .map(|license| license.package_id)
            .collect::<Vec<_>>();
        {
            let mut packages = self.packages.lock().expect("library packages poisoned");
            for license in &msg.licenses {
                let slot = packages.entry(license.package_id).or_default();
                slot.package_id = license.package_id;
                if license.access_token != 0 {
                    slot.access_token = license.access_token;
                }
                if license.change_number > slot.change_number {
                    slot.change_number = license.change_number;
                }
                slot.license_flags = license.flags;
                slot.license_type = license.license_type;
            }
        }
        if !changed_packages.is_empty() {
            self.mark_changed(&changed_packages, &[], 0);
            self.notify();
        }
    }

    pub fn get_pending_package_pics_request(&self, max_count: usize) -> Vec<PicsPackageInfoReq> {
        let packages = self.packages.lock().expect("library packages poisoned");
        packages
            .values()
            .filter(|p| !p.pics_fetched)
            .take(max_count)
            .map(|p| PicsPackageInfoReq {
                packageid: p.package_id,
                access_token: p.access_token,
            })
            .collect()
    }

    pub fn get_pending_app_pics_request(&self, max_count: usize) -> Vec<PicsAppInfoReq> {
        let apps = self.apps.lock().expect("library apps poisoned");
        apps.values()
            .filter(|a| !a.pics_fetched && (!a.missing_token || a.access_token != 0))
            .take(max_count)
            .map(|a| PicsAppInfoReq {
                appid: a.app_id,
                access_token: a.access_token,
                only_public_obsolete: false,
            })
            .collect()
    }

    pub fn get_apps_needing_access_token(&self) -> Vec<u32> {
        let apps = self.apps.lock().expect("library apps poisoned");
        apps.values()
            .filter(|a| a.missing_token && a.access_token == 0)
            .map(|a| a.app_id)
            .collect()
    }

    pub fn ingest_package_pics_response(&self, resp: &CMsgClientPICSProductInfoResponse) {
        let mut changed_packages = Vec::new();
        let mut changed_apps = Vec::new();
        let mut newly_owned_apps = 0usize;
        {
            let mut packages = self.packages.lock().expect("library packages poisoned");
            let mut apps = self.apps.lock().expect("library apps poisoned");
            for package in &resp.packages {
                changed_packages.push(package.packageid);
                let slot = packages.entry(package.packageid).or_default();
                slot.package_id = package.packageid;
                slot.change_number = package.change_number as i32;
                slot.pics_fetched = true;
                if !package.buffer.is_empty() {
                    if let Some((_prefix, root)) = vdf::parse_binary_package(&package.buffer) {
                        extract_uint32_array(root.child("appids"), &mut slot.app_ids);
                        extract_uint32_array(root.child("depotids"), &mut slot.depot_ids);
                        for app_id in &slot.app_ids {
                            changed_apps.push(*app_id);
                            let app = apps.entry(*app_id).or_default();
                            app.app_id = *app_id;
                            if !app.source_package_ids.contains(&package.packageid) {
                                if app.source_package_ids.is_empty() {
                                    newly_owned_apps += 1;
                                }
                                app.source_package_ids.push(package.packageid);
                            }
                        }
                    }
                }
            }
            for package_id in &resp.unknown_packageids {
                if let Some(package) = packages.get_mut(package_id) {
                    package.pics_fetched = true;
                    changed_packages.push(*package_id);
                }
            }
        }
        if !changed_packages.is_empty() || !changed_apps.is_empty() {
            self.mark_changed(&changed_packages, &changed_apps, newly_owned_apps);
            self.notify();
        }
    }

    pub fn ingest_app_pics_response(&self, resp: &CMsgClientPICSProductInfoResponse) {
        let mut changed_apps = Vec::new();
        {
            let mut apps = self.apps.lock().expect("library apps poisoned");
            for app_resp in &resp.apps {
                changed_apps.push(app_resp.appid);
                let app = apps.entry(app_resp.appid).or_default();
                app.app_id = app_resp.appid;
                app.change_number = app_resp.change_number;
                app.pics_fetched = true;
                if app_resp.missing_token {
                    app.missing_token = true;
                    app.pics_fetched = false;
                    continue;
                }
                app.missing_token = false;
                if app_resp.buffer.is_empty() {
                    continue;
                }
                let Some(root) = vdf::parse_auto(&app_resp.buffer) else {
                    continue;
                };
                let appinfo = if root.name.eq_ignore_ascii_case("appinfo") {
                    &root
                } else {
                    root.child("appinfo").unwrap_or(&root)
                };
                if let Some(common) = appinfo.child("common") {
                    set_string(common, "name", &mut app.name);
                    set_string(common, "sortas", &mut app.sort_as);
                    set_string(common, "type", &mut app.app_type);
                    set_string(common, "oslist", &mut app.os_list);
                    if let Some(parent) = common.child("parent") {
                        app.parent_app_id = parent.as_uint(0) as u32;
                    }
                }
                if let Some(list) = appinfo
                    .child("extended")
                    .and_then(|extended| extended.child("listofdlc"))
                {
                    app.dlc_app_ids.clear();
                    parse_csv_appids(&list.as_string(""), &mut app.dlc_app_ids);
                }
                if let Some(buildid) = appinfo
                    .child("depots")
                    .and_then(|depots| depots.child("branches"))
                    .and_then(|branches| branches.child("public"))
                    .and_then(|public| public.child("buildid"))
                {
                    app.build_id = buildid.as_uint(0) as u32;
                }
                let child_id = app.app_id;
                let parent_id = app.parent_app_id;
                if parent_id != 0 {
                    changed_apps.push(parent_id);
                    let parent = apps.entry(parent_id).or_default();
                    parent.app_id = parent_id;
                    if !parent.dlc_app_ids.contains(&child_id) {
                        parent.dlc_app_ids.push(child_id);
                    }
                }
            }
            for app_id in &resp.unknown_appids {
                if let Some(app) = apps.get_mut(app_id) {
                    app.pics_fetched = true;
                    changed_apps.push(*app_id);
                }
            }
        }
        if !changed_apps.is_empty() {
            self.mark_changed(&[], &changed_apps, 0);
            self.notify();
        }
    }

    pub fn ingest_app_access_tokens(&self, resp: &CMsgClientPICSAccessTokenResponse) {
        let mut changed_apps = Vec::new();
        {
            let mut apps = self.apps.lock().expect("library apps poisoned");
            for token in &resp.app_access_tokens {
                changed_apps.push(token.appid);
                let app = apps.entry(token.appid).or_default();
                app.app_id = token.appid;
                app.access_token = token.access_token;
                app.missing_token = false;
            }
            for app_id in &resp.app_denied_tokens {
                if let Some(app) = apps.get_mut(app_id) {
                    app.pics_fetched = true;
                    app.missing_token = false;
                    changed_apps.push(*app_id);
                }
            }
        }
        if !changed_apps.is_empty() {
            self.mark_changed(&[], &changed_apps, 0);
            self.notify();
        }
    }

    pub fn packages(&self) -> Vec<OwnedPackage> {
        self.packages
            .lock()
            .expect("library packages poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn apps(&self) -> Vec<OwnedApp> {
        self.apps
            .lock()
            .expect("library apps poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn owned_apps(&self) -> Vec<OwnedApp> {
        self.apps()
            .into_iter()
            .filter(|app| !app.source_package_ids.is_empty())
            .collect()
    }

    pub fn find_app(&self, app_id: u32) -> Option<OwnedApp> {
        self.apps
            .lock()
            .expect("library apps poisoned")
            .get(&app_id)
            .cloned()
    }

    pub fn package_count(&self) -> usize {
        self.packages
            .lock()
            .expect("library packages poisoned")
            .len()
    }

    pub fn app_count(&self) -> usize {
        self.apps.lock().expect("library apps poisoned").len()
    }

    pub fn owned_app_count(&self) -> usize {
        self.revisions
            .lock()
            .expect("library revisions poisoned")
            .owned_apps_count
    }

    fn mark_changed(
        &self,
        package_ids: &[u32],
        app_ids: &[u32],
        newly_owned_apps: usize,
    ) {
        if package_ids.is_empty() && app_ids.is_empty() {
            return;
        }
        let mut state = self.revisions.lock().expect("library revisions poisoned");
        state.revision = state.revision.saturating_add(1);
        state.owned_apps_count = state.owned_apps_count.saturating_add(newly_owned_apps);
        state.changes.push(LibraryRevisionChange {
            package_ids: package_ids.to_vec(),
            app_ids: app_ids.to_vec(),
        });
    }

    pub fn delta_json(&self, since_revision: u64) -> String {
        let (revision, mut package_ids, mut app_ids, owned_apps_count) = {
            let state = self.revisions.lock().expect("library revisions poisoned");
            let start = usize::try_from(since_revision)
                .unwrap_or(usize::MAX)
                .min(state.changes.len());
            let mut package_ids = Vec::new();
            let mut app_ids = Vec::new();
            for change in &state.changes[start..] {
                package_ids.extend_from_slice(&change.package_ids);
                app_ids.extend_from_slice(&change.app_ids);
            }
            (
                state.revision,
                package_ids,
                app_ids,
                state.owned_apps_count,
            )
        };

        // A revision of zero means the caller has no baseline. Include all current
        // entries even if they predate revision tracking, preserving a full initial sync.
        if since_revision == 0 {
            package_ids = self
                .packages
                .lock()
                .expect("library packages poisoned")
                .keys()
                .copied()
                .collect();
            app_ids = self
                .apps
                .lock()
                .expect("library apps poisoned")
                .keys()
                .copied()
                .collect();
        }
        package_ids.sort_unstable();
        package_ids.dedup();
        app_ids.sort_unstable();
        app_ids.dedup();

        let packages = {
            let packages = self.packages.lock().expect("library packages poisoned");
            package_ids
                .iter()
                .filter_map(|id| packages.get(id).cloned())
                .collect::<Vec<_>>()
        };
        let (owned_apps, removed_owned_app_ids, all_apps_count) = {
            let apps = self.apps.lock().expect("library apps poisoned");
            let owned_apps = app_ids
                .iter()
                .filter_map(|id| apps.get(id))
                .filter(|app| !app.source_package_ids.is_empty())
                .cloned()
                .collect::<Vec<_>>();
            let removed_owned_app_ids = app_ids
                .iter()
                .filter(|id| {
                    apps.get(*id)
                        .map(|app| app.source_package_ids.is_empty())
                        .unwrap_or(true)
                })
                .copied()
                .collect::<Vec<_>>();
            (owned_apps, removed_owned_app_ids, apps.len())
        };

        json!({
            "revision": revision,
            "packages": packages.iter().map(|p| json!({
                "id": p.package_id,
                "flags": p.license_flags,
                "license_type": p.license_type,
                "change_number": p.change_number,
                "access_token": p.access_token.to_string(),
                "pics_fetched": p.pics_fetched,
            })).collect::<Vec<_>>(),
            "owned_apps": owned_apps.iter().map(|a| json!({
                "id": a.app_id,
                "change_number": a.change_number,
                "name": a.name,
                "type": a.app_type,
                "sort_as": a.sort_as,
                "os_list": a.os_list,
                "parent": a.parent_app_id,
                "access_token": a.access_token.to_string(),
                "build_id": a.build_id,
                "dlc": a.dlc_app_ids,
                "src_packages": a.source_package_ids,
                "pics_fetched": a.pics_fetched,
            })).collect::<Vec<_>>(),
            "removed_owned_app_ids": removed_owned_app_ids,
            "all_apps_count": all_apps_count,
            "owned_apps_count": owned_apps_count,
        })
        .to_string()
    }

    pub fn snapshot_json(&self) -> String {
        let packages = self.packages();
        let apps = self.apps();
        let owned: Vec<_> = apps
            .iter()
            .filter(|app| !app.source_package_ids.is_empty())
            .collect();
        json!({
            "packages": packages.iter().map(|p| json!({
                "id": p.package_id,
                "flags": p.license_flags,
                "license_type": p.license_type,
                "change_number": p.change_number,
                "access_token": p.access_token.to_string(),
                "pics_fetched": p.pics_fetched,
            })).collect::<Vec<_>>(),
            "owned_apps": owned.iter().map(|a| json!({
                "id": a.app_id,
                "change_number": a.change_number,
                "name": a.name,
                "type": a.app_type,
                "sort_as": a.sort_as,
                "os_list": a.os_list,
                "parent": a.parent_app_id,
                "access_token": a.access_token.to_string(),
                "build_id": a.build_id,
                "dlc": a.dlc_app_ids,
                "src_packages": a.source_package_ids,
                "pics_fetched": a.pics_fetched,
            })).collect::<Vec<_>>(),
            "all_apps_count": apps.len(),
            "owned_apps_count": owned.len(),
        })
        .to_string()
    }

    pub fn set_observer<F>(&self, observer: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.observer.lock().expect("library observer poisoned") = Some(Arc::new(observer));
    }

    fn notify(&self) {
        let cb = self
            .observer
            .lock()
            .expect("library observer poisoned")
            .clone();
        if let Some(cb) = cb {
            cb();
        }
    }
}

fn extract_uint32_array(parent: Option<&KVNode>, out: &mut Vec<u32>) {
    if let Some(parent) = parent {
        for child in &parent.children {
            let value = child.as_uint(0);
            if value != 0 {
                out.push(value as u32);
            }
        }
    }
}

fn parse_csv_appids(csv: &str, out: &mut Vec<u32>) {
    for part in csv.split(',') {
        if let Ok(value) = part.trim().parse::<u32>() {
            if value != 0 {
                out.push(value);
            }
        }
    }
}

fn set_string(parent: &KVNode, key: &str, out: &mut String) {
    if let Some(node) = parent.child(key) {
        *out = node.as_string(out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::cmsg_client_license_list::{CMsgClientLicenseList, License};
    use crate::pb::cmsg_client_pics::{PicsAppInfoResp, PicsPackageInfoResp};

    #[test]
    fn ingests_license_and_emits_pending_package_request() {
        let store = WnLibraryStore::default();
        store.ingest_license_list(&CMsgClientLicenseList {
            eresult: 1,
            licenses: vec![License {
                package_id: 100,
                access_token: 55,
                change_number: 7,
                ..Default::default()
            }],
        });
        let pending = store.get_pending_package_pics_request(10);
        assert_eq!(pending[0].packageid, 100);
        assert_eq!(pending[0].access_token, 55);
    }

    #[test]
    fn ingests_text_app_pics_and_links_parent_dlc() {
        let store = WnLibraryStore::default();
        let text = br#""appinfo" {
            "common" { "name" "DLC" "type" "DLC" "parent" "480" }
            "extended" { "listofdlc" "481,482" }
            "depots" { "branches" { "public" { "buildid" "99" } } }
        }"#;
        store.ingest_app_pics_response(&CMsgClientPICSProductInfoResponse {
            apps: vec![PicsAppInfoResp {
                appid: 481,
                buffer: text.to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        });
        let app = store.find_app(481).unwrap();
        assert_eq!(app.name, "DLC");
        assert_eq!(app.parent_app_id, 480);
        assert_eq!(app.build_id, 99);
        assert!(store.find_app(480).unwrap().dlc_app_ids.contains(&481));
    }

    #[test]
    fn delta_only_returns_entities_changed_after_revision() {
        let store = WnLibraryStore::default();
        store.ingest_license_list(&CMsgClientLicenseList {
            eresult: 1,
            licenses: vec![License {
                package_id: 100,
                access_token: 55,
                change_number: 7,
                ..Default::default()
            }],
        });
        let first: serde_json::Value = serde_json::from_str(&store.delta_json(0)).unwrap();
        let revision = first["revision"].as_u64().unwrap();
        assert_eq!(first["packages"].as_array().unwrap().len(), 1);

        let unchanged: serde_json::Value =
            serde_json::from_str(&store.delta_json(revision)).unwrap();
        assert!(unchanged["packages"].as_array().unwrap().is_empty());
        assert!(unchanged["owned_apps"].as_array().unwrap().is_empty());

        store.ingest_app_access_tokens(&CMsgClientPICSAccessTokenResponse {
            app_access_tokens: vec![crate::pb::cmsg_client_pics::PicsAppToken {
                appid: 480,
                access_token: 99,
            }],
            ..Default::default()
        });
        let changed: serde_json::Value =
            serde_json::from_str(&store.delta_json(revision)).unwrap();
        assert_eq!(changed["removed_owned_app_ids"][0].as_u64(), Some(480));
    }

    #[test]
    fn marks_unknown_packages_as_fetched() {
        let store = WnLibraryStore::default();
        store.ingest_license_list(&CMsgClientLicenseList {
            eresult: 1,
            licenses: vec![License {
                package_id: 100,
                ..Default::default()
            }],
        });
        store.ingest_package_pics_response(&CMsgClientPICSProductInfoResponse {
            packages: vec![PicsPackageInfoResp {
                packageid: 101,
                change_number: 1,
                ..Default::default()
            }],
            unknown_packageids: vec![100],
            ..Default::default()
        });
        assert!(
            store
                .packages()
                .into_iter()
                .find(|p| p.package_id == 100)
                .unwrap()
                .pics_fetched
        );
    }
}
