//! Integration tests for `views.rs` (WP5, CONTRACT.md "Views").

use std::path::{Path, PathBuf};

use ppg3_core::manifest::BuiltInfo;
use ppg3_core::store::Store;
use ppg3_core::storeset::StoreSet;
use ppg3_core::views::{self, VcsInfo, ViewEntry, ViewSpec};

fn built() -> BuiltInfo {
    BuiltInfo {
        start_ms: 0,
        end_ms: 1,
        host: "test-host".to_string(),
        sandboxed: false,
        ppg3_version: "0.1.0".to_string(),
        retain_evict: false,
    }
}

/// Publish a single-file entry `{file_name: contents}` into `store`, keyed
/// by a fresh unique `ik`. Returns `(ik, oh)`.
fn publish_one(store: &Store, ik_seed: &str, file_name: &str, contents: &[u8]) -> String {
    let ik = format!("{:0<64}", ik_seed);
    let staging = store.open_staging().unwrap();
    std::fs::write(staging.path().join(file_name), contents).unwrap();
    let outcome = store
        .publish(
            staging,
            &ik,
            &serde_json::json!({"ppg3_key_version": 1, "seed": ik_seed}),
            built(),
            None,
        )
        .unwrap();
    outcome.oh().to_string()
}

fn roots_dir_entries(store_root: &Path, project_id: &str, generation: u64) -> Vec<String> {
    let dir = store_root
        .join("v1")
        .join("roots")
        .join(project_id)
        .join(generation.to_string());
    if !dir.is_dir() {
        return Vec::new();
    }
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    names
}

#[test]
fn generation_numbering_starts_at_one_and_increments() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let oh1 = publish_one(&store, "gen1", "a.txt", b"hello");
    let oh2 = publish_one(&store, "gen2", "a.txt", b"world");

    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let spec1 = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh: oh1,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    let n1 = views::write_generation(&project_dir, "proj", &stores, &spec1, false).unwrap();
    assert_eq!(n1, 1);

    let spec2 = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh: oh2,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    let n2 = views::write_generation(&project_dir, "proj", &stores, &spec2, false).unwrap();
    assert_eq!(n2, 2);
}

#[test]
fn view_symlinks_resolve_to_real_content() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let oh = publish_one(&store, "resolve", "result.tsv", b"1\t2\t3\n");

    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let spec = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "results/out.tsv".to_string(),
            oh,
            path_within_entry: "result.tsv".to_string(),
            store_index: 0,
        }],
    };
    let n = views::write_generation(&project_dir, "proj", &stores, &spec, false).unwrap();

    let view_path = project_dir
        .join("views")
        .join(n.to_string())
        .join("results/out.tsv");
    assert!(
        view_path.exists(),
        "symlink target must resolve: {view_path:?}"
    );
    let contents = std::fs::read_to_string(&view_path).unwrap();
    assert_eq!(contents, "1\t2\t3\n");

    // The `current` symlink must resolve the same way.
    let current_path = project_dir.join("views/current/results/out.tsv");
    assert_eq!(std::fs::read_to_string(&current_path).unwrap(), "1\t2\t3\n");

    // outputs/ (sibling of .ppg3) must resolve too.
    let outputs_path = project.path().join("outputs/results/out.tsv");
    assert_eq!(std::fs::read_to_string(&outputs_path).unwrap(), "1\t2\t3\n");
}

#[test]
fn atomic_current_swap_leaves_old_generation_links_intact() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let oh1 = publish_one(&store, "swap1", "a.txt", b"first");
    let oh2 = publish_one(&store, "swap2", "a.txt", b"second");

    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let spec1 = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh: oh1,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    let n1 = views::write_generation(&project_dir, "proj", &stores, &spec1, false).unwrap();

    let spec2 = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh: oh2,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    let n2 = views::write_generation(&project_dir, "proj", &stores, &spec2, false).unwrap();

    // Old generation's own files are untouched.
    let old_path = project_dir
        .join("views")
        .join(n1.to_string())
        .join("out.txt");
    assert_eq!(std::fs::read_to_string(&old_path).unwrap(), "first");

    // current now points at the new generation.
    let current_target = std::fs::read_link(project_dir.join("views/current")).unwrap();
    assert_eq!(current_target, PathBuf::from(n2.to_string()));
    let current_out = std::fs::read_to_string(project_dir.join("views/current/out.txt")).unwrap();
    assert_eq!(current_out, "second");
}

#[test]
fn rollback_repoints_current() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let oh1 = publish_one(&store, "rb1", "a.txt", b"one");
    let oh2 = publish_one(&store, "rb2", "a.txt", b"two");

    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let mk_spec = |oh: String| ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    let n1 = views::write_generation(&project_dir, "proj", &stores, &mk_spec(oh1), false).unwrap();
    let _n2 = views::write_generation(&project_dir, "proj", &stores, &mk_spec(oh2), false).unwrap();

    assert_eq!(
        std::fs::read_to_string(project_dir.join("views/current/out.txt")).unwrap(),
        "two"
    );

    views::rollback(&project_dir, n1).unwrap();
    assert_eq!(
        std::fs::read_to_string(project_dir.join("views/current/out.txt")).unwrap(),
        "one"
    );

    let gens = views::list_generations(&project_dir).unwrap();
    let cur = gens.iter().find(|g| g.current).unwrap();
    assert_eq!(cur.n, n1);
}

#[test]
fn rollback_to_missing_generation_errors() {
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");
    std::fs::create_dir_all(project_dir.join("views")).unwrap();
    assert!(views::rollback(&project_dir, 999).is_err());
}

#[test]
fn roots_registered_in_every_store_a_spec_links_into() {
    let store1_dir = tempfile::tempdir().unwrap();
    let store2_dir = tempfile::tempdir().unwrap();
    let store1 = Store::open("s1", store1_dir.path(), false).unwrap();
    let store2 = Store::open("s2", store2_dir.path(), false).unwrap();

    let oh1 = publish_one(&store1, "cross1", "a.txt", b"from store1");
    let oh2 = publish_one(&store2, "cross2", "b.txt", b"from store2");

    let stores = StoreSet::new(vec![store1, store2]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let spec = ViewSpec {
        entries: vec![
            ViewEntry {
                view_rel_path: "one.txt".to_string(),
                oh: oh1.clone(),
                path_within_entry: "a.txt".to_string(),
                store_index: 0,
            },
            ViewEntry {
                view_rel_path: "two.txt".to_string(),
                oh: oh2.clone(),
                path_within_entry: "b.txt".to_string(),
                store_index: 1,
            },
        ],
    };
    let n = views::write_generation(&project_dir, "cross-proj", &stores, &spec, false).unwrap();

    let roots1 = roots_dir_entries(store1_dir.path(), "cross-proj", n);
    let roots2 = roots_dir_entries(store2_dir.path(), "cross-proj", n);
    assert_eq!(roots1, vec![oh1]);
    assert_eq!(roots2, vec![oh2]);
}

#[test]
fn drop_generation_unregisters_roots_and_removes_dir() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let oh1 = publish_one(&store, "drop1", "a.txt", b"one");
    let oh2 = publish_one(&store, "drop2", "a.txt", b"two");

    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let mk_spec = |oh: String| ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    let n1 = views::write_generation(&project_dir, "proj", &stores, &mk_spec(oh1.clone()), false)
        .unwrap();
    let _n2 = views::write_generation(&project_dir, "proj", &stores, &mk_spec(oh2), false).unwrap();

    assert_eq!(roots_dir_entries(store_dir.path(), "proj", n1), vec![oh1]);
    assert!(project_dir.join("views").join(n1.to_string()).is_dir());

    views::drop_generation(&project_dir, &stores, n1).unwrap();

    assert!(roots_dir_entries(store_dir.path(), "proj", n1).is_empty());
    assert!(!project_dir.join("views").join(n1.to_string()).exists());
}

#[test]
fn drop_generation_refuses_to_drop_current() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let oh = publish_one(&store, "cur1", "a.txt", b"one");
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let spec = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    let n = views::write_generation(&project_dir, "proj", &stores, &spec, false).unwrap();
    assert!(views::drop_generation(&project_dir, &stores, n).is_err());
}

#[test]
fn keep_last_basic_drops_oldest_first() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let mut gens = Vec::new();
    for i in 0..5 {
        let oh = publish_one(
            &stores.stores[0],
            &format!("keep{i}"),
            "a.txt",
            format!("v{i}").as_bytes(),
        );
        let spec = ViewSpec {
            entries: vec![ViewEntry {
                view_rel_path: "out.txt".to_string(),
                oh,
                path_within_entry: "a.txt".to_string(),
                store_index: 0,
            }],
        };
        gens.push(views::write_generation(&project_dir, "proj", &stores, &spec, false).unwrap());
    }
    assert_eq!(gens, vec![1, 2, 3, 4, 5]);

    let dropped = views::keep_last(&project_dir, &stores, 2, false).unwrap();
    assert_eq!(dropped, vec![1, 2, 3]);

    let remaining: Vec<u64> = views::list_generations(&project_dir)
        .unwrap()
        .into_iter()
        .map(|g| g.n)
        .collect();
    assert_eq!(remaining, vec![4, 5]);
}

#[test]
fn keep_last_never_drops_current_even_if_older() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let mut gens = Vec::new();
    for i in 0..3 {
        let oh = publish_one(
            &stores.stores[0],
            &format!("cur{i}"),
            "a.txt",
            format!("v{i}").as_bytes(),
        );
        let spec = ViewSpec {
            entries: vec![ViewEntry {
                view_rel_path: "out.txt".to_string(),
                oh,
                path_within_entry: "a.txt".to_string(),
                store_index: 0,
            }],
        };
        gens.push(views::write_generation(&project_dir, "proj", &stores, &spec, false).unwrap());
    }
    // Roll back to the oldest generation, then ask to keep only the last 1
    // "logically newest" -- current (1) must survive even though it is the
    // numerically oldest.
    views::rollback(&project_dir, gens[0]).unwrap();
    let dropped = views::keep_last(&project_dir, &stores, 1, false).unwrap();
    assert!(!dropped.contains(&gens[0]));
    let remaining: Vec<u64> = views::list_generations(&project_dir)
        .unwrap()
        .into_iter()
        .map(|g| g.n)
        .collect();
    assert!(remaining.contains(&gens[0]));
}

#[test]
fn keep_last_ephemeral_rule_keeps_all_explicit_plus_n_ephemeral() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let mk = |i: usize, stores: &StoreSet| {
        let oh = publish_one(
            &stores.stores[0],
            &format!("eph{i}"),
            "a.txt",
            format!("v{i}").as_bytes(),
        );
        ViewSpec {
            entries: vec![ViewEntry {
                view_rel_path: "out.txt".to_string(),
                oh,
                path_within_entry: "a.txt".to_string(),
                store_index: 0,
            }],
        }
    };

    // explicit, ephemeral, ephemeral, ephemeral, explicit (current)
    let spec = mk(0, &stores);
    let g_explicit1 = views::write_generation(&project_dir, "proj", &stores, &spec, false).unwrap();
    let spec = mk(1, &stores);
    let g_eph1 = views::write_generation(&project_dir, "proj", &stores, &spec, true).unwrap();
    let spec = mk(2, &stores);
    let g_eph2 = views::write_generation(&project_dir, "proj", &stores, &spec, true).unwrap();
    let spec = mk(3, &stores);
    let g_eph3 = views::write_generation(&project_dir, "proj", &stores, &spec, true).unwrap();
    let spec = mk(4, &stores);
    let g_explicit2 = views::write_generation(&project_dir, "proj", &stores, &spec, false).unwrap();

    // keep last 1 ephemeral + all explicit.
    let dropped = views::keep_last(&project_dir, &stores, 1, true).unwrap();
    assert_eq!(dropped, vec![g_eph1, g_eph2]);

    let remaining: Vec<u64> = views::list_generations(&project_dir)
        .unwrap()
        .into_iter()
        .map(|g| g.n)
        .collect();
    assert!(remaining.contains(&g_explicit1));
    assert!(remaining.contains(&g_explicit2));
    assert!(remaining.contains(&g_eph3));
    assert!(!remaining.contains(&g_eph1));
    assert!(!remaining.contains(&g_eph2));
}

#[test]
fn ephemeral_marker_and_list_generations_report_it() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let oh = publish_one(&stores.stores[0], "ephmark", "a.txt", b"x");
    let spec = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    let n = views::write_generation(&project_dir, "proj", &stores, &spec, true).unwrap();
    assert!(project_dir
        .join("views")
        .join(n.to_string())
        .join(".ephemeral")
        .is_file());

    let gens = views::list_generations(&project_dir).unwrap();
    let g = gens.iter().find(|g| g.n == n).unwrap();
    assert!(g.ephemeral);
    assert_eq!(g.n_entries, 1);
}

#[test]
fn outputs_symlink_created_when_missing() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let oh = publish_one(&stores.stores[0], "outputs1", "a.txt", b"content");
    let spec = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    views::write_generation(&project_dir, "proj", &stores, &spec, false).unwrap();

    let outputs_path = project.path().join("outputs");
    let meta = std::fs::symlink_metadata(&outputs_path).unwrap();
    assert!(meta.file_type().is_symlink());
}

#[test]
fn outputs_symlink_does_not_clobber_pre_existing_real_directory() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    // A real directory named `outputs` pre-exists, with a sentinel file in
    // it that must survive.
    let outputs_path = project.path().join("outputs");
    std::fs::create_dir_all(&outputs_path).unwrap();
    std::fs::write(outputs_path.join("keep-me.txt"), b"do not delete").unwrap();

    let oh = publish_one(&stores.stores[0], "outputs2", "a.txt", b"content");
    let spec = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    views::write_generation(&project_dir, "proj", &stores, &spec, false).unwrap();

    let meta = std::fs::symlink_metadata(&outputs_path).unwrap();
    assert!(meta.is_dir() && !meta.file_type().is_symlink());
    assert_eq!(
        std::fs::read_to_string(outputs_path.join("keep-me.txt")).unwrap(),
        "do not delete"
    );
}

fn sample_vcs(committed: bool) -> VcsInfo {
    VcsInfo {
        backend: "jj".to_string(),
        commit_id: "c".repeat(40),
        change_id: "z".repeat(32),
        op_id: "0".repeat(64),
        committed,
        parent_commit_id: if committed {
            Some("p".repeat(40))
        } else {
            None
        },
    }
}

/// Write one single-entry generation, optionally with VCS info.
fn write_gen(
    project_dir: &Path,
    stores: &StoreSet,
    seed: &str,
    ephemeral: bool,
    vcs: Option<VcsInfo>,
) -> u64 {
    let oh = publish_one(&stores.stores[0], seed, "a.txt", seed.as_bytes());
    let spec = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh,
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    views::write_generation_with_vcs(project_dir, "proj", stores, &spec, ephemeral, vcs).unwrap()
}

#[test]
fn vcs_info_roundtrips_through_meta_and_list() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let n1 = write_gen(&project_dir, &stores, "vcs1", false, Some(sample_vcs(true)));
    let n2 = write_gen(&project_dir, &stores, "vcs2", false, None);

    let meta = views::read_generation_meta(&project_dir, n1).unwrap();
    let vcs = meta.vcs.expect("vcs info must persist");
    assert_eq!(vcs.backend, "jj");
    assert!(vcs.committed);
    assert_eq!(
        vcs.parent_commit_id.as_deref(),
        Some("p".repeat(40).as_str())
    );

    let gens = views::list_generations(&project_dir).unwrap();
    assert!(gens.iter().find(|g| g.n == n1).unwrap().vcs.is_some());
    assert!(gens.iter().find(|g| g.n == n2).unwrap().vcs.is_none());

    // meta.json without a vcs key (pre-VCS generations) must not serialize
    // a `"vcs"` field at all, and must still parse.
    let raw = std::fs::read_to_string(
        project_dir
            .join("views")
            .join(n2.to_string())
            .join("meta.json"),
    )
    .unwrap();
    assert!(!raw.contains("\"vcs\""));
}

#[test]
fn remove_old_generations_splits_committed_and_oplog_budgets() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    // committed, committed, committed, oplog, oplog, oplog, committed (current)
    let c1 = write_gen(&project_dir, &stores, "sc1", false, Some(sample_vcs(true)));
    let c2 = write_gen(&project_dir, &stores, "sc2", false, Some(sample_vcs(true)));
    let c3 = write_gen(&project_dir, &stores, "sc3", false, Some(sample_vcs(true)));
    let o1 = write_gen(&project_dir, &stores, "so1", false, Some(sample_vcs(false)));
    let o2 = write_gen(&project_dir, &stores, "so2", false, Some(sample_vcs(false)));
    let o3 = write_gen(&project_dir, &stores, "so3", false, Some(sample_vcs(false)));
    let cur = write_gen(&project_dir, &stores, "sc4", false, Some(sample_vcs(true)));

    // keep 2 committed + 1 op-log; current never counts against a budget.
    let report = views::remove_old_generations(&project_dir, &stores, 2, 1, false).unwrap();
    assert_eq!(report.dropped_committed, vec![c1]);
    assert_eq!(report.dropped_oplog, vec![o1, o2]);
    assert_eq!(report.kept, vec![c2, c3, o3, cur]);
    assert!(!report.dry_run);

    let remaining: Vec<u64> = views::list_generations(&project_dir)
        .unwrap()
        .into_iter()
        .map(|g| g.n)
        .collect();
    assert_eq!(remaining, vec![c2, c3, o3, cur]);
}

#[test]
fn remove_old_generations_treats_no_vcs_as_committed_and_ephemeral_as_oplog() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let novcs1 = write_gen(&project_dir, &stores, "nv1", false, None);
    let novcs2 = write_gen(&project_dir, &stores, "nv2", false, None);
    // ephemeral (watch mode) with a *committed* vcs state still lands in
    // the op-log bucket: ephemeral is transient by definition (§6.7).
    let eph = write_gen(&project_dir, &stores, "nv3", true, Some(sample_vcs(true)));
    let cur = write_gen(&project_dir, &stores, "nv4", false, None);

    let report = views::remove_old_generations(&project_dir, &stores, 1, 0, false).unwrap();
    assert_eq!(report.dropped_committed, vec![novcs1]);
    assert_eq!(report.dropped_oplog, vec![eph]);
    assert_eq!(report.kept, vec![novcs2, cur]);
}

#[test]
fn remove_old_generations_dry_run_drops_nothing() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let g1 = write_gen(
        &project_dir,
        &stores,
        "dry1",
        false,
        Some(sample_vcs(false)),
    );
    let _g2 = write_gen(&project_dir, &stores, "dry2", false, None);

    let report = views::remove_old_generations(&project_dir, &stores, 1, 0, true).unwrap();
    assert!(report.dry_run);
    assert_eq!(report.dropped_oplog, vec![g1]);

    let remaining: Vec<u64> = views::list_generations(&project_dir)
        .unwrap()
        .into_iter()
        .map(|g| g.n)
        .collect();
    assert_eq!(remaining.len(), 2, "dry run must not drop anything");
}

#[test]
fn generation_meta_json_has_expected_shape() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let oh = publish_one(&stores.stores[0], "meta1", "a.txt", b"content");
    let spec = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh: oh.clone(),
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    let n = views::write_generation(&project_dir, "myproj", &stores, &spec, false).unwrap();

    let meta = views::read_generation_meta(&project_dir, n).unwrap();
    assert_eq!(meta.project_id, "myproj");
    assert!(!meta.ephemeral);
    assert_eq!(meta.entries.len(), 1);
    assert_eq!(meta.entries[0].view_rel_path, "out.txt");
    assert_eq!(meta.entries[0].oh, oh);
    assert_eq!(meta.entries[0].store_name, "s");
}
