//! `ppg3 explain` / `ppg3 diff-entries` support (WP9, PPG3_DESIGN.md §9/§10.2,
//! CONTRACT.md "explain/diff").
//!
//! There is no global history file (§10.2): "explain" is entirely
//! reconstructed from data already on disk — a generation's `meta.json`
//! (views.rs) plus each entry's `manifest.json` (which embeds the full key
//! document, not just its hash — CONTRACT.md/§5's whole point).
//!
//! ## WP1 API friction (documented here + STATUS.md)
//!
//! `Store` has no public "read manifest by output hash" method — only
//! `lookup(ik)` (by *input* key, which also touches `.atime`). `entry_dir()`
//! and `data_dir()` are public, and `Manifest` is a public `Deserialize`
//! type, so this module reads `entries/<oh>/manifest.json` directly instead
//! of adding a method to `store.rs` (out of scope to modify per the WP5/WP9
//! brief). Likewise `Store` exposes no "list every entry" method for
//! `ppg3 store verify --sample`; `list_entries()` below walks
//! `store.entries_dir()` (a `pub(crate)` path helper, so usable from this
//! module without touching store.rs) instead.

use std::path::Path;

use serde::Serialize;
use serde_json::Value;

use crate::error::Error;
use crate::manifest::Manifest;
use crate::store::{self, Store};
use crate::storeset::StoreSet;
use crate::views;

const MANIFEST_FILE: &str = "manifest.json";

/// Added/removed/changed name sets for one key-document field (`inputs`,
/// `tools`, or `env`).
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct NameListDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

impl NameListDiff {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

/// Structured diff between two key documents (§5 shape). `a` is treated as
/// the "old"/previous document, `b` as "new"/current.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct KeyDocDiff {
    pub recipe_changed: bool,
    pub inputs: NameListDiff,
    pub tools: NameListDiff,
    pub env: NameListDiff,
    pub runtime_changed: bool,
    pub outputs_declared_changed: bool,
}

impl KeyDocDiff {
    pub fn is_empty(&self) -> bool {
        !self.recipe_changed
            && self.inputs.is_empty()
            && self.tools.is_empty()
            && self.env.is_empty()
            && !self.runtime_changed
            && !self.outputs_declared_changed
    }
}

fn diff_object_field(a: &Value, b: &Value, field: &str) -> NameListDiff {
    let empty = serde_json::Map::new();
    let oa = a.get(field).and_then(Value::as_object).unwrap_or(&empty);
    let ob = b.get(field).and_then(Value::as_object).unwrap_or(&empty);

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    for (k, v) in oa.iter() {
        match ob.get(k) {
            None => removed.push(k.clone()),
            Some(v2) if v2 != v => changed.push(k.clone()),
            Some(_) => {}
        }
    }
    for k in ob.keys() {
        if !oa.contains_key(k) {
            added.push(k.clone());
        }
    }
    added.sort();
    removed.sort();
    changed.sort();
    NameListDiff {
        added,
        removed,
        changed,
    }
}

fn as_sorted_string_list(v: Option<&Value>) -> Vec<String> {
    let mut out: Vec<String> = v
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// Structured diff between two key documents (CONTRACT.md `diff_key_documents`).
/// Pure/infallible: a missing field is simply treated as empty.
pub fn diff_key_documents(a: &Value, b: &Value) -> KeyDocDiff {
    KeyDocDiff {
        recipe_changed: a.get("job_recipe") != b.get("job_recipe"),
        inputs: diff_object_field(a, b, "inputs"),
        tools: diff_object_field(a, b, "tools"),
        env: diff_object_field(a, b, "env"),
        runtime_changed: a.get("runtime") != b.get("runtime"),
        outputs_declared_changed: as_sorted_string_list(a.get("outputs_declared"))
            != as_sorted_string_list(b.get("outputs_declared")),
    }
}

/// One changed file within `diff_entries`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChangedFile {
    pub path: String,
    pub size_a: u64,
    pub size_b: u64,
    /// `None` only if the files turned out byte-identical despite differing
    /// manifest metadata (e.g. mode-only change) — otherwise always `Some`.
    pub first_diff_offset: Option<u64>,
    pub blake3_a: String,
    pub blake3_b: String,
}

/// Per-file diff between two store entries (CONTRACT.md `diff_entries`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EntryDiff {
    pub oh_a: String,
    pub oh_b: String,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<ChangedFile>,
}

fn read_manifest_by_oh(store: &Store, oh: &str) -> Result<Manifest, Error> {
    let path = store.entry_dir(oh).join(MANIFEST_FILE);
    let bytes = std::fs::read(&path).map_err(|e| Error::io(&path, e))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| Error::CorruptStore(format!("unreadable manifest {path:?}: {e}")))
}

/// Stream-compare two files, returning the offset of the first differing
/// byte (`None` if identical). Reimplements `store.rs`'s private
/// `first_diff_offset` — that helper isn't exported and store.rs is out of
/// scope to modify for this work package (see module doc).
fn first_diff_offset(a: &Path, b: &Path) -> Result<Option<u64>, Error> {
    use std::io::Read;
    let mut fa = std::fs::File::open(a).map_err(|e| Error::io(a, e))?;
    let mut fb = std::fs::File::open(b).map_err(|e| Error::io(b, e))?;
    let mut ba = [0u8; 8192];
    let mut bb = [0u8; 8192];
    let mut offset: u64 = 0;
    loop {
        let na = fa.read(&mut ba).map_err(|e| Error::io(a, e))?;
        let nb = fb.read(&mut bb).map_err(|e| Error::io(b, e))?;
        let n = na.min(nb);
        for i in 0..n {
            if ba[i] != bb[i] {
                return Ok(Some(offset + i as u64));
            }
        }
        if na != nb {
            return Ok(Some(offset + n as u64));
        }
        if na == 0 {
            return Ok(None);
        }
        offset += n as u64;
    }
}

/// Per-file diff between two entries in the same store (CONTRACT.md
/// `diff_entries`). Deviates from CONTRACT.md's infallible-looking
/// signature by returning `Result` — reading manifests/files can fail (I/O,
/// missing entry) and every other WP1/WP9 API in this crate surfaces that
/// via `Result`; see STATUS.md.
pub fn diff_entries(store: &Store, oh_a: &str, oh_b: &str) -> Result<EntryDiff, Error> {
    let ma = read_manifest_by_oh(store, oh_a)?;
    let mb = read_manifest_by_oh(store, oh_b)?;

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    for (path, eb) in &mb.content {
        match ma.content.get(path) {
            None => added.push(path.clone()),
            Some(ea) if ea.blake3 != eb.blake3 || ea.size != eb.size || ea.mode != eb.mode => {
                let offset = if ea.blake3 != eb.blake3 {
                    first_diff_offset(
                        &store.data_dir(oh_a).join(path),
                        &store.data_dir(oh_b).join(path),
                    )?
                } else {
                    None
                };
                changed.push(ChangedFile {
                    path: path.clone(),
                    size_a: ea.size,
                    size_b: eb.size,
                    first_diff_offset: offset,
                    blake3_a: ea.blake3.clone(),
                    blake3_b: eb.blake3.clone(),
                });
            }
            Some(_) => {}
        }
    }
    for path in ma.content.keys() {
        if !mb.content.contains_key(path) {
            removed.push(path.clone());
        }
    }
    added.sort();
    removed.sort();
    changed.sort_by(|x, y| x.path.cmp(&y.path));

    Ok(EntryDiff {
        oh_a: oh_a.to_string(),
        oh_b: oh_b.to_string(),
        added,
        removed,
        changed,
    })
}

/// One step of the recursive "why did a parent input change" chain.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WhyStep {
    /// 1-based distance from the view path's own key document.
    pub depth: u32,
    /// The input name (in the *parent's* consumer) whose value changed.
    pub input_name: String,
    pub oh_a: String,
    pub oh_b: String,
    pub diff: KeyDocDiff,
}

/// Result of `explain_view_path` (CONTRACT.md §10.2).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum Explanation {
    /// The view path exists in the current generation but had no
    /// predecessor at the same path in any earlier generation.
    FirstAppearance {
        view_path: String,
        generation: u64,
        oh: String,
    },
    Diff {
        view_path: String,
        previous_generation: u64,
        current_generation: u64,
        oh_a: String,
        oh_b: String,
        diff: Box<KeyDocDiff>,
        /// Recursive resolution of changed parent inputs, depth-first,
        /// capped at `MAX_WHY_DEPTH`.
        why_chain: Vec<WhyStep>,
    },
}

const MAX_WHY_DEPTH: u32 = 8;

fn find_manifest_any_store(stores: &StoreSet, oh: &str) -> Option<Manifest> {
    stores
        .stores
        .iter()
        .find_map(|s| read_manifest_by_oh(s, oh).ok())
}

/// Recursively resolve changed `inputs` entries whose value is itself a
/// parent job's output hash (found in one of `stores`): diff the parents'
/// key documents too, and recurse, up to `MAX_WHY_DEPTH`. Inputs that don't
/// resolve to a known entry (leaf/content hashes) terminate the chain.
fn resolve_why_chain(
    stores: &StoreSet,
    doc_a: &Value,
    doc_b: &Value,
    depth: u32,
    out: &mut Vec<WhyStep>,
) {
    if depth >= MAX_WHY_DEPTH {
        return;
    }
    let diff = diff_key_documents(doc_a, doc_b);
    let inputs_a = doc_a.get("inputs").and_then(Value::as_object);
    let inputs_b = doc_b.get("inputs").and_then(Value::as_object);
    for name in &diff.inputs.changed {
        let oh_a = inputs_a.and_then(|m| m.get(name)).and_then(Value::as_str);
        let oh_b = inputs_b.and_then(|m| m.get(name)).and_then(Value::as_str);
        let (oh_a, oh_b) = match (oh_a, oh_b) {
            (Some(a), Some(b)) => (a, b),
            _ => continue,
        };
        let (Some(pa), Some(pb)) = (
            find_manifest_any_store(stores, oh_a),
            find_manifest_any_store(stores, oh_b),
        ) else {
            continue; // leaf input, not a resolvable job output — chain stops here
        };
        let child_diff = diff_key_documents(&pa.key_document, &pb.key_document);
        out.push(WhyStep {
            depth: depth + 1,
            input_name: name.clone(),
            oh_a: oh_a.to_string(),
            oh_b: oh_b.to_string(),
            diff: child_diff,
        });
        resolve_why_chain(stores, &pa.key_document, &pb.key_document, depth + 1, out);
    }
}

fn find_store_by_name<'a>(stores: &'a StoreSet, name: &str) -> Result<&'a Store, Error> {
    stores
        .stores
        .iter()
        .find(|s| s.name() == name)
        .ok_or_else(|| {
            Error::Other(format!(
                "generation meta references store {name:?} which is not in the provided StoreSet"
            ))
        })
}

/// Explain a view path: find its entry in the current generation, diff
/// against the same view path in the previous *existing* generation (older
/// generations may have been dropped by GC — "previous" skips gaps), and
/// recursively resolve changed parent inputs (PPG3_DESIGN.md §10.2). All
/// read-only.
pub fn explain_view_path(
    project_dir: &Path,
    stores: &StoreSet,
    view_path: &str,
) -> Result<Explanation, Error> {
    let current = views::current_generation_number(project_dir)?
        .ok_or_else(|| Error::Other("project has no current generation".to_string()))?;
    let cur_meta = views::read_generation_meta(project_dir, current)?;
    let cur_entry = cur_meta
        .entries
        .iter()
        .find(|e| e.view_rel_path == view_path)
        .ok_or_else(|| {
            Error::Other(format!(
                "view path {view_path:?} not found in current generation ({current})"
            ))
        })?;
    let cur_store = find_store_by_name(stores, &cur_entry.store_name)?;
    let cur_manifest = read_manifest_by_oh(cur_store, &cur_entry.oh)?;

    let mut search_from = current;
    loop {
        let prev = match views::previous_existing_generation(project_dir, search_from)? {
            None => {
                return Ok(Explanation::FirstAppearance {
                    view_path: view_path.to_string(),
                    generation: current,
                    oh: cur_entry.oh.clone(),
                });
            }
            Some(p) => p,
        };
        let prev_meta = views::read_generation_meta(project_dir, prev)?;
        let prev_entry = match prev_meta
            .entries
            .iter()
            .find(|e| e.view_rel_path == view_path)
        {
            None => {
                // View path didn't exist yet in this older generation either
                // — keep walking further back before declaring first
                // appearance, in case it appeared, briefly vanished, and
                // reappeared (renamed jobs, etc. are out of scope, but a
                // dropped intermediate generation should not falsely count
                // as "first appearance").
                search_from = prev;
                continue;
            }
            Some(e) => e,
        };
        let prev_store = find_store_by_name(stores, &prev_entry.store_name)?;
        let prev_manifest = read_manifest_by_oh(prev_store, &prev_entry.oh)?;

        let diff = diff_key_documents(&prev_manifest.key_document, &cur_manifest.key_document);
        let mut why_chain = Vec::new();
        resolve_why_chain(
            stores,
            &prev_manifest.key_document,
            &cur_manifest.key_document,
            0,
            &mut why_chain,
        );

        return Ok(Explanation::Diff {
            view_path: view_path.to_string(),
            previous_generation: prev,
            current_generation: current,
            oh_a: prev_entry.oh.clone(),
            oh_b: cur_entry.oh.clone(),
            diff: Box::new(diff),
            why_chain,
        });
    }
}

/// List every entry (`oh`) currently published in `store`. Used by
/// `ppg3 store verify --sample` (WP5 CLI) — not itself part of the
/// CONTRACT.md `explain.rs` API, but lives here rather than in `store.rs`
/// (out of scope to modify) since `Store::entries_dir()` is only
/// `pub(crate)`.
pub fn list_entries(store: &Store) -> Result<Vec<String>, Error> {
    let mut names = store::list_dir_names(&store.entries_dir())?;
    // entries_dir should only ever contain 64-hex-char oh directories, but
    // be defensive about stray dotfiles / future sibling files.
    names.retain(|n| n.len() == 64 && n.bytes().all(|b| b.is_ascii_hexdigit()));
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(recipe: &str, inputs: Value, env: Value) -> Value {
        json!({
            "ppg3_key_version": 1,
            "job_recipe": recipe,
            "inputs": inputs,
            "tools": {},
            "runtime": {"python_env": "x", "preload": [], "shim": "1"},
            "env": env,
            "outputs_declared": ["out.txt"],
        })
    }

    #[test]
    fn recipe_change_detected() {
        let a = doc("r1", json!({}), json!({}));
        let b = doc("r2", json!({}), json!({}));
        let diff = diff_key_documents(&a, &b);
        assert!(diff.recipe_changed);
        assert!(!diff.is_empty());
    }

    #[test]
    fn input_added_removed_changed() {
        let a = doc("r", json!({"x": "aaaa", "y": "bbbb"}), json!({}));
        let b = doc(
            "r",
            json!({"x": "aaaa", "y": "cccc", "z": "dddd"}),
            json!({}),
        );
        let diff = diff_key_documents(&a, &b);
        assert!(!diff.recipe_changed);
        assert_eq!(diff.inputs.added, vec!["z".to_string()]);
        assert_eq!(diff.inputs.removed, Vec::<String>::new());
        assert_eq!(diff.inputs.changed, vec!["y".to_string()]);
    }

    #[test]
    fn env_added_detected() {
        let a = doc("r", json!({}), json!({"A": "1"}));
        let b = doc("r", json!({}), json!({"A": "1", "B": "2"}));
        let diff = diff_key_documents(&a, &b);
        assert_eq!(diff.env.added, vec!["B".to_string()]);
        assert!(diff.env.removed.is_empty());
        assert!(diff.env.changed.is_empty());
    }

    #[test]
    fn no_changes_is_empty_diff() {
        let a = doc("r", json!({"x": "1"}), json!({"A": "1"}));
        let b = doc("r", json!({"x": "1"}), json!({"A": "1"}));
        assert!(diff_key_documents(&a, &b).is_empty());
    }

    #[test]
    fn runtime_and_outputs_declared_changes() {
        let mut a = doc("r", json!({}), json!({}));
        let mut b = doc("r", json!({}), json!({}));
        b["runtime"]["preload"] = json!(["numpy"]);
        let diff = diff_key_documents(&a, &b);
        assert!(diff.runtime_changed);

        a["outputs_declared"] = json!(["a.txt", "b.txt"]);
        b["outputs_declared"] = json!(["b.txt", "a.txt"]); // reordered only
        let diff2 = diff_key_documents(&a, &b);
        assert!(!diff2.outputs_declared_changed, "order must not matter");

        b["outputs_declared"] = json!(["b.txt"]);
        let diff3 = diff_key_documents(&a, &b);
        assert!(diff3.outputs_declared_changed);
    }

    #[test]
    fn first_diff_offset_finds_correct_byte() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        std::fs::write(&a, b"hello world").unwrap();
        std::fs::write(&b, b"hello WORLD").unwrap();
        assert_eq!(first_diff_offset(&a, &b).unwrap(), Some(6));
    }

    #[test]
    fn first_diff_offset_identical_files() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        std::fs::write(&a, b"same same same").unwrap();
        std::fs::write(&b, b"same same same").unwrap();
        assert_eq!(first_diff_offset(&a, &b).unwrap(), None);
    }

    #[test]
    fn first_diff_offset_length_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        std::fs::write(&a, b"short").unwrap();
        std::fs::write(&b, b"short and longer").unwrap();
        assert_eq!(first_diff_offset(&a, &b).unwrap(), Some(5));
    }
}
