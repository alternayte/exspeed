//! Cursor load/save roundtrip + atomicity tests.

use std::collections::BTreeMap;
use tempfile::tempdir;

use exspeed_broker::replication::cursor::Cursor;

#[test]
fn empty_cursor_on_missing_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cursor.json");
    let cursor = Cursor::load(&path).unwrap();
    assert!(cursor.is_empty());
}

#[test]
fn save_then_load_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cursor.json");

    let mut cursor = Cursor::new();
    cursor.set("orders", 42);
    cursor.set("payments", 100);
    cursor.save(&path).unwrap();

    let loaded = Cursor::load(&path).unwrap();
    let mut expected = BTreeMap::new();
    expected.insert("orders".to_string(), 42);
    expected.insert("payments".to_string(), 100);
    assert_eq!(loaded.as_map(), &expected);
}

#[test]
fn last_save_wins() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cursor.json");

    let mut c1 = Cursor::new();
    c1.set("orders", 1);
    c1.save(&path).unwrap();

    let mut c2 = Cursor::new();
    c2.set("orders", 999);
    c2.save(&path).unwrap();

    let loaded = Cursor::load(&path).unwrap();
    assert_eq!(loaded.get("orders"), Some(999));
}

#[test]
fn load_from_corrupt_file_returns_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cursor.json");
    std::fs::write(&path, b"not valid json {{{").unwrap();

    // Corrupt file is treated as empty + a warning is logged. Callers
    // interpret as "fresh follower."
    let cursor = Cursor::load(&path).unwrap();
    assert!(cursor.is_empty());
}

#[test]
fn load_from_wrong_value_type_returns_empty() {
    // Well-formed JSON whose values aren't u64 — the cursor map is
    // `BTreeMap<String, u64>`, so `"not-a-number"` fails deserialization.
    // Treated the same as a corrupt file: empty + warn.
    let dir = tempdir().unwrap();
    let path = dir.path().join("cursor.json");
    std::fs::write(&path, b"{\"orders\": \"not-a-number\"}").unwrap();

    let cursor = Cursor::load(&path).unwrap();
    assert!(cursor.is_empty());
}

#[test]
fn set_and_get_are_symmetric() {
    let mut cursor = Cursor::new();
    cursor.set("orders", 42);
    assert_eq!(cursor.get("orders"), Some(42));
    assert_eq!(cursor.get("unknown"), None);
}

#[test]
fn remove_drops_the_entry() {
    let mut cursor = Cursor::new();
    cursor.set("orders", 42);
    cursor.remove("orders");
    assert_eq!(cursor.get("orders"), None);
}
