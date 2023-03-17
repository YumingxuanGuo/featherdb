#[cfg(test)]
use bytes::Bytes;
#[cfg(test)]
use tempfile::tempdir;

#[cfg(test)]
use crate::storage::kv::ConcurrentStore;
#[cfg(test)]
use crate::storage::kv::KvScan;
#[cfg(test)]
use crate::storage::kv::Range;

#[cfg(test)]
fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[cfg(test)]
fn check_iter_result(iter: KvScan, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        let (key, value) = iter.next().unwrap().unwrap();
        assert_eq!(
            k,
            as_bytes(&key),
            "expected key: {:?}, actual key: {:?}",
            k,
            as_bytes(&key),
        );
        assert_eq!(
            v,
            as_bytes(&value),
            "expected value: {:?}, actual value: {:?}",
            v,
            as_bytes(&value),
        );
    }
    assert!(iter.next().is_none());
}

#[test]
fn test_storage_get() {
    use super::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.set(b"1", b"233".to_vec()).unwrap();
    storage.set(b"2", b"2333".to_vec()).unwrap();
    storage.set(b"3", b"23333".to_vec()).unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
    storage.delete(b"2").unwrap();
    assert!(storage.get(b"2").unwrap().is_none());
}

#[test]
fn test_storage_scan_memtable_1() {
    use super::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.set(b"1", b"233".to_vec()).unwrap();
    storage.set(b"2", b"2333".to_vec()).unwrap();
    storage.set(b"3", b"23333".to_vec()).unwrap();
    storage.delete(b"2").unwrap();
    check_iter_result(
        storage.scan(Range::from(..)).unwrap(),
        vec![
            (Bytes::from("1"), Bytes::from("233")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Range::from(b"1".to_vec()..=b"2".to_vec()))
            .unwrap(),
        vec![
            (Bytes::from("1"), Bytes::from("233")),
        ],
    );
    check_iter_result(
        storage
            .scan(Range::from(b"1".to_vec()..b"3".to_vec()))
            .unwrap(),
        vec![
            (Bytes::from("1"), Bytes::from("233")),
        ],
    );
}

#[test]
fn test_storage_scan_memtable_2() {
    use super::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.set(b"1", b"233".to_vec()).unwrap();
    storage.set(b"2", b"2333".to_vec()).unwrap();
    storage.set(b"3", b"23333".to_vec()).unwrap();
    storage.delete(b"1").unwrap();
    check_iter_result(
        storage.scan(Range::from(..)).unwrap(),
        vec![
            (Bytes::from("2"), Bytes::from("2333")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Range::from(b"1".to_vec()..=b"2".to_vec()))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
    check_iter_result(
        storage
            .scan(Range::from(b"1".to_vec()..b"3".to_vec()))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
}

#[test]
fn test_storage_get_after_sync() {
    use super::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.set(b"1", b"233".to_vec()).unwrap();
    storage.set(b"2", b"2333".to_vec()).unwrap();
    storage.flush().unwrap();
    storage.set(b"3", b"23333".to_vec()).unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
    storage.delete(b"2").unwrap();
    assert!(storage.get(b"2").unwrap().is_none());
}

#[test]
fn test_storage_scan_memtable_1_after_sync() {
    use super::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.set(b"1", b"233".to_vec()).unwrap();
    storage.set(b"2", b"2333".to_vec()).unwrap();
    storage.flush().unwrap();
    storage.set(b"3", b"23333".to_vec()).unwrap();
    storage.delete(b"2").unwrap();
    check_iter_result(
        storage.scan(Range::from(..)).unwrap(),
        vec![
            (Bytes::from("1"), Bytes::from("233")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Range::from(b"1".to_vec()..=b"2".to_vec()))
            .unwrap(),
        vec![(Bytes::from("1"), Bytes::from("233"))],
    );
    check_iter_result(
        storage
            .scan(Range::from(b"1".to_vec()..b"3".to_vec()))
            .unwrap(),
        vec![(Bytes::from("1"), Bytes::from("233"))],
    );
}

#[test]
fn test_storage_scan_memtable_2_after_sync() {
    use super::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.set(b"1", b"233".to_vec()).unwrap();
    storage.set(b"2", b"2333".to_vec()).unwrap();
    storage.flush().unwrap();
    storage.set(b"3", b"23333".to_vec()).unwrap();
    storage.flush().unwrap();
    storage.delete(b"1").unwrap();
    check_iter_result(
        storage.scan(Range::from(..)).unwrap(),
        vec![
            (Bytes::from("2"), Bytes::from("2333")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Range::from(b"1".to_vec()..=b"2".to_vec()))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
    check_iter_result(
        storage
            .scan(Range::from(b"2".to_vec()..b"3".to_vec()))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
}

#[test]
fn test_storage_empty() {
    use super::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();

    assert!(storage.get(b"1").unwrap().is_none());
    assert!(storage.get(b"2").unwrap().is_none());
    storage.delete(b"1").unwrap();
    assert!(storage.get(b"1").unwrap().is_none());
    assert!(storage.get(b"2").unwrap().is_none());
}

#[test]
fn test_storage_duplicate_key() {
    use super::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();

    storage.set(b"1", b"233".to_vec()).unwrap();
    storage.set(b"2", b"2333".to_vec()).unwrap();
    storage.set(b"3", b"23333".to_vec()).unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");

    storage.flush().unwrap();
    storage.set(b"1", b"new_value1".to_vec()).unwrap();
    storage.set(b"2", b"new_value2".to_vec()).unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"new_value1");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"new_value2");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");

    storage.delete(b"2").unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"new_value1");
    assert!(storage.get(b"2").unwrap().is_none());
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");

    storage.set(b"2", b"new_value_after_deletion".to_vec()).unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"new_value1");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"new_value_after_deletion");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
}

#[test]
fn test_storage_scan_inaccurate_range() {
    use super::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.set(b"1", b"1".to_vec()).unwrap();
    storage.set(b"3", b"3".to_vec()).unwrap();
    storage.set(b"5", b"5".to_vec()).unwrap();
    storage.flush().unwrap();
    storage.set(b"7", b"7".to_vec()).unwrap();
    storage.set(b"9", b"9".to_vec()).unwrap();
    storage.flush().unwrap();
    storage.delete(b"1").unwrap();
    check_iter_result(
        storage.scan(Range::from(..)).unwrap(),
        vec![
            (Bytes::from("3"), Bytes::from("3")),
            (Bytes::from("5"), Bytes::from("5")),
            (Bytes::from("7"), Bytes::from("7")),
            (Bytes::from("9"), Bytes::from("9")),
        ],
    );
    check_iter_result(
        storage
            .scan(Range::from(b"1".to_vec()..=b"2".to_vec()))
            .unwrap(),
        vec![],
    );
    check_iter_result(
        storage
            .scan(Range::from(b"2".to_vec()..b"6".to_vec()))
            .unwrap(),
        vec![
            (Bytes::from("3"), Bytes::from("3")),
            (Bytes::from("5"), Bytes::from("5")),
        ],
    );
    storage.delete(b"7").unwrap();
    check_iter_result(
        storage
            .scan(Range::from(b"2".to_vec()..=b"9".to_vec()))
            .unwrap(),
        vec![
            (Bytes::from("3"), Bytes::from("3")),
            (Bytes::from("5"), Bytes::from("5")),
            (Bytes::from("9"), Bytes::from("9")),
        ],
    );
}

#[cfg(test)]
fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:04}", idx * 3).into_bytes()
}

#[cfg(test)]
fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

#[test]
fn test_storage_concurrent_operation() {
    use std::thread;
    use std::sync::Arc;
    use super::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = Arc::new(LsmStorage::open(&dir).unwrap());

    let mut handles = vec![];
    for i in 0..10 {
        let storage = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for j in 0..100 {
                storage.set(&key_of(j * 10 + i), value_of(j * 10 + i)).unwrap();
                assert_eq!(&storage.get(&key_of(j * 10 + i)).unwrap().unwrap(), &value_of(j * 10 + i));
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..1000 {
        assert_eq!(&storage.get(&key_of(i)).unwrap().unwrap(), &value_of(i));
    }
}