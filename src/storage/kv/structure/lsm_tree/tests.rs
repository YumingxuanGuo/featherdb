#[cfg(test)]
use bytes::Bytes;
#[cfg(test)]
use tempfile::tempdir;

#[cfg(test)]
use crate::storage::kv::structure::ConcurrentStore;
#[cfg(test)]
use crate::storage::kv::structure::KvScan;
#[cfg(test)]
use crate::storage::kv::structure::Range;

#[cfg(test)]
fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[cfg(test)]
fn check_iter_result(iter: KvScan, expected: Vec<(Bytes, Bytes)>) {

    let mut iter = iter;
    for (k, v) in expected {
        // assert!(iter.is_valid());
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
    // assert!(!iter.is_valid());
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
        vec![],
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
