#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use bytes::Bytes;

use crate::error::Result;

pub trait StorageIterator {
    /// Get the current value.
    fn value(&self) -> &[u8];

    /// Get the current key.
    fn key(&self) -> &[u8];

    /// Check if the current iterator is valid.
    fn is_valid(&self) -> bool;

    /// Move to the next position.
    fn next(&mut self) -> crate::error::Result<()>;
}

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None
            };
        }

        let mut heap = BinaryHeap::new();
        
        // All invalid, select the last one as the current.
        if iters.iter().all(|iter| !iter.is_valid()) {
            let mut iters = iters;
            return Self {
                iters: heap,
                current: iters.pop().map(|iter|HeapWrapper(0, iter)),
            }
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = heap.pop();
        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|wrapper| wrapper.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();

        while let Some(mut iter) = self.iters.peek_mut() {
            if iter.1.key() == current.1.key() {
                if let err @ Err(_) = iter.1.next() {
                    PeekMut::pop(iter);
                    return err;
                }
                if !iter.1.is_valid() {
                    PeekMut::pop(iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        if let Some(mut iter) = self.iters.peek_mut() {
            if *current < *iter {
                std::mem::swap(&mut *iter, current);
            }
        }

        Ok(())
    }
}

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    a_is_next: bool,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    fn a_is_next(a: &A, b: &B) -> bool {
        if !a.is_valid() {
            return false;
        }
        if !b.is_valid() {
            return true;
        }
        a.key() < b.key()
    }

    fn skip_b(&mut self) -> Result<()> {
        if self.a.is_valid() {
            while self.b.is_valid() && self.a.key() == self.b.key() {
                self.b.next()?;
            }
        }
        Ok(())
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let mut this = Self {
            a,
            b,
            a_is_next: false,
        };
        this.skip_b()?;
        this.a_is_next = Self::a_is_next(&this.a, &this.b);
        Ok(this)
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        match self.a_is_next {
            true => self.a.key(),
            false => self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        match self.a_is_next {
            true => self.a.value(),
            false => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self.a_is_next {
            true => self.a.is_valid(),
            false => self.b.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self.a_is_next {
            true => self.a.next()?,
            false => self.b.next()?,
        }
        self.skip_b()?;
        self.a_is_next = Self::a_is_next(&self.a, &self.b);
        Ok(())
    }
}



#[derive(Clone)]
pub struct MockIterator {
    pub data: Vec<(Bytes, Bytes)>,
    pub index: usize,
}

impl MockIterator {
    pub fn new(data: Vec<(Bytes, Bytes)>) -> Self {
        Self { data, index: 0 }
    }
}

impl StorageIterator for MockIterator {
    fn next(&mut self) -> Result<()> {
        if self.index < self.data.len() {
            self.index += 1;
        }
        Ok(())
    }

    fn key(&self) -> &[u8] {
        self.data[self.index].0.as_ref()
    }

    fn value(&self) -> &[u8] {
        self.data[self.index].1.as_ref()
    }

    fn is_valid(&self) -> bool {
        self.index < self.data.len()
    }
}

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

fn check_iter_result(iter: impl StorageIterator, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        assert!(iter.is_valid());
        assert_eq!(
            k,
            iter.key(),
            "expected key: {:?}, actual key: {:?}",
            k,
            as_bytes(iter.key()),
        );
        assert_eq!(
            v,
            iter.value(),
            "expected value: {:?}, actual value: {:?}",
            v,
            as_bytes(iter.value()),
        );
        iter.next().unwrap();
    }
    assert!(!iter.is_valid());
}

#[test]
fn test_merge_1() {
    let i1 = MockIterator::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i2 = MockIterator::new(vec![
        (Bytes::from("a"), Bytes::from("1.2")),
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let i3 = MockIterator::new(vec![
        (Bytes::from("b"), Bytes::from("2.3")),
        (Bytes::from("c"), Bytes::from("3.3")),
        (Bytes::from("d"), Bytes::from("4.3")),
    ]);

    let iter = MergeIterator::create(vec![
        Box::new(i1.clone()),
        Box::new(i2.clone()),
        Box::new(i3.clone()),
    ]);

    check_iter_result(
        iter,
        vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    );

    let iter = MergeIterator::create(vec![Box::new(i3), Box::new(i1), Box::new(i2)]);

    check_iter_result(
        iter,
        vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.3")),
            (Bytes::from("c"), Bytes::from("3.3")),
            (Bytes::from("d"), Bytes::from("4.3")),
        ],
    );
}

#[test]
fn test_merge_2() {
    let i1 = MockIterator::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i2 = MockIterator::new(vec![
        (Bytes::from("d"), Bytes::from("1.2")),
        (Bytes::from("e"), Bytes::from("2.2")),
        (Bytes::from("f"), Bytes::from("3.2")),
        (Bytes::from("g"), Bytes::from("4.2")),
    ]);
    let i3 = MockIterator::new(vec![
        (Bytes::from("h"), Bytes::from("1.3")),
        (Bytes::from("i"), Bytes::from("2.3")),
        (Bytes::from("j"), Bytes::from("3.3")),
        (Bytes::from("k"), Bytes::from("4.3")),
    ]);
    let i4 = MockIterator::new(vec![]);
    let result = vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
        (Bytes::from("d"), Bytes::from("1.2")),
        (Bytes::from("e"), Bytes::from("2.2")),
        (Bytes::from("f"), Bytes::from("3.2")),
        (Bytes::from("g"), Bytes::from("4.2")),
        (Bytes::from("h"), Bytes::from("1.3")),
        (Bytes::from("i"), Bytes::from("2.3")),
        (Bytes::from("j"), Bytes::from("3.3")),
        (Bytes::from("k"), Bytes::from("4.3")),
    ];

    let iter = MergeIterator::create(vec![
        Box::new(i1.clone()),
        Box::new(i2.clone()),
        Box::new(i3.clone()),
        Box::new(i4.clone()),
    ]);
    check_iter_result(iter, result.clone());

    let iter = MergeIterator::create(vec![
        Box::new(i2.clone()),
        Box::new(i4.clone()),
        Box::new(i3.clone()),
        Box::new(i1.clone()),
    ]);
    check_iter_result(iter, result.clone());

    let iter = MergeIterator::create(vec![Box::new(i4), Box::new(i3), Box::new(i2), Box::new(i1)]);
    check_iter_result(iter, result);
}

#[test]
fn test_merge_empty() {
    let iter = MergeIterator::<MockIterator>::create(vec![]);
    check_iter_result(iter, vec![]);
}

fn check_iter_result_two(iter: impl StorageIterator, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        assert!(iter.is_valid());
        assert_eq!(iter.key(), k.as_ref());
        assert_eq!(iter.value(), v.as_ref());
        iter.next().unwrap();
    }
    assert!(!iter.is_valid());
}

#[test]
fn test_merge_1_two() {
    let i1 = MockIterator::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i2 = MockIterator::new(vec![
        (Bytes::from("a"), Bytes::from("1.2")),
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let iter = TwoMergeIterator::create(i1, i2).unwrap();
    check_iter_result_two(
        iter,
        vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    )
}

#[test]
fn test_merge_2_two() {
    let i2 = MockIterator::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i1 = MockIterator::new(vec![
        (Bytes::from("a"), Bytes::from("1.2")),
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let iter = TwoMergeIterator::create(i1, i2).unwrap();
    check_iter_result_two(
        iter,
        vec![
            (Bytes::from("a"), Bytes::from("1.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    )
}

#[test]
fn test_merge_3_two() {
    let i2 = MockIterator::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i1 = MockIterator::new(vec![
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let iter = TwoMergeIterator::create(i1, i2).unwrap();
    check_iter_result_two(
        iter,
        vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    )
}

#[test]
fn test_merge_4_two() {
    let i2 = MockIterator::new(vec![]);
    let i1 = MockIterator::new(vec![
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let iter = TwoMergeIterator::create(i1, i2).unwrap();
    check_iter_result_two(
        iter,
        vec![
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    );
    let i1 = MockIterator::new(vec![]);
    let i2 = MockIterator::new(vec![
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let iter = TwoMergeIterator::create(i1, i2).unwrap();
    check_iter_result_two(
        iter,
        vec![
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    );
}

#[test]
fn test_merge_5_two() {
    let i2 = MockIterator::new(vec![]);
    let i1 = MockIterator::new(vec![]);
    let iter = TwoMergeIterator::create(i1, i2).unwrap();
    check_iter_result_two(iter, vec![])
}
