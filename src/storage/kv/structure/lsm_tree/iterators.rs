use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use bytes::Bytes;

use crate::error::Result;

pub trait StorageIter: Iterator + DoubleEndedIterator + Clone{
    /// Get the entry for the front iterator.
    fn front_entry(&self) -> Option<(Vec<u8>, Vec<u8>)>;

    /// Get the entry for the back iterator.
    fn back_entry(&self) -> Option<(Vec<u8>, Vec<u8>)>;

    /// Check if the current iterator is valid.
    fn is_valid(&self) -> bool;

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>>;

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>>;
}

#[derive(Clone)]
struct FrontWrapper<I: StorageIter> {
    idx: usize,
    iter: Box<I>,
}

impl<I: StorageIter> PartialEq for FrontWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIter> Eq for FrontWrapper<I> {}

impl<I: StorageIter> PartialOrd for FrontWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if let Some((this_key, _)) = self.iter.front_entry() {
            if let Some((other_key, _)) = other.iter.front_entry() {
                return match this_key.cmp(&other_key) {
                    cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
                    cmp::Ordering::Less => Some(cmp::Ordering::Less),
                    cmp::Ordering::Equal => self.idx.partial_cmp(&other.idx),
                }
                .map(|x| x.reverse());
            }
        }
        // Shouldn't reach here.
        assert!(false);
        None
    }
}

impl<I: StorageIter> Ord for FrontWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Clone)]
struct BackWrapper<I: StorageIter> {
    idx: usize,
    iter: Box<I>,
}

impl<I: StorageIter> PartialEq for BackWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIter> Eq for BackWrapper<I> {}

impl<I: StorageIter> PartialOrd for BackWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if let Some((this_key, _)) = self.iter.back_entry() {
            if let Some((other_key, _)) = other.iter.back_entry() {
                return match this_key.cmp(&other_key) {
                    cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
                    cmp::Ordering::Less => Some(cmp::Ordering::Less),
                    cmp::Ordering::Equal => 
                        self.idx.partial_cmp(&other.idx).map(|ord| ord.reverse()),
                };
            }
        }
        // Shouldn't reach here.
        assert!(false);
        None
    }
}

impl<I: StorageIter> Ord for BackWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Clone)]
/// Rust-compatible iterator on a multiple iters of the same type.
pub struct MergeIter<I: StorageIter> {
    front_iters: BinaryHeap<FrontWrapper<I>>,
    back_iters: BinaryHeap<BackWrapper<I>>,
    front_entry: Option<(Vec<u8>, Vec<u8>)>,
    back_entry: Option<(Vec<u8>, Vec<u8>)>,
}

// TODO: Result<Self>
impl<I: StorageIter> MergeIter<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                front_iters: BinaryHeap::new(),
                back_iters: BinaryHeap::new(),
                front_entry: None,
                back_entry: None,
            };
        }

        let mut front_heap = BinaryHeap::new();
        let mut back_heap = BinaryHeap::new();
        
        // All invalid.
        if iters.iter().all(|iter| !iter.is_valid()) {
            return Self {
                front_iters: front_heap,
                back_iters: back_heap,
                front_entry: None,
                back_entry: None,
            }
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                let mut front_iter = iter.clone();
                front_iter.next();
                let mut back_iter = iter.clone();
                back_iter.next_back();
                front_heap.push(FrontWrapper{idx, iter: front_iter});
                back_heap.push(BackWrapper{idx, iter: back_iter});
            }
        }

        Self {
            front_iters: front_heap,
            back_iters: back_heap,
            front_entry: None,
            back_entry: None,
        }
    }
}

impl<I: StorageIter> StorageIter for MergeIter<I> {
    fn front_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.front_entry.clone()
    }

    fn back_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.back_entry.clone()
    }

    fn is_valid(&self) -> bool {
        match (&self.front_entry, &self.back_entry) {
            (Some((front_key, _)), Some((back_key, _))) => { front_key < back_key },
            (Some(_), None) => { !self.front_iters.is_empty() },
            (None, Some(_)) => { !self.back_iters.is_empty() },
            (None, None) => { true },
        }
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.is_valid() {
            false => { Ok(None) },
            true => {
                if let Some(top) = self.front_iters.peek() {
                    self.front_entry = top.iter.front_entry();
                    let (cur_key, _) = self.front_entry.as_ref()
                        .expect("should have front entry");
                    while let Some(mut top) = self.front_iters.peek_mut() {
                        let (ref top_key, _) = top.iter.front_entry()
                            .expect("should have front entry");
                        if top_key == cur_key {
                            if top.iter.try_next()?.is_none() {
                                PeekMut::pop(top);
                            }
                        } else {
                            break;
                        }
                    }
                }
                Ok(self.front_entry.clone())
            }
        }
    }
    
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.is_valid() {
            false => { Ok(None) },
            true => {
                if let Some(top) = self.back_iters.peek() {
                    self.back_entry = top.iter.back_entry();
                    let (ref cur_key, _) = self.back_entry.as_ref()
                        .expect("should have back entry");
                    while let Some(mut top) = self.back_iters.peek_mut() {
                        let (ref top_key, _) = top.iter.back_entry()
                            .expect("should have back entry");
                        if top_key == cur_key {
                            if top.iter.try_next_back()?.is_none() {
                                PeekMut::pop(top);
                            }
                        } else {
                            break;
                        }
                    }
                }
                Ok(self.back_entry.clone())
            }
        }
    }
}

impl<I: StorageIter> Iterator for MergeIter<I> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<I: StorageIter> DoubleEndedIterator for MergeIter<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[derive(Clone)]
pub struct MockIter {
    pub data: Vec<(Bytes, Bytes)>,
    pub front_index: i32,
    pub back_index: i32,
}

impl MockIter {
    pub fn new(data: Vec<(Bytes, Bytes)>) -> Self {
        Self { front_index: -1, back_index: data.len() as i32, data }
    }
}

impl StorageIter for MockIter {
    fn front_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.front_index < 0 {
            return None;
        } else {
            return Some((
                self.data[self.front_index as usize].0.to_vec(), 
                self.data[self.front_index as usize].1.to_vec(),
            ));
        }
    }

    fn back_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if (self.back_index as usize) >= self.data.len() {
            return None;
        } else {
            return Some((
                self.data[self.back_index as usize].0.to_vec(), 
                self.data[self.back_index as usize].1.to_vec(),
            ));
        }
    }

    fn is_valid(&self) -> bool {
        self.data.len() != 0 && 
        self.front_index < self.data.len() as i32 &&
        0 <= self.back_index
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.front_index += 1;
        if (self.front_index as usize) < self.data.len() {
            return Ok(Some((
                self.data[self.front_index as usize].0.to_vec(), 
                self.data[self.front_index as usize].1.to_vec(),
            )));
        }
        Ok(None)
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.back_index -= 1;
        if self.back_index >= 0 {
            return Ok(Some((
                self.data[self.back_index as usize].0.to_vec(), 
                self.data[self.back_index as usize].1.to_vec(),
            )));
        }
        Ok(None)
    }
}

impl Iterator for MockIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for MockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[derive(Clone)]
/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIter<A: StorageIter, B: StorageIter> {
    front_a: A,
    front_b: B,
    front_a_is_next: bool,
    front_entry: Option<(Vec<u8>, Vec<u8>)>,
    back_a: A,
    back_b: B,
    back_a_is_next: bool,
    back_entry: Option<(Vec<u8>, Vec<u8>)>,
}

impl<A: StorageIter, B: StorageIter> TwoMergeIter<A, B> {
    fn front_a_is_next(a: &A, b: &B) -> bool {
        match (a.is_valid(), b.is_valid()) {
            (true, true) => {
                let (a_key, _) = a.front_entry().expect("should have front entry");
                let (b_key, _) = b.front_entry().expect("should have front entry");
                a_key < b_key
            },
            (false, _) => { false },
            (true, false) => { true },
        }
    }

    fn front_skip_b(&mut self) -> Result<()> {
        if self.front_a.is_valid() {
            let (a_key, _) = self.front_a.front_entry().expect("should have front entry");
            let (mut b_key, _) = self.front_b.front_entry().expect("should have front entry");
            while self.front_b.is_valid() && a_key == b_key {
                self.front_b.try_next()?;
                (b_key, _) = self.front_b.front_entry().expect("should have front entry");
            }
        }
        Ok(())
    }

    fn back_a_is_next(a: &A, b: &B) -> bool {
        match (a.is_valid(), b.is_valid()) {
            (true, true) => {
                let (a_key, _) = a.back_entry().expect("should have front entry");
                let (b_key, _) = b.back_entry().expect("should have front entry");
                a_key > b_key
            },
            (false, _) => { false },
            (true, false) => { true },
        }
    }

    fn back_skip_b(&mut self) -> Result<()> {
        if self.back_a.is_valid() {
            let (a_key, _) = self.back_a.back_entry().expect("should have front entry");
            let (mut b_key, _) = self.back_b.back_entry().expect("should have front entry");
            while self.back_b.is_valid() && a_key == b_key {
                self.back_b.try_next_back()?;
                if self.back_b.is_valid() { (b_key, _) = self.back_b.back_entry().expect("should have front entry"); }
            }
        }
        Ok(())
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let mut front_a = a.clone();
        let mut front_b = b.clone();
        front_a.try_next()?;
        front_b.try_next()?;
        let mut back_a = a.clone();
        let mut back_b = b.clone();
        back_a.try_next_back()?;
        back_b.try_next_back()?;
        let mut this = Self {
            front_a,
            front_b,
            front_a_is_next: false,
            front_entry: None,
            back_a,
            back_b,
            back_a_is_next: false,
            back_entry: None,
        };
        this.front_skip_b()?;
        this.back_skip_b()?;
        this.front_a_is_next = Self::front_a_is_next(&this.front_a, &this.front_b);
        this.back_a_is_next = Self::back_a_is_next(&this.back_a, &this.back_b);
        Ok(this)
    }
}

impl<A: StorageIter, B: StorageIter> StorageIter for TwoMergeIter<A, B> {
    fn front_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.front_entry.clone()
    }

    fn back_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.back_entry.clone()
    }

    fn is_valid(&self) -> bool {
        todo!()
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.front_a_is_next {
            true => {
                self.front_entry = self.front_a.front_entry();
                self.front_a.try_next()?;
            },

            false => {
                self.front_entry = self.front_b.front_entry();
                self.front_b.try_next()?;
            }
        };
        self.front_skip_b()?;
        self.front_a_is_next = Self::front_a_is_next(&self.front_a, &self.front_b);
        Ok(self.front_entry.clone())
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.back_a_is_next {
            true => {
                self.back_entry = self.back_a.back_entry();
                self.back_a.try_next_back()?;
            },

            false => {
                self.back_entry = self.back_b.back_entry();
                self.back_b.try_next_back()?;
            }
        };
        self.back_skip_b()?;
        self.back_a_is_next = Self::back_a_is_next(&self.back_a, &self.back_b);
        Ok(self.back_entry.clone())
    }
}

impl<A: StorageIter, B: StorageIter> Iterator for TwoMergeIter<A, B> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<A: StorageIter, B: StorageIter> DoubleEndedIterator for TwoMergeIter<A, B> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

// ============================================================================================= //

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

// ============================================================================================= //

#[cfg(test)]
fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[cfg(test)]
fn check_result(iter: impl StorageIter, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        iter.next().unwrap();
        // assert!(iter.is_valid());
        assert_eq!(
            k,
            &iter.front_entry().unwrap().0[..],
            "expected key: {:?}, actual key: {:?}",
            k,
            as_bytes(&iter.front_entry().unwrap().0[..]),
        );
        assert_eq!(
            v,
            &iter.front_entry().unwrap().1[..],
            "expected value: {:?}, actual value: {:?}",
            v,
            as_bytes(&iter.front_entry().unwrap().1[..]),
        );
    }
    // assert!(!iter.is_valid());
}

#[cfg(test)]
fn check_result_rev(iter: impl StorageIter, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        iter.next_back().unwrap();
        // assert!(iter.is_valid());
        assert_eq!(
            k,
            &iter.back_entry().unwrap().0[..],
            "expected key: {:?}, actual key: {:?}",
            k,
            as_bytes(&iter.back_entry().unwrap().0[..]),
        );
        assert_eq!(
            v,
            &iter.back_entry().unwrap().1[..],
            "expected value: {:?}, actual value: {:?}",
            v,
            as_bytes(&iter.back_entry().unwrap().1[..]),
        );
    }
    // assert!(!iter.is_valid());
}

#[test]
fn test_merge_1_iter() {
    let i1 = MockIter::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i2 = MockIter::new(vec![
        (Bytes::from("a"), Bytes::from("1.2")),
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let i3 = MockIter::new(vec![
        (Bytes::from("b"), Bytes::from("2.3")),
        (Bytes::from("c"), Bytes::from("3.3")),
        (Bytes::from("d"), Bytes::from("4.3")),
    ]);

    let iter = MergeIter::create(vec![
        Box::new(i1.clone()),
        Box::new(i2.clone()),
        Box::new(i3.clone()),
    ]);

    check_result(
        iter,
        vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    );

    let iter = MergeIter::create(vec![Box::new(i3), Box::new(i1), Box::new(i2)]);

    check_result(
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
fn test_merge_1_iter_rev() {
    let i1 = MockIter::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i2 = MockIter::new(vec![
        (Bytes::from("a"), Bytes::from("1.2")),
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let i3 = MockIter::new(vec![
        (Bytes::from("b"), Bytes::from("2.3")),
        (Bytes::from("c"), Bytes::from("3.3")),
        (Bytes::from("d"), Bytes::from("4.3")),
    ]);

    let iter = MergeIter::create(vec![
        Box::new(i1.clone()),
        Box::new(i2.clone()),
        Box::new(i3.clone()),
    ]);

    check_result_rev(
        iter,
        vec![
            (Bytes::from("d"), Bytes::from("4.2")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("a"), Bytes::from("1.1")),
        ],
    );

    let iter = MergeIter::create(vec![Box::new(i3), Box::new(i1), Box::new(i2)]);

    check_result_rev(
        iter,
        vec![
            (Bytes::from("d"), Bytes::from("4.3")),
            (Bytes::from("c"), Bytes::from("3.3")),
            (Bytes::from("b"), Bytes::from("2.3")),
            (Bytes::from("a"), Bytes::from("1.1")),
        ],
    );
}

#[test]
fn test_merge_2_iter() {
    let i1 = MockIter::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i2 = MockIter::new(vec![
        (Bytes::from("d"), Bytes::from("1.2")),
        (Bytes::from("e"), Bytes::from("2.2")),
        (Bytes::from("f"), Bytes::from("3.2")),
        (Bytes::from("g"), Bytes::from("4.2")),
    ]);
    let i3 = MockIter::new(vec![
        (Bytes::from("h"), Bytes::from("1.3")),
        (Bytes::from("i"), Bytes::from("2.3")),
        (Bytes::from("j"), Bytes::from("3.3")),
        (Bytes::from("k"), Bytes::from("4.3")),
    ]);
    let i4 = MockIter::new(vec![]);

    let mut result = vec![
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

    let iter = MergeIter::create(vec![
        Box::new(i1.clone()),
        Box::new(i2.clone()),
        Box::new(i3.clone()),
        Box::new(i4.clone()),
    ]);
    check_result(iter, result.clone());

    let iter = MergeIter::create(vec![
        Box::new(i2.clone()),
        Box::new(i4.clone()),
        Box::new(i3.clone()),
        Box::new(i1.clone()),
    ]);
    check_result(iter, result.clone());

    let iter = MergeIter::create(vec![
        Box::new(i4.clone()),
        Box::new(i3.clone()),
        Box::new(i2.clone()),
        Box::new(i1.clone()),
    ]);
    check_result(iter, result.clone());
    
    result.reverse();

    let iter = MergeIter::create(vec![
        Box::new(i1.clone()),
        Box::new(i2.clone()),
        Box::new(i3.clone()),
        Box::new(i4.clone()),
    ]);
    check_result_rev(iter, result.clone());

    let iter = MergeIter::create(vec![
        Box::new(i2.clone()),
        Box::new(i4.clone()),
        Box::new(i3.clone()),
        Box::new(i1.clone()),
    ]);
    check_result_rev(iter, result.clone());

    let iter = MergeIter::create(vec![
        Box::new(i4.clone()),
        Box::new(i3.clone()),
        Box::new(i2.clone()),
        Box::new(i1.clone()),
    ]);
    check_result_rev(iter, result);
}

#[test]
fn test_merge_empty_iter() {
    let iter = MergeIter::<MockIter>::create(vec![]);
    check_result(iter, vec![]);
}

#[cfg(test)]
fn check_result_two(iter: impl StorageIter, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        // assert!(iter.is_valid());
        iter.next().unwrap();
        assert_eq!(&iter.front_entry().unwrap().0[..], k.as_ref());
        assert_eq!(&iter.front_entry().unwrap().1[..], v.as_ref());
    }
    // assert!(!iter.is_valid());
}

#[cfg(test)]
fn check_result_two_rev(iter: impl StorageIter, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        // assert!(iter.is_valid());
        iter.next_back().unwrap();
        assert_eq!(&iter.back_entry().unwrap().0[..], k.as_ref());
        assert_eq!(&iter.back_entry().unwrap().1[..], v.as_ref());
    }
    // assert!(!iter.is_valid());
}

#[test]
fn test_merge_1_two_iter() {
    let i1 = MockIter::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i2 = MockIter::new(vec![
        (Bytes::from("a"), Bytes::from("1.2")),
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let iter = TwoMergeIter::create(i1.clone(), i2.clone()).unwrap();
    check_result_two(
        iter,
        vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    );
    let iter_rev = TwoMergeIter::create(i1.clone(), i2.clone()).unwrap();
    check_result_two_rev(
        iter_rev,
        vec![
            (Bytes::from("d"), Bytes::from("4.2")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("a"), Bytes::from("1.1")),
        ],
    )
}

// ============================================================================================= //

#[cfg(test)]
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

#[cfg(test)]
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
