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
    is_valid: bool
}

impl<I: StorageIter> MergeIter<I> {
    pub fn create(iters: Vec<Box<I>>) -> Result<Self> {
        if iters.is_empty() {
            return Ok(Self {
                front_iters: BinaryHeap::new(),
                back_iters: BinaryHeap::new(),
                front_entry: None,
                back_entry: None,
                is_valid: false,
            });
        }

        let mut front_heap = BinaryHeap::new();
        let mut back_heap = BinaryHeap::new();
        
        // All invalid.
        if iters.iter().all(|iter| !iter.is_valid()) {
            return Ok(Self {
                front_iters: front_heap,
                back_iters: back_heap,
                front_entry: None,
                back_entry: None,
                is_valid: false,
            });
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

        Ok(Self {
            front_iters: front_heap,
            back_iters: back_heap,
            front_entry: None,
            back_entry: None,
            is_valid: true,
        })
    }
}

impl<I: StorageIter> StorageIter for MergeIter<I> {
    // None at beginning and after becoming invalid.
    fn front_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.front_entry.clone()
    }

    // None at beginning and after becoming invalid.
    fn back_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.back_entry.clone()
    }

    // Semantic change: can call next() => possesses a value.
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.is_valid() {
            false => Ok(None),
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
                    if let Some((back_key, _)) = self.back_entry.as_ref() {
                        if cur_key >= back_key {
                            self.front_entry = None;
                            self.back_entry = None;
                            self.is_valid = false;
                        }
                    }
                } else {
                    self.front_entry = None;
                    self.is_valid = false;
                }
                Ok(self.front_entry.clone())
            }
        }
    }
    
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.is_valid() {
            false => Ok(None),
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
                    if let Some((front_key, _)) = self.front_entry.as_ref() {
                        if front_key >= cur_key {
                            self.front_entry = None;
                            self.back_entry = None;
                            self.is_valid = false;
                        }
                    }
                } else {
                    self.back_entry = None;
                    self.is_valid = false;
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
            while self.front_b.is_valid() {
                let (b_key, _) = self.front_b.front_entry().expect("should have front entry");
                if a_key == b_key {
                    self.front_b.try_next()?;
                } else {
                    break;
                }
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
            while self.back_b.is_valid() {
                let (b_key, _) = self.back_b.back_entry().expect("should have front entry");
                if a_key == b_key {
                    self.back_b.try_next_back()?;
                } else {
                    break;
                }
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
        match (&self.front_entry, &self.back_entry) {
            (Some(_), Some((back_key, _))) => {
                match self.front_a_is_next {
                    true => {
                        let (next_front_key, _) = &self.front_a.front_entry()
                            .expect("should have front entry");
                        next_front_key < back_key
                    },
        
                    false => {
                        let (next_front_key, _) = &self.front_b.front_entry()
                            .expect("should have front entry");
                        next_front_key < back_key
                    }
                }
            },

            (Some(_), None) => {
                match self.front_a_is_next {
                    true => self.front_a.is_valid(),
                    false => self.front_b.is_valid(),
                }
            },

            (None, Some(_)) => {
                match self.back_a_is_next {
                    true => self.back_a.is_valid(),
                    false => self.back_b.is_valid(),
                }
            },

            (None, None) => { self.back_a.is_valid() || self.back_b.is_valid() },
        }
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.is_valid() {
            false => Ok(None),
            true => {
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
        }
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.is_valid() {
            false => Ok(None),
            true => {
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



#[cfg(test)]
fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[cfg(test)]
fn check_result(iter: impl StorageIter, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        iter.next().unwrap();
        assert!(iter.is_valid());
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
    iter.next();
    assert!(!iter.is_valid());
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
    ]).unwrap();

    check_result(
        iter,
        vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    );

    let iter = MergeIter::create(vec![Box::new(i3), Box::new(i1), Box::new(i2)]).unwrap();

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
    ]).unwrap();

    check_result_rev(
        iter,
        vec![
            (Bytes::from("d"), Bytes::from("4.2")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("a"), Bytes::from("1.1")),
        ],
    );

    let iter = MergeIter::create(vec![Box::new(i3), Box::new(i1), Box::new(i2)]).unwrap();

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
    ]).unwrap();
    check_result(iter, result.clone());

    let iter = MergeIter::create(vec![
        Box::new(i2.clone()),
        Box::new(i4.clone()),
        Box::new(i3.clone()),
        Box::new(i1.clone()),
    ]).unwrap();
    check_result(iter, result.clone());

    let iter = MergeIter::create(vec![
        Box::new(i4.clone()),
        Box::new(i3.clone()),
        Box::new(i2.clone()),
        Box::new(i1.clone()),
    ]).unwrap();
    check_result(iter, result.clone());
    
    result.reverse();

    let iter = MergeIter::create(vec![
        Box::new(i1.clone()),
        Box::new(i2.clone()),
        Box::new(i3.clone()),
        Box::new(i4.clone()),
    ]).unwrap();
    check_result_rev(iter, result.clone());

    let iter = MergeIter::create(vec![
        Box::new(i2.clone()),
        Box::new(i4.clone()),
        Box::new(i3.clone()),
        Box::new(i1.clone()),
    ]).unwrap();
    check_result_rev(iter, result.clone());

    let iter = MergeIter::create(vec![
        Box::new(i4.clone()),
        Box::new(i3.clone()),
        Box::new(i2.clone()),
        Box::new(i1.clone()),
    ]).unwrap();
    check_result_rev(iter, result);
}

#[test]
fn test_merge_empty_iter() {
    let iter = MergeIter::<MockIter>::create(vec![]).unwrap();
    check_result(iter, vec![]);
}

#[cfg(test)]
fn check_result_two(iter: impl StorageIter, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert_eq!(&iter.front_entry().unwrap().0[..], k.as_ref());
        assert_eq!(&iter.front_entry().unwrap().1[..], v.as_ref());
    }
    assert!(!iter.is_valid());
}

#[cfg(test)]
fn check_result_two_rev(iter: impl StorageIter, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        assert!(iter.is_valid());
        iter.next_back().unwrap();
        assert_eq!(&iter.back_entry().unwrap().0[..], k.as_ref());
        assert_eq!(&iter.back_entry().unwrap().1[..], v.as_ref());
    }
    assert!(!iter.is_valid());
}

#[cfg(test)]
fn check_result_two_random(iter: impl StorageIter, expected: Vec<(Bytes, Bytes)>) {
    use rand::Rng;
    let mut iter = iter;
    let mut forward = 0;
    let mut backward = expected.len();
    for _ in 0..expected.len() {
        match rand::thread_rng().gen_range(0..=1) {
            1 => {
                let (k, v) = &expected[forward];
                assert!(iter.is_valid());
                iter.next().unwrap();
                assert_eq!(&iter.front_entry().unwrap().0[..], k.as_ref());
                assert_eq!(&iter.front_entry().unwrap().1[..], v.as_ref());
                forward += 1;
            },

            0 => {
                backward -= 1;
                let (k, v) = &expected[backward];
                assert!(iter.is_valid());
                iter.next_back().unwrap();
                assert_eq!(&iter.back_entry().unwrap().0[..], k.as_ref());
                assert_eq!(&iter.back_entry().unwrap().1[..], v.as_ref());
            },

            _ => { assert!(false) },
        };
    }
    assert!(!iter.is_valid());
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
    check_result_two_random(
        iter_rev,
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

#[test]
fn test_merge_2_two_iter() {
    let i2 = MockIter::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i1 = MockIter::new(vec![
        (Bytes::from("a"), Bytes::from("1.2")),
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let iter_rev = TwoMergeIter::create(i1.clone(), i2.clone()).unwrap();
    check_result_two_rev(
        iter_rev,
        vec![
            (Bytes::from("d"), Bytes::from("4.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("a"), Bytes::from("1.2")),
        ],
    );
    let iter_rev = TwoMergeIter::create(i1.clone(), i2.clone()).unwrap();
    check_result_two_random(
        iter_rev,
        vec![
            (Bytes::from("a"), Bytes::from("1.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    );
    let iter = TwoMergeIter::create(i1, i2).unwrap();
    check_result_two(
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
fn test_merge_3_two_iter() {
    let i2 = MockIter::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
    ]);
    let i1 = MockIter::new(vec![
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let iter_rev = TwoMergeIter::create(i1.clone(), i2.clone()).unwrap();
    check_result_two_rev(
        iter_rev,
        vec![
            (Bytes::from("d"), Bytes::from("4.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("a"), Bytes::from("1.1")),
        ],
    );
    let iter_rev = TwoMergeIter::create(i1.clone(), i2.clone()).unwrap();
    check_result_two_random(
        iter_rev,
        vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    );
    let iter = TwoMergeIter::create(i1, i2).unwrap();
    check_result_two(
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
fn test_merge_4_two_iter() {
    let i2 = MockIter::new(vec![]);
    let i1 = MockIter::new(vec![
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let iter_rev = TwoMergeIter::create(i1.clone(), i2.clone()).unwrap();
    check_result_two_rev(
        iter_rev,
        vec![
            (Bytes::from("d"), Bytes::from("4.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
        ],
    );
    let iter = TwoMergeIter::create(i1, i2).unwrap();
    check_result_two(
        iter,
        vec![
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    );
    let i1 = MockIter::new(vec![]);
    let i2 = MockIter::new(vec![
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let iter_rev = TwoMergeIter::create(i1.clone(), i2.clone()).unwrap();
    check_result_two_rev(
        iter_rev,
        vec![
            (Bytes::from("d"), Bytes::from("4.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
        ],
    );
    let iter = TwoMergeIter::create(i1, i2).unwrap();
    check_result_two(
        iter,
        vec![
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ],
    );
}

#[test]
fn test_merge_5_two_iter() {
    let i2 = MockIter::new(vec![]);
    let i1 = MockIter::new(vec![]);
    let iter_rev = TwoMergeIter::create(i1.clone(), i2.clone()).unwrap();
    check_result_two_rev(iter_rev, vec![]);
    let iter = TwoMergeIter::create(i1, i2).unwrap();
    check_result_two(iter, vec![])
}