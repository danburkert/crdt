use std::cmp;

#[cfg(any(quickcheck, test))]
use quickcheck::{Arbitrary, Gen};

/// `Pn` is a building block for count-based CRDTs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Pn {
    /// The positive count.
    pub p: u64,

    /// The negative count.
    pub n: u64,
}

impl Pn {

    /// Creates a new zeroes `Pn` instance.
    pub fn new() -> Pn {
        Pn { p: 0, n: 0 }
    }

    /// Gets the current count of a `Pn`.
    pub fn count(&self) -> i64 {
        self.p as i64 - self.n as i64
    }

    /// Increments the `Pn` by an amount.
    pub fn increment(&mut self, amount: i64) {
        if amount >= 0 {
            self.p += amount as u64;
        } else {
            self.n += amount.abs() as u64;
        }
    }

    /// Merges another `Pn` into this one.
    pub fn merge(&mut self, other: Pn) {
        self.p = cmp::max(self.p, other.p);
        self.n = cmp::max(self.n, other.n);
    }
}

#[cfg(any(quickcheck, test))]
impl Arbitrary for Pn {
    fn arbitrary<G>(g: &mut G) -> Pn where G: Gen {
        Pn { p: Arbitrary::arbitrary(g), n: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Iterator<Item=Pn> + 'static> {
        Box::new((self.p, self.n).shrink().map(|(p, n)| Pn { p: p, n: n }))
    }
}
