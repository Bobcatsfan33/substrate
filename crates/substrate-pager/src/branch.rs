//! Branch trees: named heads and tags over the manifest DAG.
//!
//! A manifest is a database *state*. A **branch** is a moving pointer at one — and that is the whole
//! difference. `main` is not a thing; it is a name for whichever manifest is currently the tip.
//!
//! ```text
//!                        ┌── "experiment"  ──► M7
//!                        │
//!   M0 ──► M1 ──► M2 ──► M3 ──► M4 ──► M5  ◄── "main"
//!                 │                     │
//!                 └── "v1.0" (tag)      └── "release" (tag, frozen)
//! ```
//!
//! Every operation here is a pointer move: creating a branch, rewinding one, tagging a release. None
//! of them copies a byte, because the thing being pointed at is immutable.
//!
//! # Branches and tags are different on purpose
//!
//! A **branch** moves. A **tag** does not — it is a promise that a name will always mean the same
//! bytes. Overwriting a tag silently is how "we deployed v1.0" stops being a fact, so [`retag`] must
//! be called deliberately and [`tag`] refuses to clobber.
//!
//! [`retag`]: BranchTree::retag
//! [`tag`]: BranchTree::tag

use crate::error::{PagerError, Result};
use crate::manifest::ManifestId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The name of a branch or a tag.
pub type RefName = String;

/// The DAG of named heads for one database.
///
/// Serializable, so a fleet registry can hold one per database and a session can hold one per agent.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchTree {
    /// Named heads. These move.
    branches: BTreeMap<RefName, ManifestId>,
    /// Named states. These do not.
    tags: BTreeMap<RefName, ManifestId>,
}

impl BranchTree {
    /// An empty tree.
    pub fn new() -> Self {
        BranchTree::default()
    }

    /// A tree with a single branch at a root manifest.
    ///
    /// ```
    /// # use substrate_pager::{BranchTree, ManifestId};
    /// let root = ManifestId::from_bytes([0; 32]);
    /// let tree = BranchTree::rooted("main", root);
    /// assert_eq!(tree.head("main"), Some(root));
    /// ```
    pub fn rooted(branch: impl Into<RefName>, at: ManifestId) -> Self {
        let mut tree = BranchTree::new();
        tree.branches.insert(branch.into(), at);
        tree
    }

    /// Where a branch currently points.
    pub fn head(&self, branch: &str) -> Option<ManifestId> {
        self.branches.get(branch).copied()
    }

    /// What a tag names.
    pub fn tagged(&self, tag: &str) -> Option<ManifestId> {
        self.tags.get(tag).copied()
    }

    /// Every branch, in name order.
    pub fn branches(&self) -> impl Iterator<Item = (&str, ManifestId)> {
        self.branches.iter().map(|(name, id)| (name.as_str(), *id))
    }

    /// Every tag, in name order.
    pub fn tags(&self) -> impl Iterator<Item = (&str, ManifestId)> {
        self.tags.iter().map(|(name, id)| (name.as_str(), *id))
    }

    /// Create a branch at a manifest. Fails if the name is taken.
    ///
    /// Refusing to overwrite is deliberate: `branch("main", ...)` on a repository that already has a
    /// `main` is nearly always a mistake, and silently moving it would discard whatever `main` was
    /// pointing at. Use [`BranchTree::set_head`] to move a branch on purpose.
    pub fn branch(&mut self, name: impl Into<RefName>, at: ManifestId) -> Result<()> {
        let name = name.into();
        if self.branches.contains_key(&name) {
            return Err(PagerError::RefExists {
                kind: "branch",
                name,
            });
        }
        self.branches.insert(name, at);
        Ok(())
    }

    /// Move a branch. **O(1)** — this is `commit`, `rewind`, and `reset` all at once.
    ///
    /// The abandoned suffix is not deleted; it stays readable until GC decides nothing points at it.
    /// That is what lets an agent explore three hypotheses, discard two, and still audit what it
    /// discarded (docs/03 §3.1).
    pub fn set_head(&mut self, branch: &str, to: ManifestId) -> Result<ManifestId> {
        let slot = self
            .branches
            .get_mut(branch)
            .ok_or_else(|| PagerError::NoSuchRef {
                kind: "branch",
                name: branch.to_string(),
            })?;
        let previous = *slot;
        *slot = to;
        Ok(previous)
    }

    /// Delete a branch. The manifests it pointed at survive until GC.
    pub fn delete_branch(&mut self, branch: &str) -> Result<ManifestId> {
        self.branches
            .remove(branch)
            .ok_or_else(|| PagerError::NoSuchRef {
                kind: "branch",
                name: branch.to_string(),
            })
    }

    /// Name a state, permanently. Fails if the tag exists.
    ///
    /// A tag that can be silently moved is not a tag, it is a branch with worse marketing — and
    /// "we shipped v1.0" stops being a checkable fact.
    pub fn tag(&mut self, name: impl Into<RefName>, at: ManifestId) -> Result<()> {
        let name = name.into();
        if let Some(existing) = self.tags.get(&name) {
            if *existing == at {
                return Ok(()); // idempotent: tagging the same state with the same name is a no-op
            }
            return Err(PagerError::RefExists { kind: "tag", name });
        }
        self.tags.insert(name, at);
        Ok(())
    }

    /// Move a tag anyway, deliberately.
    ///
    /// Exists because sometimes you really do have to re-cut a release. It is a separate function
    /// from [`BranchTree::tag`] so that it is impossible to do by accident, and so that it is greppable
    /// when someone asks how a tag moved.
    pub fn retag(&mut self, name: impl Into<RefName>, at: ManifestId) -> Option<ManifestId> {
        self.tags.insert(name.into(), at)
    }

    /// Every manifest any branch or tag points at.
    ///
    /// **These are GC's roots.** Anything not reachable from one of them — through parent pointers
    /// and overlay bases — is garbage.
    pub fn roots(&self) -> Vec<ManifestId> {
        let mut roots: Vec<ManifestId> = self.branches.values().copied().collect();
        roots.extend(self.tags.values().copied());
        roots.sort_unstable();
        roots.dedup();
        roots
    }

    /// How many branches.
    pub fn len(&self) -> usize {
        self.branches.len()
    }

    /// True if there are no branches.
    pub fn is_empty(&self) -> bool {
        self.branches.is_empty()
    }

    /// Serialize, for a registry to hold.
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|source| PagerError::Codec {
            op: "encode",
            source,
        })
    }

    /// Deserialize.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|source| PagerError::Codec {
            op: "decode",
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(n: u8) -> ManifestId {
        ManifestId::from_bytes([n; 32])
    }

    #[test]
    fn branches_move_and_tags_do_not() -> Result<()> {
        let mut tree = BranchTree::rooted("main", m(1));

        // A branch moves.
        let previous = tree.set_head("main", m(2))?;
        assert_eq!(previous, m(1));
        assert_eq!(tree.head("main"), Some(m(2)));

        // A tag does not.
        tree.tag("v1.0", m(2))?;
        assert!(
            tree.tag("v1.0", m(3)).is_err(),
            "a tag that can be silently moved is not a tag"
        );
        assert_eq!(tree.tagged("v1.0"), Some(m(2)));

        // ...unless you say so, out loud, in a different function.
        assert_eq!(tree.retag("v1.0", m(3)), Some(m(2)));
        assert_eq!(tree.tagged("v1.0"), Some(m(3)));
        Ok(())
    }

    #[test]
    fn tagging_the_same_state_twice_is_a_no_op_not_an_error() -> Result<()> {
        let mut tree = BranchTree::rooted("main", m(1));
        tree.tag("v1.0", m(1))?;
        tree.tag("v1.0", m(1))?; // idempotent — the tag already means exactly this
        Ok(())
    }

    #[test]
    fn creating_a_branch_that_exists_is_refused() -> Result<()> {
        let mut tree = BranchTree::rooted("main", m(1));
        assert!(
            tree.branch("main", m(2)).is_err(),
            "silently moving main would discard whatever it pointed at"
        );
        tree.branch("experiment", m(1))?;
        assert_eq!(tree.head("experiment"), Some(m(1)));
        Ok(())
    }

    #[test]
    fn moving_a_branch_that_does_not_exist_is_an_error_not_a_creation() {
        let mut tree = BranchTree::new();
        assert!(matches!(
            tree.set_head("ghost", m(1)),
            Err(PagerError::NoSuchRef { .. })
        ));
    }

    #[test]
    fn roots_are_every_branch_and_every_tag() -> Result<()> {
        // GC's correctness depends on this being complete. A tag left out of `roots()` is a tagged
        // release that gets garbage collected, which is a very bad afternoon.
        let mut tree = BranchTree::rooted("main", m(1));
        tree.branch("experiment", m(2))?;
        tree.tag("v1.0", m(3))?;

        let roots = tree.roots();
        assert!(roots.contains(&m(1)));
        assert!(roots.contains(&m(2)));
        assert!(roots.contains(&m(3)), "a tag is a GC root too");
        assert_eq!(roots.len(), 3);
        Ok(())
    }

    #[test]
    fn a_deleted_branch_stops_being_a_root() -> Result<()> {
        let mut tree = BranchTree::rooted("main", m(1));
        tree.branch("doomed", m(2))?;
        assert_eq!(tree.delete_branch("doomed")?, m(2));
        assert!(!tree.roots().contains(&m(2)));
        assert!(tree.delete_branch("doomed").is_err());
        Ok(())
    }

    #[test]
    fn round_trips_through_bytes() -> Result<()> {
        let mut tree = BranchTree::rooted("main", m(1));
        tree.branch("experiment", m(2))?;
        tree.tag("v1.0", m(3))?;
        assert_eq!(BranchTree::decode(&tree.encode()?)?, tree);
        Ok(())
    }
}
