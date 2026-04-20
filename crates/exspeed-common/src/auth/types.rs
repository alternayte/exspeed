use std::sync::Arc;

use enumset::{EnumSet, EnumSetType};
use serde::Deserialize;

use crate::auth::glob::StreamGlob;
use crate::types::StreamName;

/// Verbs a credential can hold. `EnumSetType` gives us compact `EnumSet<Action>`
/// (u8 bitset under the hood) plus ergonomic contains/insert/union.
#[derive(Debug, EnumSetType, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Publish,
    Subscribe,
    Admin,
    /// Cluster-internal: allows a follower pod to receive replication events
    /// from the leader via the cluster port. Orthogonal to all other verbs.
    Replicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Permission {
    pub streams: StreamGlob,
    pub actions: EnumSet<Action>,
}

/// An authenticated principal. Returned by `CredentialStore::lookup` and
/// carried on the TCP connection + in HTTP request extensions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub name: String,
    pub permissions: Vec<Permission>,
}

impl Identity {
    /// True if *any* permission grants `action` on a matching `stream`.
    /// Iterates all permissions; ordering doesn't matter (allowlist-only,
    /// no deny rules in v1).
    pub fn authorize(&self, action: Action, stream: &StreamName) -> bool {
        self.permissions
            .iter()
            .any(|p| p.actions.contains(action) && p.streams.matches(stream))
    }

    /// Does this identity hold the `Admin` verb anywhere? Coarse HTTP gate.
    pub fn has_any_admin_permission(&self) -> bool {
        self.permissions
            .iter()
            .any(|p| p.actions.contains(Action::Admin))
    }

    /// Does this identity have `Admin` with a wildcard-all glob? Required
    /// for HTTP endpoints that don't target a specific stream (connectors,
    /// queries, views, connections).
    pub fn has_global_admin(&self) -> bool {
        self.permissions
            .iter()
            .any(|p| p.actions.contains(Action::Admin) && p.streams.is_wildcard_all())
    }
}

/// Arc'd handle. Cheap to clone. Used on both TCP and HTTP paths.
pub type IdentityRef = Arc<Identity>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StreamName;

    fn n(s: &str) -> StreamName {
        StreamName::try_from(s).unwrap()
    }

    fn ident(perms: Vec<(&str, &[Action])>) -> Identity {
        Identity {
            name: "test".into(),
            permissions: perms
                .into_iter()
                .map(|(g, acts)| Permission {
                    streams: StreamGlob::compile(g, "test").unwrap(),
                    actions: acts.iter().copied().collect(),
                })
                .collect(),
        }
    }

    #[test]
    fn authorize_allow_on_glob_and_verb_match() {
        let id = ident(vec![("orders-*", &[Action::Publish, Action::Subscribe])]);
        assert!(id.authorize(Action::Publish, &n("orders-placed")));
    }

    #[test]
    fn authorize_deny_when_verb_absent() {
        let id = ident(vec![("orders-*", &[Action::Subscribe])]);
        assert!(!id.authorize(Action::Publish, &n("orders-placed")));
    }

    #[test]
    fn authorize_deny_when_glob_misses() {
        let id = ident(vec![("orders-*", &[Action::Publish])]);
        assert!(!id.authorize(Action::Publish, &n("payments-placed")));
    }

    #[test]
    fn authorize_iterates_all_permissions() {
        let id = ident(vec![
            ("orders-*", &[Action::Publish]),
            ("audit-log", &[Action::Publish]),
        ]);
        assert!(id.authorize(Action::Publish, &n("audit-log")));
    }

    #[test]
    fn has_any_admin_permission() {
        let scoped_admin = ident(vec![("team-a-*", &[Action::Admin])]);
        assert!(scoped_admin.has_any_admin_permission());
        assert!(!scoped_admin.has_global_admin());

        let global = ident(vec![("*", &[Action::Admin])]);
        assert!(global.has_any_admin_permission());
        assert!(global.has_global_admin());

        let non_admin = ident(vec![("*", &[Action::Publish, Action::Subscribe])]);
        assert!(!non_admin.has_any_admin_permission());
        assert!(!non_admin.has_global_admin());
    }

    #[test]
    fn authorize_replicate_verb_separate_from_admin() {
        // A pure replicator identity: can replicate but not admin.
        let rep_only = ident(vec![("*", &[Action::Replicate])]);
        assert!(rep_only.authorize(Action::Replicate, &n("any-stream")));
        assert!(!rep_only.authorize(Action::Admin, &n("any-stream")));

        // An admin identity without Replicate cannot replicate — admin is not
        // a superset.
        let admin_only = ident(vec![("*", &[Action::Admin])]);
        assert!(!admin_only.authorize(Action::Replicate, &n("any-stream")));
    }

    #[test]
    fn empty_permissions_is_deny_all() {
        let id = Identity {
            name: "empty".into(),
            permissions: vec![],
        };
        assert!(!id.authorize(Action::Publish, &n("x")));
        assert!(!id.authorize(Action::Admin, &n("x")));
        assert!(!id.has_any_admin_permission());
    }
}
