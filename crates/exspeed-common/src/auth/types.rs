use std::sync::Arc;

use enumset::{EnumSet, EnumSetType};
use serde::Deserialize;

use crate::auth::glob::StreamGlob;
use crate::subject::SubjectFilter;
use crate::types::StreamName;

/// Subjects of core (non-persistent) messages that carry request-reply
/// responses. Anyone may publish to them; subscribing needs a concrete inbox
/// (`_INBOX.<id>`, optionally with a wildcard after it).
pub const INBOX_PREFIX: &str = "_INBOX";

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

/// `publish` / `subscribe` on core (non-persistent) message subjects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectPermission {
    pub subjects: SubjectFilter,
    pub actions: EnumSet<Action>,
}

/// An authenticated principal. Returned by `CredentialStore::lookup` and
/// carried on the TCP connection + in HTTP request extensions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub name: String,
    pub permissions: Vec<Permission>,
    /// Permissions on core message subjects.
    pub subject_permissions: Vec<SubjectPermission>,
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

    /// Whether this identity may publish (`Publish`) to, or subscribe
    /// (`Subscribe`) with, the core-message subject filter `filter`.
    /// Subscribing needs a permission covering every subject the filter
    /// matches. A wildcard-all stream permission (`streams = "*"`) grants
    /// the same verbs on every subject. Replies (`_INBOX.…`) may always be
    /// published, and a concrete inbox may always be subscribed to.
    pub fn authorize_subject(&self, action: Action, filter: &SubjectFilter) -> bool {
        if is_inbox(filter, action) {
            return true;
        }
        self.permissions
            .iter()
            .any(|p| p.actions.contains(action) && p.streams.is_wildcard_all())
            || self
                .subject_permissions
                .iter()
                .any(|p| p.actions.contains(action) && p.subjects.covers(filter))
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

/// `_INBOX.<id>` (plus anything after it) for a subscription; any
/// `_INBOX.…` subject for a publish.
fn is_inbox(filter: &SubjectFilter, action: Action) -> bool {
    let p = filter.as_pattern();
    let mut tokens = p.split('.');
    if tokens.next() != Some(INBOX_PREFIX) {
        return false;
    }
    match action {
        Action::Publish => filter.is_literal() && tokens.next().is_some(),
        Action::Subscribe => tokens.next().is_some_and(|id| id != "*" && id != ">"),
        _ => false,
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
            subject_permissions: Vec::new(),
        }
    }

    fn f(s: &str) -> SubjectFilter {
        SubjectFilter::parse(s).unwrap()
    }

    #[test]
    fn subject_authorization() {
        let mut id = ident(vec![("orders-*", &[Action::Publish, Action::Subscribe])]);
        id.subject_permissions.push(SubjectPermission {
            subjects: f("rpc.>"),
            actions: Action::Subscribe.into(),
        });
        assert!(id.authorize_subject(Action::Subscribe, &f("rpc.users.*")));
        assert!(!id.authorize_subject(Action::Subscribe, &f(">")));
        assert!(!id.authorize_subject(Action::Publish, &f("rpc.users.get")));
        // A stream glob that isn't `*` grants nothing on subjects.
        assert!(!id.authorize_subject(Action::Publish, &f("orders-1")));
        // Inboxes: replies may always be published; a concrete inbox may
        // always be subscribed to, but not every inbox at once.
        assert!(id.authorize_subject(Action::Publish, &f("_INBOX.abc.1")));
        assert!(id.authorize_subject(Action::Subscribe, &f("_INBOX.abc.*")));
        assert!(!id.authorize_subject(Action::Subscribe, &f("_INBOX.>")));
        assert!(!id.authorize_subject(Action::Subscribe, &f("_INBOX.*.x")));
        assert!(!id.authorize_subject(Action::Publish, &f("_INBOX")));
        // Global stream permissions cover subjects too.
        let admin = ident(vec![("*", &[Action::Publish, Action::Subscribe])]);
        assert!(admin.authorize_subject(Action::Subscribe, &f(">")));
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
            subject_permissions: vec![],
        };
        assert!(!id.authorize(Action::Publish, &n("x")));
        assert!(!id.authorize(Action::Admin, &n("x")));
        assert!(!id.has_any_admin_permission());
    }
}
