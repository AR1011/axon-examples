//! Who may do what with a document: one table (design §4.2) that every route and every websocket frame asks, and the
//! document's sharing, which the document itself keeps and journals.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::wire::{Malformed, Reader, Writer};

/// A member's level, or a share link's. `None` is a link that is off.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    #[default]
    None,
    View,
    Edit,
}

impl Level {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "off" | "none" => Some(Self::None),
            "view" => Some(Self::View),
            "edit" => Some(Self::Edit),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::None => "off",
            Self::View => "view",
            Self::Edit => "edit",
        }
    }

    pub fn from_u8(b: u8) -> Result<Self, Malformed> {
        [Self::None, Self::View, Self::Edit].get(b as usize).copied().ok_or(Malformed)
    }
}

/// How a member came in: asked by the owner, or by opening the share link.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Via {
    Invited,
    Link,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Member {
    pub level: Level,
    pub via: Via,
}

/// A document's sharing. The document is its authority; the catalog keeps a copy to list it by.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Acl {
    pub owner: String,
    pub link: Level,
    /// Raised by a link reset: a link or a guest token of an older version lets no one in.
    pub link_version: u64,
    pub members: BTreeMap<String, Member>,
    /// When the owner moved it to the trash, in Unix ms.
    pub trashed_ms: Option<u64>,
    /// Anyone may read the text at `/p/<id>`, while it is not in the trash.
    pub published: bool,
}

/// Who asks: a signed-in user, a guest of a view link at the link's version when the guest got in, anyone at the
/// published page, or an operator, who may only give a document a new owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Principal {
    User(String),
    Guest { name: String, link_version: u64 },
    Reader,
    Operator(String),
}

impl Principal {
    pub fn name(&self) -> &str {
        match self {
            Self::User(name) | Self::Guest { name, .. } | Self::Operator(name) => name,
            Self::Reader => "",
        }
    }

    pub fn user(&self) -> Option<&str> {
        match self {
            Self::User(name) => Some(name),
            Self::Guest { .. } | Self::Reader | Self::Operator(_) => None,
        }
    }

    pub fn write(&self, w: &mut Writer) {
        match self {
            Self::User(name) => w.u8(0).str(name),
            Self::Guest { name, link_version } => w.u8(1).str(name).uint(*link_version),
            Self::Reader => w.u8(2),
            Self::Operator(name) => w.u8(3).str(name),
        };
    }

    pub fn read(r: &mut Reader<'_>) -> Result<Self, Malformed> {
        match r.u8()? {
            0 => Ok(Self::User(r.string()?)),
            1 => Ok(Self::Guest { name: r.string()?, link_version: r.uint()? }),
            2 => Ok(Self::Reader),
            3 => Ok(Self::Operator(r.string()?)),
            _ => Err(Malformed),
        }
    }
}

/// A column of the permission table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Owner,
    EditMember,
    ViewMember,
    ViewGuest,
    /// Reaches only a published page.
    Reader,
}

/// A row of the permission table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Open, read live, see presence.
    Open,
    /// Edit text, import (replace).
    Edit,
    Export,
    /// History list, view, diff, authors.
    History,
    NameVersion,
    Restore,
    /// See people and share settings, read-only for members.
    SeePeople,
    /// Change sharing, invite, remove people, publish.
    Share,
    /// Move to trash, untrash, delete for good.
    Trash,
    /// Remove from my list.
    Hide,
    ReadPublished,
}

impl Action {
    pub const ALL: [Action; 11] = [
        Self::Open,
        Self::Edit,
        Self::Export,
        Self::History,
        Self::NameVersion,
        Self::Restore,
        Self::SeePeople,
        Self::Share,
        Self::Trash,
        Self::Hide,
        Self::ReadPublished,
    ];
}

impl Role {
    pub const ALL: [Role; 5] = [Self::Owner, Self::EditMember, Self::ViewMember, Self::ViewGuest, Self::Reader];

    /// The table, design §4.2.
    pub fn may(self, action: Action) -> bool {
        use Action::*;
        use Role::*;
        match action {
            Open | Export => !matches!(self, Reader),
            Edit | NameVersion | Restore => matches!(self, Owner | EditMember),
            History | SeePeople => matches!(self, Owner | EditMember | ViewMember),
            Share | Trash => self == Owner,
            Hide => matches!(self, EditMember | ViewMember),
            ReadPublished => true,
        }
    }
}

/// Why an action was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    Forbidden,
    /// In the trash: only the owner may act, and only to untrash or delete for good.
    Trashed,
}

impl Acl {
    pub fn new(owner: &str) -> Self {
        Self { owner: owner.into(), ..Self::default() }
    }

    /// The principal's column, `None` for one with no access.
    pub fn role(&self, who: &Principal) -> Option<Role> {
        match who {
            Principal::User(name) if *name == self.owner => Some(Role::Owner),
            Principal::User(name) => match self.members.get(name)?.level {
                Level::Edit => Some(Role::EditMember),
                Level::View => Some(Role::ViewMember),
                Level::None => None,
            },
            // A guest of the current link, while it lets anyone in. An edit link lets a guest view (Q6).
            Principal::Guest { link_version, .. } => {
                (*link_version == self.link_version && self.link >= Level::View).then_some(Role::ViewGuest)
            }
            Principal::Reader => self.published.then_some(Role::Reader),
            Principal::Operator(_) => None,
        }
    }

    /// The principal's level in the document: what its socket may do.
    pub fn level(&self, who: &Principal) -> Level {
        match self.role(who) {
            Some(Role::Owner | Role::EditMember) => Level::Edit,
            Some(Role::ViewMember | Role::ViewGuest) => Level::View,
            Some(Role::Reader) | None => Level::None,
        }
    }

    pub fn check(&self, who: &Principal, action: Action) -> Result<Role, Refused> {
        let role = self.role(who).filter(|role| role.may(action)).ok_or(Refused::Forbidden)?;
        match self.trashed_ms {
            Some(_) if role == Role::Owner && action == Action::Trash => Ok(role),
            Some(_) => Err(Refused::Trashed),
            None => Ok(role),
        }
    }

    pub fn write(&self, w: &mut Writer) {
        // Published is a bit of the link's byte: an ACL written before there was publishing reads as it was.
        w.str(&self.owner).u8(self.link as u8 | u8::from(self.published) << 2).uint(self.link_version);
        w.uint(self.trashed_ms.map_or(0, |ms| ms + 1)).uint(self.members.len() as u64);
        for (name, member) in &self.members {
            w.str(name).u8(member.level as u8).u8(member.via as u8);
        }
    }

    pub fn read(r: &mut Reader<'_>) -> Result<Self, Malformed> {
        let (owner, link) = (r.string()?, r.u8()?);
        let (published, link, link_version) = (link & 4 != 0, Level::from_u8(link & !4)?, r.uint()?);
        let trashed_ms = r.uint()?.checked_sub(1);
        let mut members = BTreeMap::new();
        for _ in 0..r.uint()? {
            let name = r.string()?;
            let level = Level::from_u8(r.u8()?)?;
            let via = [Via::Invited, Via::Link].get(r.u8()? as usize).copied().ok_or(Malformed)?;
            members.insert(name, Member { level, via });
        }
        Ok(Self { owner, link, link_version, members, trashed_ms, published })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.write(&mut w);
        w.into_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Malformed> {
        let mut r = Reader::new(bytes);
        let acl = Self::read(&mut r)?;
        r.end()?;
        Ok(acl)
    }
}

/// A change of a document's sharing, by whom it is asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    Link(Level),
    /// A new link secret: the old link and its guests let no one in. Members stay.
    ResetLink,
    /// Invites a user, or sets a member's level.
    Member(String, Level),
    RemoveMember(String),
    /// Removes every member who came in by the link.
    RemoveLinkMembers,
    /// A signed-in user opened the link at this version and becomes a member at its level.
    JoinLink(u64),
    Trash(u64),
    Untrash,
    Publish(bool),
    /// An operator gives the document to another user. The owner before stays, as an edit member.
    Owner(String),
}

impl Change {
    /// Applies the change if `who` may make it: the new ACL, which may equal the old one.
    pub fn apply(&self, acl: &Acl, who: &Principal) -> Result<Acl, Refused> {
        let mut next = acl.clone();
        match self {
            Self::JoinLink(version) => {
                let user = who.user().ok_or(Refused::Forbidden)?;
                if acl.trashed_ms.is_some() {
                    return Err(Refused::Trashed);
                }
                if *version != acl.link_version || acl.link == Level::None {
                    return Err(Refused::Forbidden);
                }
                if user != acl.owner && acl.level(who) < acl.link {
                    next.members.insert(user.into(), Member { level: acl.link, via: Via::Link });
                }
                return Ok(next);
            }
            Self::Owner(user) => {
                let Principal::Operator(_) = who else { return Err(Refused::Forbidden) };
                if *user != acl.owner {
                    next.members.remove(user);
                    next.members.insert(acl.owner.clone(), Member { level: Level::Edit, via: Via::Invited });
                    next.owner = user.clone();
                }
                return Ok(next);
            }
            Self::Trash(_) | Self::Untrash => {
                if acl.role(who) != Some(Role::Owner) {
                    return Err(Refused::Forbidden);
                }
                next.trashed_ms = match self {
                    Self::Trash(ms) => Some(acl.trashed_ms.unwrap_or(*ms)),
                    _ => None,
                };
                return Ok(next);
            }
            _ => {}
        }
        acl.check(who, Action::Share)?;
        match self {
            Self::Link(level) => next.link = *level,
            Self::ResetLink => next.link_version += 1,
            Self::Member(user, Level::None) | Self::RemoveMember(user) => {
                next.members.remove(user);
            }
            Self::Member(user, _) if *user == acl.owner => {}
            Self::Member(user, level) => {
                next.members.insert(user.clone(), Member { level: *level, via: Via::Invited });
            }
            Self::RemoveLinkMembers => next.members.retain(|_, m| m.via != Via::Link),
            Self::Publish(published) => next.published = *published,
            Self::JoinLink(_) | Self::Trash(_) | Self::Untrash | Self::Owner(_) => unreachable!("handled above"),
        }
        Ok(next)
    }

    pub fn write(&self, w: &mut Writer) {
        match self {
            Self::Link(level) => w.u8(0).u8(*level as u8),
            Self::ResetLink => w.u8(1),
            Self::Member(user, level) => w.u8(2).str(user).u8(*level as u8),
            Self::RemoveMember(user) => w.u8(3).str(user),
            Self::RemoveLinkMembers => w.u8(4),
            Self::JoinLink(version) => w.u8(5).uint(*version),
            Self::Trash(ms) => w.u8(6).uint(*ms),
            Self::Untrash => w.u8(7),
            Self::Publish(published) => w.u8(8).u8(u8::from(*published)),
            Self::Owner(user) => w.u8(9).str(user),
        };
    }

    pub fn read(r: &mut Reader<'_>) -> Result<Self, Malformed> {
        Ok(match r.u8()? {
            0 => Self::Link(Level::from_u8(r.u8()?)?),
            1 => Self::ResetLink,
            2 => Self::Member(r.string()?, Level::from_u8(r.u8()?)?),
            3 => Self::RemoveMember(r.string()?),
            4 => Self::RemoveLinkMembers,
            5 => Self::JoinLink(r.uint()?),
            6 => Self::Trash(r.uint()?),
            7 => Self::Untrash,
            8 => Self::Publish(r.u8()? != 0),
            9 => Self::Owner(r.string()?),
            _ => return Err(Malformed),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(name: &str) -> Principal {
        Principal::User(name.into())
    }

    fn shared() -> Acl {
        let mut acl = Acl::new("ana");
        acl.link = Level::View;
        acl.link_version = 3;
        acl.members.insert("ben".into(), Member { level: Level::Edit, via: Via::Invited });
        acl.members.insert("cid".into(), Member { level: Level::View, via: Via::Link });
        acl
    }

    /// The table of design §4.2, row by row: owner, edit member, view member, view guest, published reader.
    #[test]
    fn the_permission_table() {
        use Action::*;
        let rows = [
            (Open, [true, true, true, true, false]),
            (Edit, [true, true, false, false, false]),
            (Export, [true, true, true, true, false]),
            (History, [true, true, true, false, false]),
            (NameVersion, [true, true, false, false, false]),
            (Restore, [true, true, false, false, false]),
            (SeePeople, [true, true, true, false, false]),
            (Share, [true, false, false, false, false]),
            (Trash, [true, false, false, false, false]),
            (Hide, [false, true, true, false, false]),
            (ReadPublished, [true, true, true, true, true]),
        ];
        assert_eq!(rows.len(), Action::ALL.len());
        for (action, want) in rows {
            let got: Vec<bool> = Role::ALL.iter().map(|role| role.may(action)).collect();
            assert_eq!(got, want, "{action:?}");
        }
    }

    #[test]
    fn roles_come_from_the_owner_the_members_and_the_current_link() {
        let acl = shared();
        assert_eq!(acl.role(&user("ana")), Some(Role::Owner));
        assert_eq!(acl.role(&user("ben")), Some(Role::EditMember));
        assert_eq!(acl.role(&user("cid")), Some(Role::ViewMember));
        assert_eq!(acl.role(&user("dee")), None, "a signed-in user who never joined");
        let guest = |v| Principal::Guest { name: "eve".into(), link_version: v };
        assert_eq!(acl.role(&guest(3)), Some(Role::ViewGuest));
        assert_eq!(acl.role(&guest(2)), None, "a guest of a link that was reset");
        let mut edit = acl.clone();
        edit.link = Level::Edit;
        assert_eq!(edit.level(&guest(3)), Level::View, "guests never edit (Q6)");
        let mut off = acl.clone();
        off.link = Level::None;
        assert_eq!(off.role(&guest(3)), None);
        assert_eq!(off.role(&user("cid")), Some(Role::ViewMember), "members by link stay when it is off (Q9)");
        assert_eq!(acl.role(&Principal::Reader), None, "not published");
        let mut published = acl.clone();
        published.published = true;
        assert_eq!(published.check(&Principal::Reader, Action::ReadPublished), Ok(Role::Reader));
        assert_eq!(published.check(&Principal::Reader, Action::Export), Err(Refused::Forbidden));
        published.trashed_ms = Some(1);
        assert_eq!(published.check(&Principal::Reader, Action::ReadPublished), Err(Refused::Trashed));
        assert_eq!(acl.role(&Principal::Operator("op".into())), None, "an operator reads nothing");
    }

    #[test]
    fn only_the_owner_publishes_and_only_an_operator_gives_a_new_owner() {
        let acl = shared();
        assert_eq!(Change::Publish(true).apply(&acl, &user("ben")), Err(Refused::Forbidden));
        assert!(Change::Publish(true).apply(&acl, &user("ana")).unwrap().published);
        let op = Principal::Operator("op".into());
        assert_eq!(Change::Publish(true).apply(&acl, &op), Err(Refused::Forbidden));
        assert_eq!(Change::Owner("ben".into()).apply(&acl, &user("ana")), Err(Refused::Forbidden), "not the owner");
        let given = Change::Owner("ben".into()).apply(&acl, &op).unwrap();
        assert_eq!(given.owner, "ben");
        assert_eq!(given.members.get("ben"), None, "the owner is not a member");
        assert_eq!(given.members["ana"], Member { level: Level::Edit, via: Via::Invited });
        assert_eq!(Change::Owner("ben".into()).apply(&given, &op).unwrap(), given, "to the owner: no change");
    }

    #[test]
    fn the_trash_leaves_the_owner_only_untrash_and_delete() {
        let mut acl = shared();
        acl.trashed_ms = Some(5);
        assert_eq!(acl.check(&user("ana"), Action::Trash), Ok(Role::Owner));
        assert_eq!(acl.check(&user("ana"), Action::Open), Err(Refused::Trashed));
        assert_eq!(acl.check(&user("ben"), Action::Open), Err(Refused::Trashed));
        assert_eq!(acl.check(&user("ben"), Action::Trash), Err(Refused::Forbidden));
        assert_eq!(Change::Untrash.apply(&acl, &user("ana")).unwrap().trashed_ms, None);
        assert_eq!(Change::Untrash.apply(&acl, &user("ben")), Err(Refused::Forbidden));
        assert_eq!(Change::Trash(9).apply(&acl, &user("ana")).unwrap().trashed_ms, Some(5), "trashed once");
    }

    #[test]
    fn sharing_changes_only_by_the_owner_and_a_reset_shuts_old_links_out() {
        let acl = shared();
        assert_eq!(Change::Link(Level::Edit).apply(&acl, &user("ben")), Err(Refused::Forbidden));
        let reset = Change::ResetLink.apply(&acl, &user("ana")).unwrap();
        assert_eq!(reset.link_version, 4);
        assert_eq!(Change::JoinLink(3).apply(&reset, &user("dee")), Err(Refused::Forbidden), "the old link");
        let joined = Change::JoinLink(4).apply(&reset, &user("dee")).unwrap();
        assert_eq!(joined.members["dee"], Member { level: Level::View, via: Via::Link });
        let mut edit = joined.clone();
        edit.link = Level::Edit;
        let up = Change::JoinLink(4).apply(&edit, &user("ben")).unwrap();
        assert_eq!(up.members["ben"].via, Via::Invited, "a member at the link's level or above keeps how they came");
        assert_eq!(Change::JoinLink(4).apply(&edit, &user("ana")).unwrap(), edit, "the owner never becomes a member");
        let pruned = Change::RemoveLinkMembers.apply(&joined, &user("ana")).unwrap();
        assert_eq!(pruned.members.keys().collect::<Vec<_>>(), ["ben"]);
        let guest = Principal::Guest { name: "eve".into(), link_version: 4 };
        assert_eq!(Change::JoinLink(4).apply(&joined, &guest), Err(Refused::Forbidden), "a guest joins as nobody");
        let invited = Change::Member("fay".into(), Level::Edit).apply(&acl, &user("ana")).unwrap();
        assert_eq!(invited.members["fay"], Member { level: Level::Edit, via: Via::Invited });
        let removed = Change::Member("fay".into(), Level::None).apply(&invited, &user("ana")).unwrap();
        assert!(!removed.members.contains_key("fay"));
    }

    #[test]
    fn an_acl_and_its_changes_round_trip() {
        let mut acl = shared();
        acl.trashed_ms = Some(0);
        assert_eq!(Acl::decode(&acl.encode()).unwrap(), acl);
        acl.published = true;
        assert_eq!(Acl::decode(&acl.encode()).unwrap(), acl);
        assert!(Acl::decode(&acl.encode()[..5]).is_err());
        for change in [
            Change::Link(Level::Edit),
            Change::ResetLink,
            Change::Member("x".into(), Level::View),
            Change::RemoveMember("x".into()),
            Change::RemoveLinkMembers,
            Change::JoinLink(7),
            Change::Trash(9),
            Change::Untrash,
            Change::Publish(true),
            Change::Owner("y".into()),
        ] {
            let mut w = Writer::new();
            change.write(&mut w);
            let bytes = w.into_vec();
            assert_eq!(Change::read(&mut Reader::new(&bytes)).unwrap(), change);
        }
        for who in [
            user("a"),
            Principal::Guest { name: "g".into(), link_version: 2 },
            Principal::Reader,
            Principal::Operator("o".into()),
        ] {
            let mut w = Writer::new();
            who.write(&mut w);
            let bytes = w.into_vec();
            assert_eq!(Principal::read(&mut Reader::new(&bytes)).unwrap(), who);
        }
    }
}
