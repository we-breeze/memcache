//! Wire-protocol codecs.
//!
//! Both submodules operate on a [`crate::connection::Connection`] through its
//! buffered IO primitives. [`text`] implements the ASCII protocol and [`binary`]
//! the binary protocol.

pub(crate) mod binary;
pub(crate) mod text;

/// A storage command shared by both protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreCommand {
    Set,
    Add,
    Replace,
    Append,
    Prepend,
}

impl StoreCommand {
    /// The ASCII verb for this command.
    pub(crate) fn text_verb(self) -> &'static str {
        match self {
            StoreCommand::Set => "set",
            StoreCommand::Add => "add",
            StoreCommand::Replace => "replace",
            StoreCommand::Append => "append",
            StoreCommand::Prepend => "prepend",
        }
    }

    /// The binary opcode for this command.
    pub(crate) fn binary_opcode(self) -> u8 {
        match self {
            StoreCommand::Set => binary::opcode::SET,
            StoreCommand::Add => binary::opcode::ADD,
            StoreCommand::Replace => binary::opcode::REPLACE,
            StoreCommand::Append => binary::opcode::APPEND,
            StoreCommand::Prepend => binary::opcode::PREPEND,
        }
    }

    /// Append/prepend do not carry flags/expiry extras.
    pub(crate) fn has_storage_extras(self) -> bool {
        !matches!(self, StoreCommand::Append | StoreCommand::Prepend)
    }
}
