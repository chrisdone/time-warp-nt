-- | Classification of nodes and messages
module Network.Broadcast.OutboundQueue.Classification (
    MsgType(..)
  , NodeType(..)
  , ClassifyMsg(..)
  , ClassifyNode(..)
  ) where

{-------------------------------------------------------------------------------
  Classification of messages and destinations
-------------------------------------------------------------------------------}

-- | Message types
data MsgType =
    -- | Announcement of a new block
    --
    -- This is a block header, not the actual value of the block.
    MsgBlockHeader

    -- | New transaction
  | MsgTransaction

    -- | MPC messages
  | MsgMPC
  deriving (Show, Eq, Ord)

-- | Node types
data NodeType =
    -- | Core node
    --
    -- A core node is one that is able to generate blocks
    NodeCore

    -- | Edge node
    --
    -- An edge node is one that generates new transactions
  | NodeEdge

    -- | Relay node
    --
    -- A relay node neither generates blocks nor generates transactions.
    -- They sit in between core nodes and edge nodes.
  | NodeRelay
  deriving (Show, Eq, Ord)

-- | Classify a message (to determine precedence)
class ClassifyMsg msg where
  classifyMsg :: msg -> MsgType

-- | Classify a destination (to determine precedence)
class ClassifyNode nid where
  classifyNode :: nid -> NodeType
