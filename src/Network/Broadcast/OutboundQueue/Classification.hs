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
    -- Core nodes:
    --
    -- * can become slot leader
    -- * never create currency transactions
    NodeCore

    -- | Edge node
    --
    -- Edge nodes:
    --
    -- * cannot become slot leader
    -- * creates currency transactions,
    -- * cannot communicate with core nodes
    -- * may or may not be behind NAT/firewalls
  | NodeEdge

    -- | Relay node
    --
    -- Relay nodes:
    --
    -- * cannot become slot leader
    -- * never create currency transactions
    -- * can communicate with core nodes
  | NodeRelay
  deriving (Show, Eq, Ord)

-- | Classify a message (to determine precedence)
class ClassifyMsg msg where
  classifyMsg :: msg -> MsgType

-- | Classify a destination (to determine precedence)
class ClassifyNode nid where
  classifyNode :: nid -> NodeType
