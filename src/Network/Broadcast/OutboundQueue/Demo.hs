{-# LANGUAGE RecursiveDo #-}

-- | Demo for the outbound queue
--
-- NOTE: Currently broken!
module Network.Broadcast.OutboundQueue.Demo (relayDemo) where

import Control.Concurrent
import Control.Monad
import Data.Function
import Data.Set (Set)
import System.IO.Unsafe
import qualified Data.Set as Set

import Network.Broadcast.OutboundQueue (OutboundQ)
import Network.Broadcast.OutboundQueue.Classification
import qualified Network.Broadcast.OutboundQueue as OutQ

{-------------------------------------------------------------------------------
  Relay demo
-------------------------------------------------------------------------------}

relayDemo :: IO ()
relayDemo = mdo
    let block :: String -> IO () -> IO ()
        block label act = do
          logMsg label
          act
--          flush relayer
          threadDelay 500000

    -- Set up some test nodes

    nodeC1 <- newNode NodeCfg {
                  nodeId          = "C"
                , nodeType        = NodeCore
                , nodeRecvDelay   = 0
--                , nodeMaxInFlight = 1
--                , nodeRelayDelay  = 500000
--                , nodePeers       = simplePeers [nodeR]
--                , nodeRelayer     = relayer
                }
    nodeE1 <- newNode NodeCfg {
                  nodeId          = "E1"
                , nodeType        = NodeEdge
                , nodeRecvDelay   = 0
--                , nodeMaxInFlight = 1
--                , nodeRelayDelay  = 500000
--                , nodePeers       = simplePeers [nodeR]
--                , nodeRelayer     = relayer
                }
    nodeE2 <- newNode NodeCfg {
                  nodeId          = "E2"
                , nodeType        = NodeEdge
                , nodeRecvDelay   = 0
--                , nodeMaxInFlight = 1
--                , nodeRelayDelay  = 500000
--                , nodePeers       = simplePeers [nodeR]
--                , nodeRelayer     = relayer
                }
    nodeR  <- newNode NodeCfg {
                  nodeId          = "R1"
                , nodeType        = NodeRelay
                , nodeRecvDelay   = 0
--                , nodeMaxInFlight = 1
--                , nodeRelayDelay  = 0
--                , nodePeers       = simplePeers [nodeC1, nodeE1, nodeE2]
--                , nodeRelayer     = relayer
                }

    block "* Basic relay test" $ do
      sendFrom nodeE1 (Msg MsgTransaction 0)
      sendFrom nodeC1 (Msg MsgBlockHeader 0)

    block "* Rate limiting" $ do
      forM_ [Msg MsgTransaction n | n <- [1..10]] $ \msg ->
        sendFrom nodeE1 msg

    block "* Priorities" $ do
      forM_ [Msg MsgTransaction n | n <- [11..20]] $ \msg ->
        sendFrom nodeE1 msg
      forM_ [Msg MsgBlockHeader n | n <- [11..20]] $ \msg ->
        sendFrom nodeC1 msg

    -- Two core nodes that communicate directly with each other
    -- (disjoint from the nodes we set up above)

    nodeC2 <- newNode NodeCfg {
                  nodeId          = "C"
                , nodeType        = NodeCore
                , nodeRecvDelay   = 0
--                , nodeMaxInFlight = 1
--                , nodeRelayDelay  = 0
--                , nodePeers       = simplePeers [nodeC3]
--                , nodeRelayer     = relayer
                }
    nodeC3 <- newNode NodeCfg {
                  nodeId          = "C"
                , nodeType        = NodeCore
                , nodeRecvDelay   = 500000
--                , nodeMaxInFlight = 2
--                , nodeRelayDelay  = 0
--                , nodePeers       = simplePeers [nodeC2]
--                , nodeRelayer     = relayer
                }

    block "* Latency masking" $ do
      forM_ [Msg MsgBlockHeader n | n <- [21..30]] $ \msg ->
        sendFrom nodeC2 msg

    logMsg "End of demo"

{-------------------------------------------------------------------------------
  Outbound queue used for the demo
-------------------------------------------------------------------------------}

type DemoQ = OutboundQ Msg Node

data Msg = Msg { msgType :: MsgType, _msgContents :: Int }
  deriving (Show, Ord, Eq)

instance ClassifyMsg  Msg  where classifyMsg  = msgType
instance ClassifyNode Node where classifyNode = nodeType . nodeCfg

newDemoQ :: NodeType -> IO DemoQ
newDemoQ selfType = do
    demoQ <- OutQ.new (OutQ.defaultEnqueuePolicy selfType)
                      (OutQ.defaultDequeuePolicy selfType)
    _tid  <- forkIO $ OutQ.dequeueThread demoQ sendMsg
    return demoQ
  where
    sendMsg = \msg Node{..} -> send nodeChan msg
{-
    policy  = RelayerPolicy {
        rPolicyDelay       = return . Just . nodeRelayDelay . nodeCfg
      , rPolicyMaxInFlight = nodeMaxInFlight . nodeCfg
      }
-}

{-------------------------------------------------------------------------------
  Model of a node

  We model a node as a thread that relays any message it had not previously
  received.
-------------------------------------------------------------------------------}

data NodeCfg = NodeCfg {
      -- | Node ID (needed because the relayer wants an Ord instance)
      nodeId :: String

      -- | Node type
    , nodeType :: NodeType

      -- | Delay on synchronous communication
      --
      -- Used to model slow nodes
    , nodeRecvDelay :: Delay
    }

data Node = Node {
      nodeCfg     :: NodeCfg
    , nodeChan    :: SyncVar Msg
    , nodeMsgPool :: MVar (Set Msg)
    , nodeOutQ    :: DemoQ
    }

instance Eq   Node where (==) = (==) `on` (nodeId . nodeCfg)
instance Ord  Node where (<=) = (<=) `on` (nodeId . nodeCfg)
instance Show Node where show = show .    (nodeId . nodeCfg)

newNode :: NodeCfg -> IO Node
newNode nodeCfg@NodeCfg{..} = do
    nodeOutQ    <- newDemoQ nodeType
    nodeChan    <- newSyncVar
    nodeMsgPool <- newMsgPool
    _tid <- forkIO $ forever $ do
      msg   <- recv nodeChan nodeRecvDelay
      added <- addToMsgPool nodeMsgPool msg
      when added $ do
        logMsg $ nodeId ++ ": received " ++ show msg
        OutQ.enqueue nodeOutQ msg OutQ.OriginForward mempty
    return Node{..}

sendFrom :: Node -> Msg -> IO ()
sendFrom Node{nodeCfg = NodeCfg{..}, ..} msg = do
    modifyMVar_ nodeMsgPool $ \msgPool -> return $! Set.insert msg msgPool
    OutQ.enqueue nodeOutQ msg OutQ.OriginSender mempty

{-------------------------------------------------------------------------------
  Message pool
-------------------------------------------------------------------------------}

type MsgPool = MVar (Set Msg)

newMsgPool :: IO MsgPool
newMsgPool = newMVar Set.empty

-- | Add a message to the pool
--
-- Returns whether the message was new.
addToMsgPool :: MsgPool -> Msg -> IO Bool
addToMsgPool pool msg = modifyMVar pool $ \msgs -> return $!
    if Set.member msg msgs
      then (msgs, False)
      else (Set.insert msg msgs, True)

{-------------------------------------------------------------------------------
  Model synchronous communication
-------------------------------------------------------------------------------}

data SyncVar a = SyncVar (MVar (a, MVar ()))

-- | Delay models slow communication networks
type Delay = Int

newSyncVar :: IO (SyncVar a)
newSyncVar = SyncVar <$> newEmptyMVar

send :: SyncVar a -> a -> IO ()
send (SyncVar v) a = do
    ack <- newEmptyMVar
    putMVar v (a, ack)
    takeMVar ack

recv :: SyncVar a -> Delay -> IO a
recv (SyncVar v) delay = do
    (a, ack) <- takeMVar v
    -- We run the acknowledgement in a separate thread, to model a node
    -- spawning a listener for each incoming request
    _tid <- forkIO $ do
      threadDelay delay
      putMVar ack ()
    return a

{-------------------------------------------------------------------------------
  Auxiliary: thread-safe logging
-------------------------------------------------------------------------------}

logLock :: MVar ()
{-# NOINLINE logLock #-}
logLock = unsafePerformIO $ newMVar ()

logMsg :: String -> IO ()
logMsg msg = withMVar logLock $ \() -> putStrLn msg
