-- | Demo for the outbound queue
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
relayDemo = do
    let block :: String -> [Node] -> IO () -> IO ()
        block label nodes act = do
          logMsg label
          act
          mapM_ (OutQ.flush . nodeOutQ) nodes
          threadDelay 500000

    -- Set up some test nodes

    nodeC1 <- newNode $ NodeCfg "C"  NodeCore  (CommsDelay 0)
    nodeE1 <- newNode $ NodeCfg "E1" NodeEdge  (CommsDelay 0)
    nodeE2 <- newNode $ NodeCfg "E2" NodeEdge  (CommsDelay 0)
    nodeR  <- newNode $ NodeCfg "R1" NodeRelay (CommsDelay 0)

    setPeers nodeC1 [nodeR]
    setPeers nodeE1 [nodeR]
    setPeers nodeE2 [nodeR]
    setPeers nodeR  [nodeC1, nodeE1, nodeE2]

    block "* Basic relay test" [nodeE1, nodeC1, nodeR] $ do
      sendFrom nodeE1 (Msg MsgTransaction 0)
      sendFrom nodeC1 (Msg MsgBlockHeader 0)

    block "* Rate limiting" [nodeE1, nodeR] $ do
      -- Edge nodes are rate limited to 1 msg/sec to relay nodes
      forM_ [Msg MsgTransaction n | n <- [1..10]] $ \msg ->
        sendFrom nodeE1 msg

    block "* Priorities" [nodeR] $ do
      -- These transactions will _only_ be sent to the core nodes
      forM_ [Msg MsgTransaction n | n <- [11..20]] $ \msg ->
        sendFrom nodeR msg
      -- These blocks will be sent to both core nodes and relay nodes, but
      -- the transactions sent to the core nodes will take precedence over
      -- the block headers sent to the core nodes.
      forM_ [Msg MsgBlockHeader n | n <- [11..20]] $ \msg ->
        sendFrom nodeR msg

    -- Two core nodes that communicate directly with each other
    -- (disjoint from the nodes we set up above)

    nodeC2 <- newNode $ NodeCfg "C2" NodeCore (CommsDelay 0)
    nodeC3 <- newNode $ NodeCfg "C3" NodeCore (CommsDelay 500000)

    setPeers nodeC2 [nodeC3]

    block "* Latency masking" [nodeC2] $ do
      -- Core to core communication is allowed higher concurrency
      -- Although that's not explicitly rate limited, we've set up the nodes to
      -- model slow communication
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
    , nodeCommsDelay :: CommsDelay
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
      msg   <- recv nodeChan nodeCommsDelay
      added <- addToMsgPool nodeMsgPool msg
      when added $ do
        logMsg $ nodeId ++ ": received " ++ show msg
        OutQ.enqueue nodeOutQ msg OutQ.OriginForward mempty
    return Node{..}

setPeers :: Node -> [Node] -> IO ()
setPeers Node{..} = OutQ.subscribe nodeOutQ . OutQ.simplePeers

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
newtype CommsDelay = CommsDelay Int

newSyncVar :: IO (SyncVar a)
newSyncVar = SyncVar <$> newEmptyMVar

send :: SyncVar a -> a -> IO ()
send (SyncVar v) a = do
    ack <- newEmptyMVar
    putMVar v (a, ack)
    takeMVar ack

recv :: SyncVar a -> CommsDelay -> IO a
recv (SyncVar v) (CommsDelay delay) = do
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
