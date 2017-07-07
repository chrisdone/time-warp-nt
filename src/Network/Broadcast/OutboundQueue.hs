{-------------------------------------------------------------------------------
  Outbound message queue

  Intended for qualified import

  > import Network.Broadcast.OutboundQ (OutboundQ)
  > import qualified Network.Broadcast.OutboundQ as OutQ
  > import Network.Broadcast.OutboundQueue.Classification

  References:
  * https://issues.serokell.io/issue/CSL-1272
  * IERs_V2.md
-------------------------------------------------------------------------------}

{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}

module Network.Broadcast.OutboundQueue (
    OutboundQ -- opaque
    -- * Initialization
  , new
    -- ** Enqueueing policy
  , Precedence(..)
  , MaxAhead(..)
  , MaxQueueSize(..)
  , Fallback(..)
  , Enqueue(..)
  , EnqueuePolicy
  , defaultEnqueuePolicy
    -- ** Dequeueing policy
  , RateLimit(..)
  , MaxInFlight(..)
  , Dequeue(..)
  , DequeuePolicy
  , defaultDequeuePolicy
    -- * Enqueueing
  , Origin(..)
  , enqueue
    -- * Dequeuing
  , SendMsg
  , dequeueThread
    -- ** Controlling the dequeuer
  , flush
  , waitShutdown
    -- * Peers
  , Peers(..)
  , AllOf
  , Alts
  , simplePeers
  , subscribe
  , unsubscribe
  ) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Lens
import Control.Monad
import Data.Map.Strict (Map)
import Data.Monoid ((<>))
import Data.Set (Set)
import System.Random (randomRIO)
import qualified Data.Map.Strict as Map
import qualified Data.Set        as Set

import Network.Broadcast.OutboundQueue.Classification
import Network.Broadcast.OutboundQueue.ConcurrentMultiQueue (MultiQueue)
import qualified Network.Broadcast.OutboundQueue.ConcurrentMultiQueue as MQ

{-------------------------------------------------------------------------------
  Precedence levels
-------------------------------------------------------------------------------}

-- | Precedence levels
--
-- These precedence levels are not given meaningful names because the same kind
-- of message might be given different precedence levels on different kinds of
-- nodes. Meaning is given to these levels in the enqueueing policy.
data Precedence = P1 | P2 | P3 | P4 | P5
  deriving (Show, Eq, Ord, Enum, Bounded)

enumPrecLowestFirst :: [Precedence]
enumPrecLowestFirst = [minBound .. maxBound]

enumPrecHighestFirst :: [Precedence]
enumPrecHighestFirst = reverse enumPrecLowestFirst

{-------------------------------------------------------------------------------
  Known peers
-------------------------------------------------------------------------------}

-- | All known peers, split per node type, in order of preference
data Peers nid = Peers {
      _peersCore  :: AllOf (Alts nid)
    , _peersRelay :: AllOf (Alts nid)
    , _peersEdge  :: AllOf (Alts nid)
    }

-- | Each of these need to be contacted (in arbitrary order)
type AllOf a = [a]

-- | Non-empty list of alternatives (in order of preference)
type Alts a = [a]

makeLenses ''Peers

peersOfType :: NodeType -> Lens' (Peers nid) (AllOf (Alts nid))
peersOfType NodeCore  = peersCore
peersOfType NodeRelay = peersRelay
peersOfType NodeEdge  = peersEdge

-- | Construct 'Peers' from a list of node IDs
--
-- This effective means that all of these peers will be sent all (relevant)
-- messages.
simplePeers :: forall nid. ClassifyNode nid => [nid] -> Peers nid
simplePeers = go mempty
  where
    go :: Peers nid -> [nid] -> Peers nid
    go acc []     = acc
    go acc (n:ns) = go (acc & peersOfType (classifyNode n) %~ ([n] :)) ns

instance Monoid (Peers nid) where
  mempty      = Peers [] [] []
  mappend a b = Peers {
                    _peersCore  = comb _peersCore
                  , _peersRelay = comb _peersRelay
                  , _peersEdge  = comb _peersEdge
                  }
    where
      comb :: Monoid a => (Peers nid -> a) -> a
      comb f = f a `mappend` f b

removePeers :: forall nid. Ord nid => Set nid -> Peers nid -> Peers nid
removePeers nids peers =
    peers & peersCore  %~ remove
          & peersRelay %~ remove
          & peersEdge  %~ remove
  where
    remove :: AllOf (Alts nid) -> AllOf (Alts nid)
    remove = map $ filter (`Set.notMember` nids)

{-------------------------------------------------------------------------------
  Enqueing policy

  The enquing policy is intended to guarantee that at the point of enqueing
  we can be reasonably sure that the message will get to where it needs to be
  within the maximum time bounds.
-------------------------------------------------------------------------------}

-- | Maximum number of messages allowed "ahead" of the message to be enqueued
--
-- This is the total number of messages currently in-flight or in-queue with a
-- precedence at or above the message to be enqueued (i.e., all messages that
-- will be handled before the new message).
newtype MaxAhead = MaxAhead Int

-- | Maximum queue size
--
-- If we cannot find an alternative satisfying 'MaxAhead' and fall back to a
-- random alternative, 'MaxQueueSize' will determine whether we first need to
-- drop an old message before enqueing the new message (to keep the queue size
-- bounded).
data MaxQueueSize = MaxQueueSize Int

-- | What to do if we couldn't pick a node?
data Fallback =
    -- | Pick a random alternative
    FallbackRandom MaxQueueSize

    -- | Don't send the message at all
  | FallbackNone

data Enqueue = Enqueue {
      enqNodeType   :: NodeType
    , enqMaxAhead   :: MaxAhead
    , enqFallback   :: Fallback
    , enqPrecedence :: Precedence
    }

-- | The enqueuing policy
--
-- The enqueueing policy decides what kind of peer to send each message to,
-- how to pick alternatives, and which precedence level to assign to the
-- message. However, it does NOT decide _how many_ alternatives to pick; we
-- pick one from _each_ of the lists that we are given. It is the responsiblity
-- of the next layer up to configure these peers as desired.
--
-- TODO: Verify the max queue sizes against the updated policy.
type EnqueuePolicy =
           MsgType  -- ^ Type of the message we want to send
        -> Origin   -- ^ Where did this message originate?
        -> [Enqueue]

defaultEnqueuePolicy :: NodeType       -- ^ Type of this node
                     -> EnqueuePolicy
defaultEnqueuePolicy NodeCore = go
  where
    -- Enqueue policy for core nodes
    go :: EnqueuePolicy
    go MsgBlockHeader _ = [
        Enqueue NodeCore  (MaxAhead 1) (FallbackRandom (MaxQueueSize 1)) P1
      , Enqueue NodeRelay (MaxAhead 1) (FallbackRandom (MaxQueueSize 1)) P3
      ]
    go MsgMPC _ = [
        Enqueue NodeCore (MaxAhead 2) (FallbackRandom (MaxQueueSize 2)) P2
        -- not sent to relay nodes
      ]
    go MsgTransaction _ = [
        Enqueue NodeCore (MaxAhead 20) (FallbackRandom (MaxQueueSize 20)) P4
        -- not sent to relay nodes
      ]
defaultEnqueuePolicy NodeRelay = go
  where
    -- Enqueue policy for relay nodes
    go :: EnqueuePolicy
    go MsgBlockHeader _ = [
        Enqueue NodeRelay (MaxAhead 1) (FallbackRandom (MaxQueueSize 1)) P1
      , Enqueue NodeCore  (MaxAhead 1) (FallbackRandom (MaxQueueSize 1)) P2
      , Enqueue NodeEdge  (MaxAhead 1) (FallbackRandom (MaxQueueSize 1)) P3
      ]
    go MsgTransaction _ = [
        Enqueue NodeCore  (MaxAhead 20) (FallbackRandom (MaxQueueSize 20)) P4
      , Enqueue NodeRelay (MaxAhead 20) (FallbackRandom (MaxQueueSize 20)) P5
        -- transactions not forwarded to edge nodes
      ]
    go MsgMPC _ = [
        -- Relay nodes never sent any MPC messages to anyone
      ]
defaultEnqueuePolicy NodeEdge = go
  where
    -- Enqueue policy for edge nodes
    go :: EnqueuePolicy
    go MsgTransaction OriginSender = [
        Enqueue NodeRelay (MaxAhead 1) (FallbackRandom (MaxQueueSize 100)) P1
      ]
    go MsgTransaction OriginForward = [
        -- don't forward transactions that weren't created at this node
      ]
    go MsgBlockHeader _ = [
        -- not forwarded
      ]
    go MsgMPC _ = [
        -- not relevant
      ]

{-------------------------------------------------------------------------------
  Dequeue policy
-------------------------------------------------------------------------------}

data Dequeue = Dequeue {
      -- | Delay before sending the next message (to this node)
      deqRateLimit :: RateLimit

      -- | Maximum number of in-flight messages (to this node node)
    , deqMaxInFlight :: MaxInFlight
    }

-- | Rate limiting
data RateLimit = NoRateLimiting | MaxMsgPerSec Int

-- | Maximum number of in-flight messages (for latency hiding)
newtype MaxInFlight = MaxInFlight Int

-- | Dequeue policy
--
-- The dequeue policy epends only on the type of the node we're sending to,
-- not the same of the message we're sending.
type DequeuePolicy = NodeType -> Dequeue

defaultDequeuePolicy :: NodeType -- ^ Our node type
                     -> DequeuePolicy
defaultDequeuePolicy NodeCore = go
  where
    -- Dequeueing policy for core nodes
    go :: DequeuePolicy
    go NodeCore  = Dequeue NoRateLimiting (MaxInFlight 2)
    go NodeRelay = Dequeue NoRateLimiting (MaxInFlight 1)
    go NodeEdge  = error "defaultDequeuePolicy: core to edge not applicable"
defaultDequeuePolicy NodeRelay = go
  where
    -- Dequeueing policy for relay nodes
    go :: DequeuePolicy
    go NodeCore  = Dequeue (MaxMsgPerSec 1) (MaxInFlight 1)
    go NodeRelay = Dequeue (MaxMsgPerSec 3) (MaxInFlight 2)
    go NodeEdge  = Dequeue (MaxMsgPerSec 1) (MaxInFlight 1)
defaultDequeuePolicy NodeEdge = go
  where
    -- Dequeueing policy for edge nodes
    go :: DequeuePolicy
    go NodeCore  = error "defaultDequeuePolicy: edge to core not applicable"
    go NodeRelay = Dequeue (MaxMsgPerSec 1) (MaxInFlight 1)
    go NodeEdge  = error "defaultDequeuePolicy: edge to edge not applicable"

{-------------------------------------------------------------------------------
  Thin wrapper around ConcurrentMultiQueue
-------------------------------------------------------------------------------}

-- | The values we store in the multiqueue
--
-- Pair a destination with the message (payload)
data Packet msg nid = Packet {payload :: msg, dest :: nid}
  deriving (Show)

-- | The keys we use to index the multiqueue
data Key nid =
    -- | All messages with a certain precedence
    --
    -- Used when dequeuing to determine the next message to send
    KeyByPrec Precedence

    -- | All messages to a certain destination
    --
    -- Used when dequeing to determine max in-flight to a particular destination
    -- (for latency hiding)
  | KeyByDest nid

    -- | All messages with a certain precedence to a particular destination
    --
    -- Used when enqueuing to determine routing (enqueuing policy)
  | KeyByDestPrec nid Precedence
  deriving (Show, Eq, Ord)

-- | MultiQueue instantiated at the types we need
type MQ msg nid = MultiQueue (Key nid) (Packet msg nid)

mqEnqueue :: Ord nid => MQ msg nid -> Packet msg nid -> Precedence -> IO ()
mqEnqueue qs pkt@Packet{dest} prec =
  MQ.enqueue qs [ KeyByPrec prec
                , KeyByDest dest
                , KeyByDestPrec dest prec
                ]
                pkt

-- | Check whether a node is not currently busy
--
-- (i.e., number of in-flight messages is less than the max)
type NotBusy nid = nid -> Bool

mqDequeue :: Ord nid => MQ msg nid -> NotBusy nid -> IO (Maybe (Packet msg nid))
mqDequeue qs notBusy =
    foldr1 orElseM [
        MQ.dequeue (KeyByPrec prec) (notBusy . dest) qs
      | prec <- enumPrecHighestFirst
      ]

{-------------------------------------------------------------------------------
  In-flight messages (i.e., messages already sent but not yet acknowledged)
-------------------------------------------------------------------------------}

-- | Handler for a single message
type MsgHandler = Async ()

-- | How many messages are currently in-flight to a given destination?
--
-- For each destination we record the set of thread IDs of the threads
-- responsible for the in-flight messages (we spawn a new thread per message).
type InFlight nid = Map nid (Map ThreadId MsgHandler)

addInFlight :: Ord nid => MVar (InFlight nid) -> nid -> MsgHandler -> IO ()
addInFlight vInFlight nid handler = modifyMVar_ vInFlight $ \inFlight ->
    return $! Map.alter aux nid inFlight
  where
    aux :: Maybe (Map ThreadId MsgHandler) -> Maybe (Map ThreadId MsgHandler)
    aux Nothing   = Just $ Map.singleton (asyncThreadId handler) handler
    aux (Just ts) = Just $ Map.insert (asyncThreadId handler) handler ts

delInFlight :: Ord nid => MVar (InFlight nid) -> nid -> ThreadId -> IO ()
delInFlight vInFlight nid tid = modifyMVar_ vInFlight $ \inFlight ->
    return $! Map.alter aux nid inFlight
  where
    aux :: Maybe (Map ThreadId MsgHandler) -> Maybe (Map ThreadId MsgHandler)
    aux Nothing   = error "delInFlight: already deleted"
    aux (Just ts) = let ts' = Map.delete tid ts
                    in if Map.null ts'
                      then Nothing
                      else Just ts'

{-------------------------------------------------------------------------------
  Initialization
-------------------------------------------------------------------------------}

data OutboundQ msg nid = (ClassifyMsg msg, ClassifyNode nid, Ord nid) => OutQ {
      -- | Enqueuing policy
      qEnqueuePolicy :: EnqueuePolicy

      -- | Dequeueing policy
    , qDequeuePolicy :: DequeuePolicy

      -- | Messages sent but not yet acknowledged
    , qInFlight :: MVar (InFlight nid)

      -- | Messages scheduled but not yet sent
    , qScheduled :: MQ msg nid

      -- | Known peers
    , qPeers :: MVar (Peers nid)

      -- | Used to send control messages to the main thread
    , qCtrlMsg :: MVar CtrlMsg

      -- | Signal we use to wake up blocked threads
    , qSignal :: Signal CtrlMsg
    }

-- | Initialize the outbound queue
--
-- NOTE: The dequeuing thread must be started separately. See 'dequeueThread'.
new :: (ClassifyMsg msg, ClassifyNode nid, Ord nid)
    => EnqueuePolicy -> DequeuePolicy -> IO (OutboundQ msg nid)
new qEnqueuePolicy qDequeuePolicy = do
    qInFlight  <- newMVar Map.empty
    qScheduled <- MQ.new
    qPeers     <- newMVar mempty
    qCtrlMsg   <- newEmptyMVar

    -- Only look for control messages when the queue is empty
    let checkCtrlMsg :: IO (Maybe CtrlMsg)
        checkCtrlMsg = do
          qSize <- MQ.size qScheduled
          if qSize == 0
            then tryTakeMVar qCtrlMsg
            else return Nothing

    qSignal <- newSignal checkCtrlMsg
    return OutQ{..}

{-------------------------------------------------------------------------------
  Interpreter for the enqueing policy
-------------------------------------------------------------------------------}

-- TODO: Don't send to same node twice
rsEnqueue :: OutboundQ msg nid -> msg -> Origin -> Peers nid -> IO ()
rsEnqueue outQ@OutQ{..} msg origin peers =
    forM_ (qEnqueuePolicy (classifyMsg msg) origin) $ \enq@Enqueue{..} ->
      forM_ (peers ^. peersOfType enqNodeType) $ \alts -> do
        mAlt <- pickAlt outQ enq alts `orElseM` pickFallback outQ enq alts
        case mAlt of
          Nothing  -> return () -- Drop message
          Just alt -> do mqEnqueue qScheduled (Packet msg alt) enqPrecedence
                         poke qSignal

pickAlt :: OutboundQ msg nid -> Enqueue -> Alts nid -> IO (Maybe nid)
pickAlt outQ Enqueue{enqMaxAhead = MaxAhead maxAhead, ..} alts =
    foldr1 orElseM [ do
        ahead <- currentlyAhead outQ alt enqPrecedence
        return $ if ahead < maxAhead
                   then Just alt
                   else Nothing
      | alt <- alts
      ]

pickFallback :: OutboundQ msg nid -> Enqueue -> Alts nid -> IO (Maybe nid)
pickFallback OutQ{..} Enqueue{..} alts =
    case enqFallback of
      FallbackNone ->
        return Nothing
      FallbackRandom (MaxQueueSize maxQueueSize) -> do
        alt <- (alts !!) <$> randomRIO (0, length alts - 1)
        queueSize <- MQ.sizeBy (KeyByDestPrec alt enqPrecedence) qScheduled
        when (queueSize >= maxQueueSize) $
          MQ.removeFront (KeyByDestPrec alt enqPrecedence) qScheduled
        return (Just alt)

-- | Check how many messages are currently ahead
--
-- NOTE: This is of course a highly dynamic value; by the time we get to
-- actually enqueue the message the value might be slightly different. Bounds
-- are thus somewhat fuzzy.
currentlyAhead :: OutboundQ msg nid -> nid -> Precedence -> IO Int
currentlyAhead OutQ{qScheduled} nid prec =
    sum <$> forM [prec .. maxBound]
                 (\prec' -> MQ.sizeBy (KeyByDestPrec nid prec') qScheduled)

{-------------------------------------------------------------------------------
  Interpreter for the dequeueing policy
-------------------------------------------------------------------------------}

checkMaxInFlight :: (Ord nid, ClassifyNode nid)
                 => DequeuePolicy -> InFlight nid -> NotBusy nid
checkMaxInFlight dequeuePolicy inFlight nid =
    maybe 0 Map.size (Map.lookup nid inFlight) < n
  where
    MaxInFlight n = deqMaxInFlight (dequeuePolicy (classifyNode nid))

delay :: ClassifyNode nid => DequeuePolicy -> nid -> IO ()
delay dequeuePolicy nid =
    case deqRateLimit (dequeuePolicy (classifyNode nid)) of
      NoRateLimiting -> return ()
      MaxMsgPerSec n -> threadDelay (1000000 `div` n)

rsDequeue :: forall msg nid.
             OutboundQ msg nid -> SendMsg msg nid -> IO (Maybe CtrlMsg)
rsDequeue OutQ{..} sendMsg = do
    mPacket <- getPacket
    case mPacket of
      Left ctrlMsg -> return $ Just ctrlMsg
      Right packet -> sendPacket packet >> return Nothing
  where
    getPacket :: IO (Either CtrlMsg (Packet msg nid))
    getPacket = retryIfNothing qSignal $ do
      inFlight <- readMVar qInFlight
      mqDequeue qScheduled (checkMaxInFlight qDequeuePolicy inFlight)

    -- Send the packet we just dequeued
    --
    -- At this point we have dequeued the message but not yet recorded it as
    -- in-flight. That's okay though: the only function whose behaviour is
    -- affected by 'rsInFlight' is 'rsDequeue', the main thread (this thread) is
    -- the only thread calling 'rsDequeue', and we will update 'rsInFlight'
    -- before dequeueing the next message.
    --
    -- We start a new thread to handle the conversation. This is a bit of a
    -- subtle design decision. We could instead start the conversation here in
    -- the main thread, and fork a thread only to wait for the acknowledgement.
    -- The problem with doing that is that if that conversation gets blocked or
    -- delayed for any reason, it will block or delay the whole outbound queue.
    -- The downside of the /current/ solution is that it makes priorities
    -- somewhat less meaningful: although the priorities dictate in which order
    -- we fork threads to handle conversations, after that those threads all
    -- compete with each other (amongst other things, for use of the network
    -- device), with no real way to prioritize any one thread over the other. We
    -- will be able to solve this conumdrum properly once we move away from TCP
    -- and use the RINA network architecture instead.
    sendPacket :: Packet msg nid -> IO ()
    sendPacket packet@Packet{dest} = mask_ $ do
      barrier <- newEmptyMVar
      handler <- asyncWithUnmask $ sendThread barrier packet
      addInFlight qInFlight dest handler
      putMVar barrier () -- Only start aux thread after registration

    sendThread :: MVar () -> Packet msg nid -> (IO () -> IO ()) -> IO ()
    sendThread barrier Packet{..} unmask = do
      -- wait for go-ahead
      readMVar barrier
      -- Discard any errors thrown by the callback
      _mErr <- catchAll $ unmask $ do
        sendMsg payload dest
        delay qDequeuePolicy dest
      delInFlight qInFlight dest =<< myThreadId
      poke qSignal

    catchAll :: IO () -> IO (Either SomeException ())
    catchAll = try

{-------------------------------------------------------------------------------
  Public interface to enqueing
-------------------------------------------------------------------------------}

-- | Where did the message we're sending originate?
--
-- We need this because, for example, edge nodes will want to send /their/
-- transactions to relay nodes, but not any transactions that they /received/
-- from relay nodes.
data Origin =
    -- | It originated at the node who's sending it
    --
    -- For instance, for a transaction this means it was created on this (edge)
    -- node; for a block it would mean it was constructed on this (core) node.
    OriginSender

    -- | It originated elsewhere; we're just forwarding it
  | OriginForward

-- | Queue a message to be send to all peers
--
-- The message will be sent to the specified peers as well as any subscribers.
--
-- TODO: Ultimately we want to move to a model where we /only/ have
-- subscription; after all, it's no problem for statically configured nodes to
-- also subscribe when they are created. We don't use such a model just yet to
-- make integration easier.
enqueue :: OutboundQ msg nid
        -> msg        -- ^ Message to send
        -> Origin     -- ^ Origin of this message
        -> Peers nid  -- ^ Additional peers (in addition to subscribers)
        -> IO ()
enqueue outQ@OutQ{..} msg origin peers' = do
    peers <- readMVar qPeers
    rsEnqueue outQ msg origin (peers <> peers')

{-------------------------------------------------------------------------------
  Dequeue thread
-------------------------------------------------------------------------------}

-- | Action to send a message
--
-- The action should block until the message has been acknowledged by the peer.
--
-- NOTE:
--
-- * The IO action will be run in a separate thread.
-- * No additional timeout is applied to the 'SendMsg', so if one is
--   needed it must be provided externally.
type SendMsg msg nid = msg -> nid -> IO ()

-- | The dequeue thread
--
-- It is the responsibility of the next layer up to fork this thread; this
-- function does not return unless told to terminate using 'waitShutdown'.
dequeueThread :: OutboundQ msg nid -> SendMsg msg nid -> IO ()
dequeueThread outQ@OutQ{..} sendMsg =
    loop `finally` killAuxThreads
  where
    loop :: IO ()
    loop = do
      mCtrlMsg <- rsDequeue outQ sendMsg
      case mCtrlMsg of
        Nothing      -> loop
        Just ctrlMsg -> do
          waitAuxThreads
          case ctrlMsg of
            Shutdown ack -> putMVar ack ()
            Flush    ack -> putMVar ack () >> loop

    getAuxThreads :: IO [MsgHandler]
    getAuxThreads = flatten <$> readMVar qInFlight
      where
        flatten :: InFlight nid -> [MsgHandler]
        flatten = concatMap Map.elems . Map.elems

    waitAuxThreads :: IO ()
    waitAuxThreads = mapM_ wait =<< getAuxThreads

    killAuxThreads :: IO ()
    killAuxThreads = mapM_ cancel =<< getAuxThreads

{-------------------------------------------------------------------------------
  Controlling the dequeue thread
-------------------------------------------------------------------------------}

-- | Control messages sent to the main thread
--
-- NOTE: These are given lower precedence than non-control messages.
data CtrlMsg =
    Shutdown (MVar ())
  | Flush    (MVar ())

-- | Gracefully shutdown the relayer
waitShutdown :: OutboundQ msg nid -> IO ()
waitShutdown OutQ{..} = do
    ack <- newEmptyMVar
    putMVar qCtrlMsg $ Shutdown ack
    poke qSignal
    takeMVar ack

-- | Wait for all messages currently enqueued to have been sent
flush :: OutboundQ msg nid -> IO ()
flush OutQ{..} = do
    ack <- newEmptyMVar
    putMVar qCtrlMsg $ Flush ack
    poke qSignal
    takeMVar ack

{-------------------------------------------------------------------------------
  Subscription
-------------------------------------------------------------------------------}

-- | Subscribe to the outbound queue
--
-- NOTE: Behind NAT nodes: Edge nodes behind NAT can contact a relay node to ask
-- to be notified of messages. The listener on the relay node should call
-- 'subscribe' on its outbound queue to subscribe the edge node that contacted
-- it. Then the  conversation should remain open, so that the (heavy-weight) TCP
-- connection between the edge node and the relay node is kept open. When the
-- edge node disappears the listener thread on the relay node should call
-- 'unsubscribe' to remove the edge node from its outbound queue again.
subscribe :: OutboundQ msg nid -> Peers nid -> IO ()
subscribe OutQ{..} peers' =
    modifyMVar_ qPeers $ \peers ->
      return $! peers <> peers'

-- | Unsubscribe some nodes
--
-- See 'subscribe'.
--
-- TODO: Delete now-useless outbound messages from the queues.
unsubscribe :: Ord nid => OutboundQ msg nid -> [nid] -> IO ()
unsubscribe OutQ{..} nids =
    modifyMVar_ qPeers $ \peers ->
      return $! removePeers (Set.fromList nids) peers

{-------------------------------------------------------------------------------
  Auxiliary: Signalling

  A signal is used to detect whether " something " changed between two points in
  time, and block a thread otherwise. Only a single thread should be calling
  'retryIfNothing'; other threads should call 'poke' to indicate when
  something changed and the blocked action can be retried. A signal is _not_ a
  counter: we don't keep track of how often 'poke' is called.
-------------------------------------------------------------------------------}

data Signal b = Signal {
    -- | Used to wake up the blocked thread
    signalPokeVar :: MVar ()

    -- | Used to send out-of-band control messages to the suspended thread
  , signalCtrlMsg :: IO (Maybe b)
  }

newSignal :: IO (Maybe b) -> IO (Signal b)
newSignal signalCtrlMsg = do
    signalPokeVar <- newEmptyMVar
    return Signal{..}

poke :: Signal b -> IO ()
poke Signal{..} = void $ tryPutMVar signalPokeVar ()

-- | Keep retrying an action until it succeeds, blocking between attempts.
retryIfNothing :: forall a b. Signal b -> IO (Maybe a) -> IO (Either b a)
retryIfNothing Signal{..} act = go
  where
    go :: IO (Either b a)
    go = do
      ma <- act
      case ma of
        Just a  -> return (Right a)
        Nothing -> do
          -- If the action did not return a value, wait for a concurrent thread
          -- to signal that something has changed (may already have happened as
          -- the action was running, of course, in which case we try again
          -- immediately).
          --
          -- If there were multiple changes, then the signal will only remember
          -- that there /was/ a change, not how many of them. This is ok,
          -- however: we run the action again in this new state, no matter how
          -- many changes took place. If in that new state the action still
          -- fails, then we will wait for further changes on the next iteration.
          mCtrlMsg <- signalCtrlMsg
          case mCtrlMsg of
            Nothing      -> takeMVar signalPokeVar >>= \() -> go
            Just ctrlMsg -> return (Left ctrlMsg)

{-------------------------------------------------------------------------------
  Auxiliary
-------------------------------------------------------------------------------}

orElseM :: IO (Maybe a) -> IO (Maybe a) -> IO (Maybe a)
orElseM f g = f >>= maybe g (return . Just)
