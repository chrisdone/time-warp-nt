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

{-# LANGUAGE FlexibleContexts    #-}
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
  , Enqueue(..)
  , EnqueuePolicy
  , defaultEnqueuePolicy
    -- ** Dequeueing policy
  , RateLimit(..)
  , MaxInFlight(..)
  , Dequeue(..)
  , DequeuePolicy
  , defaultDequeuePolicy
    -- ** Failure policy
  , FailurePolicy
  , ReconsiderAfter(..)
  , defaultFailurePolicy
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
import Control.Exception
import Control.Lens
import Control.Monad
import Control.Monad.IO.Class
import Data.Map.Strict (Map)
import Data.Maybe (catMaybes)
import Data.Monoid ((<>))
import Data.Set (Set)
import Data.Text (Text)
import Formatting (sformat, (%), shown)
import System.Wlog.CanLog
import qualified Data.Map.Strict as Map
import qualified Data.Set        as Set

import Network.Broadcast.OutboundQueue.Classification
import Network.Broadcast.OutboundQueue.ConcurrentMultiQueue (MultiQueue)
import qualified Network.Broadcast.OutboundQueue.ConcurrentMultiQueue as MQ
import qualified Mockable as M

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
  deriving (Show)

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
  Enqueueing policy

  The enquing policy is intended to guarantee that at the point of enqueing
  we can be reasonably sure that the message will get to where it needs to be
  within the maximum time bounds.
-------------------------------------------------------------------------------}

-- | Maximum number of messages allowed "ahead" of the message to be enqueued
--
-- This is the total number of messages currently in-flight or in-queue with a
-- precedence at or above the message to be enqueued (i.e., all messages that
-- will be handled before the new message).
--
-- If we cannot find any alternative that doesn't match requirements we simply
-- give up on (this list of) alternatives.
newtype MaxAhead = MaxAhead Int

data Enqueue = Enqueue {
      enqNodeType   :: NodeType
    , enqMaxAhead   :: MaxAhead
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
        Enqueue NodeCore  (MaxAhead 0) P1
      , Enqueue NodeRelay (MaxAhead 0) P3
      ]
    go MsgMPC _ = [
        Enqueue NodeCore (MaxAhead 1) P2
        -- not sent to relay nodes
      ]
    go MsgTransaction _ = [
        Enqueue NodeCore (MaxAhead 20) P4
        -- not sent to relay nodes
      ]
defaultEnqueuePolicy NodeRelay = go
  where
    -- Enqueue policy for relay nodes
    go :: EnqueuePolicy
    go MsgBlockHeader _ = [
        Enqueue NodeRelay (MaxAhead 0) P1
      , Enqueue NodeCore  (MaxAhead 0) P2
      , Enqueue NodeEdge  (MaxAhead 0) P3
      ]
    go MsgTransaction _ = [
        Enqueue NodeCore  (MaxAhead 20) P4
      , Enqueue NodeRelay (MaxAhead 20) P5
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
        Enqueue NodeRelay (MaxAhead 0) P1
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
  Failure policy
-------------------------------------------------------------------------------}

-- | The failure policy determines what happens when a failure occurs as we send
-- a message to a particular node: how long (in sec) should we wait until we
-- consider this node to be a viable alternative again?
type FailurePolicy = NodeType -> MsgType -> SomeException -> ReconsiderAfter

-- | How long (in sec) after a failure should we reconsider this node again for
-- new messages?
newtype ReconsiderAfter = ReconsiderAfter Int

-- | Default failure policy
--
-- TODO: Implement proper policy
defaultFailurePolicy :: NodeType -- ^ Our node type
                     -> FailurePolicy
defaultFailurePolicy _ourType _theirType _msgType _err = ReconsiderAfter 200

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

mqEnqueue :: (MonadIO m, Ord nid)
          => MQ msg nid -> Packet msg nid -> Precedence -> m ()
mqEnqueue qs pkt@Packet{dest} prec = liftIO $
  MQ.enqueue qs [ KeyByPrec prec
                , KeyByDest dest
                , KeyByDestPrec dest prec
                ]
                pkt

-- | Check whether a node is not currently busy
--
-- (i.e., number of in-flight messages is less than the max)
type NotBusy nid = nid -> Bool

mqDequeue :: (MonadIO m, Ord nid)
          => MQ msg nid -> NotBusy nid -> m (Maybe (Packet msg nid))
mqDequeue qs notBusy =
    foldr1 orElseM [
        liftIO $ MQ.dequeue (KeyByPrec prec) (notBusy . dest) qs
      | prec <- enumPrecHighestFirst
      ]

{-------------------------------------------------------------------------------
  State Initialization
-------------------------------------------------------------------------------}

-- | How many messages are in-flight to each destination?
type InFlight nid = Map nid Int

-- | Which nodes suffered from a recent communication failure?
type Failures nid = Set nid

inFlightTo :: Ord nid => nid -> Lens' (InFlight nid) Int
inFlightTo nid = at nid . anon 0 (== 0)

-- | The outbound queue (opaque data structure)
data OutboundQ msg nid = ( ClassifyMsg msg
                         , ClassifyNode nid
                         , Ord nid
                         , Show nid
                         , Show msg
                         ) => OutQ {
      -- | Enqueuing policy
      qEnqueuePolicy :: EnqueuePolicy

      -- | Dequeueing policy
    , qDequeuePolicy :: DequeuePolicy

      -- | Failure policy
    , qFailurePolicy :: FailurePolicy

      -- | Messages sent but not yet acknowledged
    , qInFlight :: MVar (InFlight nid)

      -- | Messages scheduled but not yet sent
    , qScheduled :: MQ msg nid

      -- | Known peers
    , qPeers :: MVar (Peers nid)

      -- | Recent communication failures
    , qFailures :: MVar (Failures nid)

      -- | Used to send control messages to the main thread
    , qCtrlMsg :: MVar CtrlMsg

      -- | Signal we use to wake up blocked threads
    , qSignal :: Signal CtrlMsg
    }

-- | Initialize the outbound queue
--
-- NOTE: The dequeuing thread must be started separately. See 'dequeueThread'.
new :: ( MonadIO m
       , ClassifyMsg msg
       , ClassifyNode nid
       , Ord nid
       , Show nid
       , Show msg
       )
    => EnqueuePolicy
    -> DequeuePolicy
    -> FailurePolicy
    -> m (OutboundQ msg nid)
new qEnqueuePolicy qDequeuePolicy qFailurePolicy = liftIO $ do
    qInFlight  <- newMVar Map.empty
    qScheduled <- MQ.new
    qPeers     <- newMVar mempty
    qCtrlMsg   <- newEmptyMVar
    qFailures  <- newMVar Set.empty

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
intEnqueue :: forall m msg nid. (MonadIO m, WithLogger m)
           => OutboundQ msg nid -> msg -> Origin -> Peers nid -> m ()
intEnqueue outQ@OutQ{..} msg origin peers =
    forM_ (qEnqueuePolicy (classifyMsg msg) origin) $ \enq@Enqueue{..} -> do

      let fwdSets :: AllOf (Alts nid)
          fwdSets = peers ^. peersOfType enqNodeType

      sentTo <- forM fwdSets $ \alts -> do
        mAlt <- liftIO $ pickAlt outQ enq alts
        case mAlt of
          Nothing  -> logWarning $ noAlt alts
          Just alt -> liftIO $ do
            mqEnqueue qScheduled (Packet msg alt) enqPrecedence
            poke qSignal
        return mAlt

      -- Log an error if we didn't manage to send the message to any peer
      -- at all (provided that we were configured to send it to some)
      when (not (null fwdSets) && null (catMaybes sentTo :: [nid])) $
        logError msgLost
  where
    noAlt :: [nid] -> Text
    noAlt = sformat ("Could not choose suitable alternative from " % shown)

    msgLost :: Text
    msgLost = sformat ( "Failed to send message "
                      % shown
                      % " with origin "
                      % shown
                      % " to peers "
                      % shown
                      )
                      msg
                      origin
                      peers

pickAlt :: MonadIO m
        => OutboundQ msg nid -> Enqueue -> Alts nid -> m (Maybe nid)
pickAlt outQ Enqueue{enqMaxAhead = MaxAhead maxAhead, ..} alts =
    foldr1 orElseM [ do
        failure <- hasRecentFailure outQ alt
        ahead   <- countAhead outQ alt enqPrecedence
        return $ if not failure && ahead <= maxAhead
                   then Just alt
                   else Nothing
      | alt <- alts
      ]

-- | Check how many messages are currently ahead
--
-- NOTE: This is of course a highly dynamic value; by the time we get to
-- actually enqueue the message the value might be slightly different. Bounds
-- are thus somewhat fuzzy.
--
-- TODO: Include in-flight.
countAhead :: MonadIO m => OutboundQ msg nid -> nid -> Precedence -> m Int
countAhead OutQ{qScheduled} nid prec = sum <$>
    forM [prec .. maxBound] (\prec' -> liftIO $
      MQ.sizeBy (KeyByDestPrec nid prec') qScheduled)

{-------------------------------------------------------------------------------
  Interpreter for the dequeueing policy
-------------------------------------------------------------------------------}

checkMaxInFlight :: (Ord nid, ClassifyNode nid)
                 => DequeuePolicy -> InFlight nid -> NotBusy nid
checkMaxInFlight dequeuePolicy inFlight nid =
    Map.findWithDefault 0 nid inFlight < n
  where
    MaxInFlight n = deqMaxInFlight (dequeuePolicy (classifyNode nid))

applyRateLimit :: (MonadIO m, ClassifyNode nid) => DequeuePolicy -> nid -> m ()
applyRateLimit dequeuePolicy nid = liftIO $
    case deqRateLimit (dequeuePolicy (classifyNode nid)) of
      NoRateLimiting -> return ()
      MaxMsgPerSec n -> threadDelay (1000000 `div` n)

intDequeue :: forall m msg nid.
              OutboundQ msg nid
           -> ThreadRegistry m
           -> SendMsg m msg nid
           -> m (Maybe CtrlMsg)
intDequeue outQ@OutQ{..} threadRegistry@TR{} sendMsg = do
    mPacket <- getPacket
    case mPacket of
      Left ctrlMsg -> return $ Just ctrlMsg
      Right packet -> sendPacket packet >> return Nothing
  where
    getPacket :: m (Either CtrlMsg (Packet msg nid))
    getPacket = retryIfNothing qSignal $ do
      inFlight <- liftIO $ readMVar qInFlight
      mqDequeue qScheduled (checkMaxInFlight qDequeuePolicy inFlight)

    -- Send the packet we just dequeued
    --
    -- At this point we have dequeued the message but not yet recorded it as
    -- in-flight. That's okay though: the only function whose behaviour is
    -- affected by 'rsInFlight' is 'intDequeue', the main thread (this thread) is
    -- the only thread calling 'intDequeue', and we will update 'rsInFlight'
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
    sendPacket :: Packet msg nid -> m ()
    sendPacket packet@Packet{..} = do
      applyMVar_ qInFlight $ inFlightTo dest %~ (\n -> n + 1)
      forkThread threadRegistry $ \unmask -> do
        mErr <- M.try $ unmask $ do
          sendMsg payload dest
          applyRateLimit qDequeuePolicy dest
        case mErr of
          Left err -> intFailure outQ threadRegistry packet err
          Right () -> return ()
        applyMVar_ qInFlight $ inFlightTo dest %~ (\n -> n - 1)
        liftIO $ poke qSignal

{-------------------------------------------------------------------------------
  Interpreter for failure policy
-------------------------------------------------------------------------------}

-- | What do we know when sending a message fails?
--
-- NOTE: Since we don't send messages to nodes listed in failures, we can
-- assume that there isn't an existing failure here.
--
-- TODO: Reduce the time of the actual send from
intFailure :: forall m msg nid.
              OutboundQ msg nid
           -> ThreadRegistry m
           -> Packet msg nid
           -> SomeException
           -> m ()
intFailure OutQ{..} threadRegistry@TR{} Packet{..} err = do
    applyMVar_ qFailures $ Set.insert dest
    forkThread threadRegistry $ \unmask -> do
      unmask $ liftIO $ threadDelay (delay * 1000000)
      applyMVar_ qFailures $ Set.delete dest
  where
    delay :: Int
    ReconsiderAfter delay = qFailurePolicy (classifyNode dest)
                                           (classifyMsg payload)
                                           err

hasRecentFailure :: MonadIO m => OutboundQ msg nid -> nid -> m Bool
hasRecentFailure OutQ{..} nid = liftIO $ Set.member nid <$> readMVar qFailures

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
  deriving (Show)

-- | Queue a message to be send to all peers
--
-- The message will be sent to the specified peers as well as any subscribers.
--
-- TODO: Ultimately we want to move to a model where we /only/ have
-- subscription; after all, it's no problem for statically configured nodes to
-- also subscribe when they are created. We don't use such a model just yet to
-- make integration easier.
--
-- TODO: Offer a synchronous send.
enqueue :: (MonadIO m, WithLogger m)
        => OutboundQ msg nid
        -> msg        -- ^ Message to send
        -> Origin     -- ^ Origin of this message
        -> Peers nid  -- ^ Additional peers (in addition to subscribers)
        -> m ()
enqueue outQ@OutQ{..} msg origin peers' = do
    peers <- liftIO $ readMVar qPeers
    intEnqueue outQ msg origin (peers <> peers')

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
type SendMsg m msg nid = msg -> nid -> m ()

-- | The dequeue thread
--
-- It is the responsibility of the next layer up to fork this thread; this
-- function does not return unless told to terminate using 'waitShutdown'.
dequeueThread :: forall m msg nid. (
                   MonadIO              m
                 , M.Mockable M.Bracket m
                 , M.Mockable M.Catch   m
                 , M.Mockable M.Async   m
                 , M.Mockable M.Fork    m
                 , Ord (M.ThreadId      m)
                 )
              => OutboundQ msg nid -> SendMsg m msg nid -> m ()
dequeueThread outQ@OutQ{..} sendMsg = withThreadRegistry $ \threadRegistry ->
    let loop :: m ()
        loop = do
          mCtrlMsg <- intDequeue outQ threadRegistry sendMsg
          case mCtrlMsg of
            Nothing      -> loop
            Just ctrlMsg -> do
              waitAllThreads threadRegistry
              case ctrlMsg of
                Shutdown ack -> do liftIO $ putMVar ack ()
                Flush    ack -> do liftIO $ putMVar ack ()
                                   loop

    in loop

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
waitShutdown :: MonadIO m => OutboundQ msg nid -> m ()
waitShutdown OutQ{..} = liftIO $ do
    ack <- newEmptyMVar
    putMVar qCtrlMsg $ Shutdown ack
    poke qSignal
    takeMVar ack

-- | Wait for all messages currently enqueued to have been sent
flush :: MonadIO m => OutboundQ msg nid -> m ()
flush OutQ{..} = liftIO $ do
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
subscribe :: MonadIO m => OutboundQ msg nid -> Peers nid -> m ()
subscribe OutQ{..} peers' = applyMVar_ qPeers (<> peers')

-- | Unsubscribe some nodes
--
-- See 'subscribe'.
--
-- TODO: Delete now-useless outbound messages from the queues.
unsubscribe :: MonadIO m => Ord nid => OutboundQ msg nid -> [nid] -> m ()
unsubscribe OutQ{..} = applyMVar_ qPeers . removePeers . Set.fromList

{-------------------------------------------------------------------------------
  Auxiliary: starting and registering threads
-------------------------------------------------------------------------------}

data ThreadRegistry m =
       ( MonadIO              m
       , M.Mockable M.Async   m
       , M.Mockable M.Bracket m
       , M.Mockable M.Fork    m
       , M.Mockable M.Catch   m
       , Ord (M.ThreadId      m)
       )
    => TR (MVar (Map (M.ThreadId m) (M.Promise m ())))

-- | Create a new thread registry, killing all threads when the action
-- terminates.
withThreadRegistry :: ( MonadIO              m
                      , M.Mockable M.Bracket m
                      , M.Mockable M.Async   m
                      , M.Mockable M.Fork    m
                      , M.Mockable M.Catch   m
                      , Ord (M.ThreadId      m)
                      )
                   => (ThreadRegistry m -> m ()) -> m ()
withThreadRegistry k = do
    threadRegistry <- liftIO $ TR <$> newMVar Map.empty
    k threadRegistry `M.finally` killAllThreads threadRegistry

killAllThreads :: ThreadRegistry m -> m ()
killAllThreads (TR reg) = do
    threads <- applyMVar reg $ \threads -> (Map.empty, Map.elems threads)
    mapM_ M.cancel threads

waitAllThreads :: ThreadRegistry m -> m ()
waitAllThreads (TR reg) = do
    threads <- applyMVar reg $ \threads -> (Map.empty, Map.elems threads)
    mapM_ M.wait threads

type Unmask m = forall a. m a -> m a

-- | Fork a new thread, taking care of registration and unregistration
forkThread :: ThreadRegistry m -> (Unmask m -> m ()) -> m ()
forkThread (TR reg) threadBody = M.mask_ $ do
    barrier <- liftIO $ newEmptyMVar
    thread  <- M.asyncWithUnmask $ \unmask -> do
                 tid <- M.myThreadId
                 liftIO $ takeMVar barrier
                 threadBody unmask `M.finally`
                   applyMVar_ reg (at tid .~ Nothing)
    tid     <- M.asyncThreadId thread
    applyMVar_ reg (at tid .~ Just thread)
    liftIO $ putMVar barrier ()

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

    -- | Check to see if there is an out-of-bound control message available
  , signalCtrlMsg :: IO (Maybe b)
  }

newSignal :: IO (Maybe b) -> IO (Signal b)
newSignal signalCtrlMsg = do
    signalPokeVar <- newEmptyMVar
    return Signal{..}

poke :: Signal b -> IO ()
poke Signal{..} = void $ tryPutMVar signalPokeVar ()

-- | Keep retrying an action until it succeeds, blocking between attempts.
retryIfNothing :: forall m a b. MonadIO m
               => Signal b -> m (Maybe a) -> m (Either b a)
retryIfNothing Signal{..} act = go
  where
    go :: m (Either b a)
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
          mCtrlMsg <- liftIO $ signalCtrlMsg
          case mCtrlMsg of
            Just ctrlMsg ->
              return (Left ctrlMsg)
            Nothing -> do
              liftIO $ takeMVar signalPokeVar
              go

{-------------------------------------------------------------------------------
  Auxiliary
-------------------------------------------------------------------------------}

orElseM :: Monad m => m (Maybe a) -> m (Maybe a) -> m (Maybe a)
orElseM f g = f >>= maybe g (return . Just)

applyMVar :: MonadIO m => MVar a -> (a -> (a, b)) -> m b
applyMVar mv f = liftIO $ modifyMVar mv $ \a -> return $! f a

applyMVar_ :: MonadIO m => MVar a -> (a -> a) -> m ()
applyMVar_ mv f = liftIO $ modifyMVar_ mv $ \a -> return $! f a
