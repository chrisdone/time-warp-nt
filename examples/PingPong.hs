{-# LANGUAGE DeriveDataTypeable    #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecursiveDo           #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}

module Main where

import           Control.Monad              (forM_)
import           Control.Monad.IO.Class     (liftIO)
import           Data.Store                 (Store)
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Char8      as B8
import           Data.Data                  (Data)
import           Data.Time.Units            (Microsecond, fromMicroseconds)
import           GHC.Generics               (Generic)
import           Mockable.Concurrent        (delay, fork, killThread)
import           Mockable.Production
import           Network.Transport.Abstract (closeTransport)
import           Network.Transport.Concrete (concrete)
import qualified Network.Transport.TCP      as TCP
import           Node
import           Node.Message.Store         (StoreP (..))
import           Node.Util.Monitor          (startMonitor)
import           System.Random

-- | Type for messages from the workers to the listeners.
data Ping = Ping
deriving instance Generic Ping
deriving instance Data Ping
deriving instance Show Ping
instance Store Ping
instance Message Ping where
    formatMessage _ = "Ping"

-- | Type for messages from the listeners to the workers.
data Pong = Pong
deriving instance Generic Pong
deriving instance Show Pong
instance Store Pong

type Packing = StoreP

worker :: NodeId -> StdGen -> [NodeId] -> Worker Packing BS.ByteString Production
worker anId generator peerIds = pingWorker generator
    where
    pingWorker :: StdGen -> SendActions Packing BS.ByteString Production -> Production ()
    pingWorker gen sendActions = loop gen
        where
        loop :: StdGen -> Production ()
        loop g = do
            let (i, gen') = randomR (0,1000000) g
                us = fromMicroseconds i :: Microsecond
            delay us
            let pong :: NodeId -> ConversationActions Ping Pong Production -> Production ()
                pong peerId cactions = do
                    liftIO . putStrLn $ show anId ++ " sent PING to " ++ show peerId
                    received <- recv cactions maxBound
                    case received of
                        Just Pong -> liftIO . putStrLn $ show anId ++ " heard PONG from " ++ show peerId
                        Nothing -> error "Unexpected end of input"
            forM_ peerIds $ \peerId -> withConnectionTo sendActions peerId $
                \_ -> Conversation (pong peerId)
            loop gen'

listeners :: NodeId -> BS.ByteString -> [Listener Packing BS.ByteString Production]
listeners anId = const [pongListener]
    where
    pongListener :: Listener Packing BS.ByteString Production
    pongListener = Listener $ \peerData peerId (cactions :: ConversationActions Pong Ping Production) -> do
        liftIO . putStrLn $ show anId ++  " heard PING from " ++ show peerId ++ " with peer data " ++ B8.unpack peerData
        send cactions Pong
        liftIO . putStrLn $ show anId ++ " sent PONG to " ++ show peerId

main :: IO ()
main = runProduction $ do

    let params = TCP.defaultTCPParameters { TCP.tcpCheckPeerHost = True }
    Right transport_ <- liftIO $
        TCP.createTransport (TCP.defaultTCPAddr "127.0.0.1" "10128") params
    let transport = concrete transport_

    let prng1 = mkStdGen 0
    let prng2 = mkStdGen 1
    let prng3 = mkStdGen 2
    let prng4 = mkStdGen 3

    liftIO . putStrLn $ "Starting nodes"
    node (simpleNodeEndPoint transport) (const noReceiveDelay) (const noReceiveDelay)
         prng1 StoreP (B8.pack "I am node 1") defaultNodeEnvironment $ \node1 ->
        NodeAction (listeners . nodeId $ node1) $ \sactions1 -> do
            _ <- startMonitor 8000 runProduction node1
            node (simpleNodeEndPoint transport) (const noReceiveDelay) (const noReceiveDelay)
                  prng2 StoreP (B8.pack "I am node 2") defaultNodeEnvironment $ \node2 ->
                NodeAction (listeners . nodeId $ node2) $ \sactions2 -> do
                    _ <- startMonitor 8001 runProduction node2
                    tid1 <- fork $ worker (nodeId node1) prng3 [nodeId node2] sactions1
                    tid2 <- fork $ worker (nodeId node2) prng4 [nodeId node1] sactions2
                    liftIO . putStrLn $ "Hit return to stop"
                    _ <- liftIO getChar
                    killThread tid1
                    killThread tid2
                    liftIO . putStrLn $ "Stopping nodes"
    liftIO . putStrLn $ "All done."
    closeTransport transport
