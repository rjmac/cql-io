-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- | This driver operates on some state which must be initialised prior to
-- executing client operations and terminated eventually. The library uses
-- <http://hackage.haskell.org/package/tinylog tinylog> for its logging
-- output and expects a 'Logger'.
--
-- For example:
--
-- @
-- > import Data.Text (Text)
-- > import Data.Functor.Identity
-- > import Database.CQL.IO as Client
-- > import Database.CQL.Protocol
-- > import qualified System.Logger as Logger
-- >
-- > g <- Logger.new Logger.defSettings
-- > c <- Client.init g defSettings
-- > let p = QueryParams One False () Nothing Nothing Nothing
-- > runClient c $ query ("SELECT cql_version from system.local" :: QueryString R () (Identity Text)) p
-- [Identity "3.2.0"]
-- > shutdown c
-- @
--

{-# LANGUAGE DeriveFunctor #-}

module Database.CQL.IO
    ( -- * Client settings
      Settings
    , defSettings
    , addContact
    , setCompression
    , setConnectTimeout
    , setContacts
    , setIdleTimeout
    , setKeyspace
    , setMaxConnections
    , setMaxStreams
    , setMaxTimeouts
    , setPolicy
    , setPoolStripes
    , setPortNumber
    , setProtocolVersion
    , setResponseTimeout
    , setSendTimeout
    , setRetrySettings
    , setMaxRecvBuffer

      -- ** Retry Settings
    , RetrySettings
    , noRetry
    , retryForever
    , maxRetries
    , adjustConsistency
    , constDelay
    , expBackoff
    , fibBackoff
    , adjustSendTimeout
    , adjustResponseTimeout

      -- * Client monad
    , Client
    , MonadClient (..)
    , ClientState
    , DebugInfo   (..)
    , QueryLike
    , PreparedStatement
    , init
    , runClient
    , retry
    , shutdown
    , debugInfo

    , query
    , query1
    , write
    , schema
    , batch

    , Page      (..)
    , emptyPage
    , paginate

    , prepare

      -- ** low-level
    , request
    , command

      -- * Policies
    , Policy (..)
    , random
    , roundRobin

      -- ** Hosts
    , Host
    , HostEvent (..)
    , InetAddr  (..)
    , hostAddr
    , dataCentre
    , rack

    -- * Exceptions
    , InvalidSettings    (..)
    , InternalError      (..)
    , HostError          (..)
    , ConnectionError    (..)
    , UnexpectedResponse (..)
    , Timeout            (..)
    ) where

import Control.Applicative
import Control.Monad.Catch
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.IORef
import Data.Maybe (isJust, listToMaybe)
import Database.CQL.Protocol
import Database.CQL.IO.Client
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.Settings
import Database.CQL.IO.Types
import Prelude hiding (init)

-- | Things that can be used as a CQL query.
class QueryLike q where
    -- | Convert the given 'QueryLike' and parameters into a request object.
    toRequest :: (MonadClient m) => q k a b -> QueryParams a -> m (Request k a b)
    -- | Re-prepares a prepared statement after receiving an 'Unprepared' exception,
    -- if applicable.  Re-throws the 'Error' otherwise.
    fixupStalePreparedStatement :: (MonadClient m, Tuple a, Tuple b) => q k a b -> Error -> m ()

instance QueryLike QueryString where
    toRequest q p = return $ RqQuery (Query q p)
    fixupStalePreparedStatement _ e = throwM e

instance QueryLike PreparedStatement where
    toRequest (PS _ qRef) p = liftIO $ do
        q <- readIORef qRef
        return $ RqExecute (Execute q p)
    fixupStalePreparedStatement (PS q qRef) _ = do
        res <- runRequest (RqPrepare (Prepare q)) -- this succeeded once already; it should be fine to redo
        case res of
            RsResult _ (PreparedResult qid _ _) -> liftIO $ atomicWriteIORef qRef qid
            _                                   -> throwM UnexpectedResponse

-- | A 'prepare'd query.
data PreparedStatement k a b = PS (QueryString k a b) (IORef (QueryId k a b))

instance Show (PreparedStatement k a b) where
  showsPrec d (PS (QueryString s) _) r =
                  let x = "PreparedStatement " ++ show s
                  in if d > 10 then "(" ++ x ++ ")" ++ r
                     else x ++ r

runRequest :: (MonadClient m, Tuple a, Tuple b) => Request k a b -> m (Response k a b)
runRequest req = do
    res <- request req
    case res of
        RsError _ e -> throwM e
        _           -> return res

runQuery :: (MonadClient m, Tuple a, Tuple b, QueryLike q) => q k a b -> QueryParams a -> m (Response k a b)
runQuery q p = do
    req <- toRequest q p
    runRequest req `catch` handleUnprepared
    where handleUnprepared e@(Unprepared _ _) = do
              fixupStalePreparedStatement q e
              runQuery q p
          handleUnprepared e = do
              throwM e

-- | Run a CQL read-only query against a Cassandra node.
query :: (MonadClient m, Tuple a, Tuple b, QueryLike q) => q R a b -> QueryParams a -> m [b]
query q p = do
    r <- runQuery q p
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> throwM UnexpectedResponse

-- | Run a CQL read-only query against a Cassandra node.
query1 :: (MonadClient m, Tuple a, Tuple b, QueryLike q) => q R a b -> QueryParams a -> m (Maybe b)
query1 q p = listToMaybe <$> query q p

-- | Run a CQL insert/update query against a Cassandra node.
write :: (MonadClient m, Tuple a, QueryLike q) => q W a () -> QueryParams a -> m ()
write q p = void $ runQuery q p

-- | Run a CQL schema query against a Cassandra node.
schema :: (MonadClient m, Tuple a, QueryLike q) => q S a () -> QueryParams a -> m (Maybe SchemaChange)
schema x y = do
    r <- runQuery x y
    case r of
        RsResult _ (SchemaChangeResult s) -> return $ Just s
        RsResult _ VoidResult             -> return Nothing
        _                                 -> throwM UnexpectedResponse

-- | Run a batch query against a Cassandra node.
batch :: MonadClient m => Batch -> m ()
batch b = command (RqBatch b)

-- | Return value of 'paginate'. Contains the actual result values as well
-- as an indication of whether there is more data available and the actual
-- action to fetch the next page.
data Page a = Page
    { hasMore  :: !Bool
    , result   :: [a]
    , nextPage :: Client (Page a)
    } deriving (Functor)

-- | A page with an empty result list.
emptyPage :: Page a
emptyPage = Page False [] (return emptyPage)

-- | Run a CQL read-only query against a Cassandra node.
--
-- This function is like 'query', but limits the result size to 10000
-- (default) unless there is an explicit size restriction given in
-- 'QueryParams'. The returned 'Page' can be used to continue the query.
--
-- Please note that -- as of Cassandra 2.1.0 -- if your requested page size
-- is equal to the result size, 'hasMore' might be true and a subsequent
-- 'nextPage' will return an empty list in 'result'.
paginate :: (MonadClient m, Tuple a, Tuple b, QueryLike q) => q R a b -> QueryParams a -> m (Page b)
paginate q p = do
    let p' = p { pageSize = pageSize p <|> Just 10000 }
    r <- runQuery q p'
    case r of
        RsResult _ (RowsResult m b) ->
            if isJust (pagingState m) then
                return $ Page True b (paginate q p' { queryPagingState = pagingState m })
            else
                return $ Page False b (return emptyPage)
        _ -> throwM UnexpectedResponse

-- | Prepare a CQL query on a Cassandra node for multiple uses.
--
-- The resulting 'PreparedStatement' does not have to be used on any
-- particular connection; if it is used on a node that doesn't know
-- about it, the client will automatically re-prepare it when the
-- 'Unprepared' error is signalled.
prepare :: (MonadClient m, Tuple a, Tuple b) => QueryString k a b -> m (PreparedStatement k a b)
prepare q = do
    r <- runRequest (RqPrepare (Prepare q))
    case r of
        RsResult _ (PreparedResult qid _ _) -> liftIO $ do
            qRef <- newIORef qid
            return $ PS q qRef
        _                                   -> do
            throwM UnexpectedResponse
