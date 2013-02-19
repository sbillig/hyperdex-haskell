{-# LANGUAGE ForeignFunctionInterface #-}

module Database.HyperDex.Internal.Client where

import Foreign
import Foreign.C.Types
import Foreign.C.String

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.Map (Map)
import qualified Data.Map.Strict as M

import Data.IORef
import Control.Concurrent.MVar
import Control.Concurrent.Chan
import Control.Applicative
import Control.Monad.Loops

{#import Database.HyperDex.Internal.Types#}

#include <hyperclient.h>

{#context lib="hyperclient" prefix="hyperclient"#}
{#pointer *hyperclient as ClientPtr#}

data Client = Client (ForeignPtr ()) Pending

type Pending = IORef (Map ReqId Req)

type ReqId = {#type int64_t#}
type ReqPayload = (ForeignPtr CInt, ForeignPtr (Ptr Attribute), ForeignPtr CULong)


data Req = WriteReq  (ForeignPtr CInt) (MVar Response)
         | ReadReq   ReqPayload (MVar Response)
         | SearchReq ReqPayload (Chan Response)

data Response = EmptyResponse
              | WriteResponse ReturnCode
              | ReadResponse  ReturnCode [Attribute]
                deriving Show


withClientPtr :: Client -> (ClientPtr -> IO a) -> IO a
withClientPtr (Client fp _) action = withForeignPtr fp action

connect :: ByteString -> Int -> IO Client
connect ip port =
    B.useAsCString ip $ \ipp -> do
      fp <- {#call create#} ipp (fromIntegral port) >>= newForeignPtr p_destroy
      Client fp <$> newIORef (M.empty)

foreign import ccall "hyperclient.h &hyperclient_destroy"
  p_destroy :: FunPtr (ClientPtr -> IO ())

addPending :: Client -> ReqId -> Req -> IO ()
addPending (Client _ p) i r = modifyIORef p $ M.insert i r

getPending :: Client -> ReqId -> IO (Maybe Req, Int)
getPending (Client _ p) i = do
  m <- readIORef p
  let (mv, m') = M.updateLookupWithKey (\_ _ -> Nothing) i m
      count = M.size m'
  writeIORef p m'
  return (mv, count)

numPending :: Client -> IO Int
numPending (Client _ p) = M.size <$> readIORef p

addSpace :: Client -> ByteString -> IO ReturnCode
addSpace client desc =
    withClientPtr  client $ \cptr ->
    B.useAsCString desc   $ \sptr ->
        cToEnum <$> {#call add_space#} cptr sptr

rmSpace :: Client -> ByteString -> IO ReturnCode
rmSpace client space =
    withClientPtr  client $ \cptr ->
    B.useAsCString space  $ \sptr ->
        cToEnum <$> {#call rm_space#} cptr sptr

attributeType :: Client -> ByteString -> ByteString -> IO (Datatype, ReturnCode)
attributeType c s f =
    withClientPtr  c $ \cptr ->
    B.useAsCString s $ \sptr ->
    B.useAsCString f $ \fptr ->
    alloca           $ \stat ->
        do
          dt <- cToEnum <$> {#call attribute_type#} cptr sptr fptr stat
          rc <- if dt == DatatypeGarbage
                then cToEnum <$> peek stat
                else return Success
          return (dt, rc)


loopAll :: Client -> Int -> IO (ReturnCode, Int)
loopAll client timeout = iterateWhile test $ loop client timeout
    where test (rc, ct) = rc == Success && ct > 0

loop :: Client -> Int -> IO (ReturnCode, Int)
loop client timeout =
    withClientPtr client $ \cptr ->
    alloca               $ \stat -> do
      rid <- {#call loop#} cptr (fromIntegral timeout) stat
      lrc <-  cToEnum <$> peek stat
      count <- if rid >= 0
               then handleResponse rid
               else numPending client
      return (lrc, count)

    where
      readPayload :: ReqPayload -> IO (ReturnCode, [Attribute])
      readPayload (rc', as', ac') = do
        rc <- cToEnum      <$> withForeignPtr rc' peek
        ac <- fromIntegral <$> withForeignPtr ac' peek
        as <- withForeignPtr as' peek
        attrs <- if rc == Success
                 then peekAndDestroy ac as
                 else return []
        return (rc, attrs)

      handleResponse :: ReqId -> IO Int
      handleResponse rid = do
        (mreq, ct) <- getPending client rid
        case mreq of
          Nothing -> return ct
          Just (WriteReq rcp m) ->
              do
                resp <- WriteResponse . cToEnum <$> withForeignPtr rcp peek
                putMVar m resp
                return ct
          Just (ReadReq pl m) ->
              do
                (rc, as) <- readPayload pl
                putMVar m $ ReadResponse rc as
                return ct
          Just req@(SearchReq pl m) ->
              do
                (rc, as) <- readPayload pl
                writeChan m $ ReadResponse rc as
                if rc == SearchDone
                 then return ct
                 else addPending client rid req >> return (ct + 1)


get :: Client -> ByteString -> ByteString -> IO (MVar Response)
get client space key =
    withClientPtr     client $ \cptr ->
    B.useAsCString    space  $ \spacep ->
    B.useAsCStringLen key    $ \(keyp, keyl) ->
        do
          status   <- mallocForeignPtr
          attrs    <- mallocForeignPtr
          attrs_sz <- mallocForeignPtr
          let kl = fromIntegral keyl

          rid <- withForeignPtr status   $ \s  ->
                 withForeignPtr attrs    $ \a  ->
                 withForeignPtr attrs_sz $ \as ->
                     {#call get#} cptr spacep keyp kl s a as

          m   <- newEmptyMVar
          addPending client rid $
            ReadReq ((castForeignPtr status), attrs, attrs_sz) m
          return m

search :: Client -> ByteString -> [Check] -> IO (Chan Response)
search client space preds =
    withClientPtr client $ \cptr        ->
    B.useAsCString space $ \spacep      ->
    xwithArrayLen preds  $ \predl predp ->
        do
          status   <- mallocForeignPtr
          attrs    <- mallocForeignPtr
          attrs_sz <- mallocForeignPtr

          rid <- withForeignPtr status   $ \s  ->
                 withForeignPtr attrs    $ \a  ->
                 withForeignPtr attrs_sz $ \as ->
                     {#call search#} cptr spacep predp predl s a as

          m   <- newChan
          addPending client rid $
            SearchReq ((castForeignPtr status), attrs, attrs_sz) m
          return m



type CWriteFunction = ClientPtr -> CString -> CString -> SizeT
                    -> Ptr Attribute -> SizeT -> Ptr CInt -> IO ReqId

type WriteFunction = Client -> ByteString -> ByteString -> [Attribute]
                   -> IO (MVar Response)

writeCall :: CWriteFunction -> WriteFunction
writeCall cfun client space key a =
  withClientPtr     client $ \cptr         ->
  B.useAsCString    space  $ \spacep       ->
  B.useAsCStringLen key    $ \(keyp, keyl) ->
  xwithArrayLen     a      $ \asl asp      ->
      do
        let kl = fromIntegral keyl

        statp <- mallocForeignPtr

        rid   <- withForeignPtr statp $ cfun cptr spacep keyp kl asp asl
        m     <- newEmptyMVar

        addPending client rid $ WriteReq statp m
        return m


put              :: WriteFunction
put              = writeCall {#call put#}

putIfNotExist    :: WriteFunction
putIfNotExist    = writeCall {#call put_if_not_exist#}

atomicAdd        :: WriteFunction
atomicAdd        = writeCall {#call atomic_add#}

atomicSub        :: WriteFunction
atomicSub        = writeCall {#call atomic_sub#}

atomicMul        :: WriteFunction
atomicMul        = writeCall {#call atomic_mul#}

atomicDiv        :: WriteFunction
atomicDiv        = writeCall {#call atomic_div#}

atomicMod        :: WriteFunction
atomicMod        = writeCall {#call atomic_mod#}

atomicAnd        :: WriteFunction
atomicAnd        = writeCall {#call atomic_and#}

atomicOr         :: WriteFunction
atomicOr         = writeCall {#call atomic_or#}

atomicXor        :: WriteFunction
atomicXor        = writeCall {#call atomic_xor#}

stringPrepend    :: WriteFunction
stringPrepend    = writeCall {#call string_prepend#}

stringAppend     :: WriteFunction
stringAppend     = writeCall {#call string_append#}

listLpush        :: WriteFunction
listLpush        = writeCall {#call list_lpush#}

listRpush        :: WriteFunction
listRpush        = writeCall {#call list_rpush#}

setAdd           :: WriteFunction
setAdd           = writeCall {#call set_add#}

setRemove        :: WriteFunction
setRemove        = writeCall {#call set_remove#}

setIntersect     :: WriteFunction
setIntersect     = writeCall {#call set_intersect#}

setUnion         :: WriteFunction
setUnion         = writeCall {#call set_union#}

{-
mapAdd           :: WriteFunction
mapAdd           = writeCall {#call map_add#}

mapRemove        :: WriteFunction
mapRemove        = writeCall {#call map_remove#}

mapAtomicAdd     :: WriteFunction
mapAtomicAdd     = writeCall {#call map_atomic_add#}

mapAtomicSub     :: WriteFunction
mapAtomicSub     = writeCall {#call map_atomic_sub#}

mapAtomicMul     :: WriteFunction
mapAtomicMul     = writeCall {#call map_atomic_mul#}

mapAtomicDiv     :: WriteFunction
mapAtomicDiv     = writeCall {#call map_atomic_div#}

mapAtomicMod     :: WriteFunction
mapAtomicMod     = writeCall {#call map_atomic_mod#}

mapAtomicAnd     :: WriteFunction
mapAtomicAnd     = writeCall {#call map_atomic_and#}

mapAtomicOr      :: WriteFunction
mapAtomicOr      = writeCall {#call map_atomic_or#}

mapAtomicXor     :: WriteFunction
mapAtomicXor     = writeCall {#call map_atomic_xor#}

mapStringPrepend :: WriteFunction
mapStringPrepend = writeCall {#call map_string_prepend#}

mapStringAppend  :: WriteFunction
mapStringAppend  = writeCall {#call map_string_append#}
-}