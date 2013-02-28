
{-# LANGUAGE ForeignFunctionInterface #-}

module Database.HyperDex.Internal.Client where

import Foreign
import Foreign.C.Types
import Foreign.C.String
import Control.Applicative

import Data.ByteString (ByteString)
import qualified Data.ByteString as B

{#import Database.HyperDex.Internal.Types#}

#include <hyperclient.h>

{#context lib="hyperclient" prefix="hyperclient"#}
{#pointer *hyperclient as ClientPtr#}
newtype Client = Client (ForeignPtr ())

withWriteReq :: (RawStatusPtr -> IO a) -> IO (a, Request)
withWriteReq action = do
  stat <- mallocForeignPtr
  res  <- withForeignPtr stat action
  return (res, WriteReq stat)

withReadReq :: Bool
            -> (RawStatusPtr -> RawAttrPtr -> RawSizePtr -> IO a)
            -> IO (a, Request)
withReadReq isMulti action = do
  stat     <- mallocForeignPtr
  attrs    <- mallocForeignPtr
  attrs_sz <- mallocForeignPtr

  rid <- withForeignPtr stat     $ \s  ->
         withForeignPtr attrs    $ \a  ->
         withForeignPtr attrs_sz $ \as ->
             action s a as
  return (rid, ReadReq isMulti stat attrs attrs_sz)

withClientPtr :: Client -> (ClientPtr -> IO a) -> IO a
withClientPtr (Client fp) = withForeignPtr fp

connect :: ByteString -> Int -> IO Client
connect ip port = B.useAsCString ip $ \ipp ->
                  Client <$> ({#call create#} ipp (fromIntegral port)
                              >>= newForeignPtr p_destroy)

foreign import ccall "hyperclient.h &hyperclient_destroy"
  p_destroy :: FunPtr (ClientPtr -> IO ())

addSpace :: Client -> ByteString -> IO ReturnCode
addSpace c desc = withClientPtr c     $ \cp ->
                  B.useAsCString desc $ \dp ->
                      cToEnum <$> {#call add_space#} cp dp

rmSpace :: Client -> Space -> IO ReturnCode
rmSpace c s = cToEnum <$> withCS c s {#call rm_space#}


attributeType :: Client -> Space -> ByteString -> IO (Datatype, ReturnCode)
attributeType c s f =
    withCS c s       $ \cp sp ->
    B.useAsCString f $ \fp    ->
    alloca           $ \stat  ->
        do
          dt <- cToEnum <$> {#call attribute_type#} cp sp fp stat
          rc <- if dt == DatatypeGarbage
                then cToEnum <$> peek stat
                else return Success
          return (dt, rc)

loop :: Client -> Int -> IO (ReturnCode, ReqId)
loop client timeout =
    withClientPtr client $ \cptr ->
    alloca               $ \stat -> do
      rid <- {#call loop#} cptr (fromIntegral timeout) stat
      lrc <- cToEnum <$> peek stat
      return (lrc, rid)


withCS :: Client -> Space -> (ClientPtr -> CString -> IO a) -> IO a
withCS c s action = withClientPtr  c $ \cp ->
                    B.useAsCString s (action cp)

withCSK :: Client -> Space -> Key
        -> (ClientPtr -> CString -> CString -> SizeT -> IO a)
        -> IO a
withCSK c s k action = withCS c s          $ \cp sp ->
                       B.useAsCStringLen k $ \(kp, kl) ->
                           action cp sp kp (fromIntegral kl)

get :: Client -> Space -> Key -> IO (ReqId, Request)
get c s k = withCSK c s k     $ \cp sp kp kl ->
            withReadReq False $ {#call get#} cp sp kp kl


del :: Client -> Space -> Key -> IO (ReqId, Request)
del c s k = withCSK c s k $ \cp sp kp kl ->
            withWriteReq  $ {#call del#} cp sp kp kl


search :: Client -> Space -> [Check] -> IO (ReqId, Request)
search c s preds =
    withCS c s           $ \cp sp       ->
    xwithArrayLen preds  $ \predl predp ->
        withReadReq True $ {#call search#} cp sp predp predl


type CWriteFunction = ClientPtr -> CString -> CString -> SizeT
                    -> Ptr Attribute -> SizeT -> Ptr CInt -> IO ReqId

type WriteFunction = Client -> Space -> Key -> [Attribute]
                   -> IO (ReqId, Request)

writeCall :: CWriteFunction -> WriteFunction
writeCall cfun c s k a =
    withCSK c s k    $ \cp sp kp kl ->
    xwithArrayLen a  $ \asl asp     ->
        withWriteReq $ cfun cp sp kp kl asp asl


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
