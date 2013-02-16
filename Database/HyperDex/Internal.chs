{-# LANGUAGE ForeignFunctionInterface #-}

module Database.HyperDex.Internal where

import Data.Int
import Data.Word
import Foreign
import Foreign.C.String
import Foreign.C.Types
import Control.Applicative
import Control.Monad
import Control.Monad.Loops

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B
import qualified Data.Map.Strict as M
import Data.Map (Map)
import Data.IORef
import Control.Concurrent.MVar

#include <hyperclient.h>
#include <hyperdex.h>

{#context lib="hyperclient" prefix="hyperclient"#}
{#pointer *hyperclient as ClientPtr#}
{#pointer *attribute as AttributePtr#}

#c
typedef struct hyperclient_attribute hyperclient_attribute;
typedef struct hyperclient_map_attribute hyperclient_map_attribute;
typedef struct hyperclient_attribute_check hyperclient_attribute_check;
typedef enum hyperdatatype hyperdatatype;
#endc

data Attribute = Attribute
    { attrName  :: ByteString
    , attrValue :: Value
    } deriving (Eq, Show)

data Value
    = SingleGeneric ByteString
    | SingleString  ByteString
    | SingleInt     Int64
    | SingleFloat   Double
    | ListGeneric   [ByteString]
    | ListString    [ByteString]
    | ListInt       [Int64]
    | ListFloat     [Double]
      deriving (Eq, Show)
    -- TODO: sets and maps

peekAttr :: AttributePtr -> IO Attribute
peekAttr p = do
  dtype <- cToEnum      <$> {#get attribute.datatype#} p :: IO Datatype
  size  <- fromIntegral <$> {#get attribute.value_sz#} p
  vptr  <- {#get attribute.value#} p
  value <- peekValue dtype size vptr
  name  <- B.packCString =<< {#get attribute.attr#} p
  return $ Attribute name value

pokeAttr :: CString -> AttributePtr -> Attribute -> IO ()
pokeAttr buf p (Attribute k v) = do
  let size = sizeOfValue v
      klen = B.length k
      valp = plusPtr buf (klen + 1)
  pokeBS (castPtr buf) k
  pokeByteOff buf klen (0 :: Word8)
  pokeValue valp v
  {#set attribute.attr#}     p $ buf
  {#set attribute.value#}    p $ valp
  {#set attribute.datatype#} p $ enumToC $ datatype v
  {#set attribute.value_sz#} p $ fromIntegral size

sizeOfAttr :: Attribute -> Int
sizeOfAttr (Attribute k v) = B.length k + 1 + sizeOfValue v

withAttributes :: [Attribute] -> ((AttributePtr, CULong) -> IO a) -> IO a
withAttributes as action =
    allocaBytes (attsz + bufsz) $ \p -> do
      foldM_ poker (plusPtr p attsz, p) as
      action (castPtr p, fromIntegral $ length as)
    where
      attsz = length as * {#sizeof attribute#}
      bufsz = sum $ map (sizeOfValue . attrValue) as
      poker (buf, p) a = pokeAttr buf p a >>
                         return (plusPtr buf (sizeOfAttr a),
                                 plusPtr p {#sizeof attribute#})

instance Storable Datatype where
    sizeOf _    = {#sizeof hyperdatatype#}
    alignment _ = 4
    peek p   = cToEnum <$> peek (castPtr p :: Ptr {#type hyperdatatype#})
    poke p x = poke (castPtr p) (enumToC x :: {#type hyperdatatype#})

datatype :: Value -> Datatype
datatype (SingleGeneric _) = DatatypeGeneric
datatype (SingleString  _) = DatatypeString
datatype (SingleInt     _) = DatatypeInt64
datatype (SingleFloat   _) = DatatypeFloat
datatype (ListGeneric   _) = DatatypeListGeneric
datatype (ListString    _) = DatatypeListString
datatype (ListInt       _) = DatatypeListInt64
datatype (ListFloat     _) = DatatypeListFloat

sizeOfValue :: Value -> Int
sizeOfValue (SingleGeneric x) = B.length x
sizeOfValue (SingleString  x) = B.length x
sizeOfValue (SingleInt     _) = 8
sizeOfValue (SingleFloat   _) = 8
sizeOfValue (ListGeneric   x) = sum $ map (sizeOf . LString) x
sizeOfValue (ListString    x) = sum $ map (sizeOf . LString) x
sizeOfValue (ListInt       x) = 8 * length x
sizeOfValue (ListFloat     x) = 8 * length x

peekValue :: Datatype -> Int -> CString -> IO Value
peekValue DatatypeGeneric     n p = SingleGeneric <$> B.packCStringLen (p, n)
peekValue DatatypeString      n p = SingleString  <$> B.packCStringLen (p, n)
peekValue DatatypeInt64       _ p = SingleInt   <$> peek (castPtr p)
peekValue DatatypeFloat       _ p = SingleFloat <$> peek (castPtr p)
peekValue DatatypeListGeneric n p = ListGeneric . (map unLString)
                                    <$> peekBytes n (castPtr p)
peekValue DatatypeListString  n p = ListString  . (map unLString)
                                    <$> peekBytes n (castPtr p)
peekValue DatatypeListInt64   n p = ListInt   <$> peekArray (div n 8) (castPtr p)
peekValue DatatypeListFloat   n p = ListFloat <$> peekArray (div n 8) (castPtr p)

pokeValue :: CString -> Value -> IO ()
pokeValue p (SingleGeneric  x) = pokeBS    (castPtr p) x
pokeValue p (SingleString   x) = pokeBS    (castPtr p) x
pokeValue p (SingleInt      x) = poke      (castPtr p) x
pokeValue p (SingleFloat    x) = poke      (castPtr p) x
pokeValue p (ListGeneric    x) = pokeArray (castPtr p) $ map LString x
pokeValue p (ListString     x) = pokeArray (castPtr p) $ map LString x
pokeValue p (ListInt        x) = pokeArray (castPtr p) x
pokeValue p (ListFloat      x) = pokeArray (castPtr p) x

peekBytes :: Storable a => Int -> Ptr a -> IO [a]
peekBytes 0 _ = return []
peekBytes n p = do
  v <- peek p
  let sz = sizeOf v
  rest <- peekBytes (n - sz) (p `plusPtr` sz)
  return $ v : rest

-- | peekArray for types without a Storable instance
peekMany :: (Ptr x -> IO a) -> Int -> Int -> Ptr x -> IO [a]
peekMany p size count ptr | count <= 0 = return []
                          | otherwise  = f (count - 1) []
    where
      f 0 acc = do e <- p ptr;                    return (e:acc)
      f n acc = do e <- p (plusPtr ptr (n*size)); f (n-1) (e:acc)

newtype LString = LString { unLString :: ByteString }
instance Storable LString where
    alignment _ = 4
    sizeOf s = 4 + B.length (unLString s)
    poke p s = do
      let len = B.length (unLString s)
      poke (castPtr p) (fromIntegral len :: Word32)
      pokeBS (castPtr p) (unLString s)

    peek p = do
      len <- peek (castPtr p) :: IO Word32
      LString <$> B.packCStringLen (castPtr p, fromIntegral len)

-- | Blindly copy bytestring into memory location.
pokeBS :: Ptr Word8 -> ByteString -> IO ()
pokeBS p s = do
  let (fp, off, len) = B.toForeignPtr s
  withForeignPtr fp $ \sp -> do
    B.memcpy p (sp `plusPtr` off) (fromIntegral len)

cToEnum :: (Integral a, Enum b) => a -> b
cToEnum = toEnum . fromIntegral

enumToC :: (Integral a, Enum b) => b -> a
enumToC = fromIntegral . fromEnum


data Client = Client (ForeignPtr ()) Pending

type Pending = IORef (Map ReqId Req)

type ReqId = {#type int64_t#}
data Req = WriteReq  (ForeignPtr CInt) (MVar Response)
         | ReadReq   (ForeignPtr CInt) (ForeignPtr AttributePtr)
                     (ForeignPtr CULong) (MVar Response)
--         | SearchReq (ForeignPtr CInt) (ForeignPtr AttributePtr)
--                     (ForeignPtr CULong) (Chan Response)

data Response = EmptyResponse
              | WriteResponse ReturnCode
              | ReadResponse  ReturnCode [Attribute]
                deriving Show

fulfillResponse Nothing = return ()
fulfillResponse (Just (WriteReq rcp m)) =
    putMVar m =<< WriteResponse . cToEnum <$> withForeignPtr rcp peek
fulfillResponse (Just (ReadReq rcp attrpp acp m)) = do
  rc <- cToEnum      <$> withForeignPtr rcp peek
  ct <- fromIntegral <$> withForeignPtr acp peek
  attrp <- withForeignPtr attrpp peek
  putStr "fulfill read: " >> print (rc, ct, attrp)
  case rc of
    Success -> do
            as <- peekMany peekAttr {#sizeof attribute#} ct attrp
            {#call destroy_attrs#} attrp (fromIntegral ct)
            putMVar m $ ReadResponse rc as

    _ -> putMVar m $   ReadResponse rc []


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
    alloca               $ \stat ->
        do
          rid <- {#call loop#} cptr (fromIntegral timeout) stat
          rc <-  cToEnum <$> peek stat

          count <- if rid >= 0
                   then do (req, ct) <- getPending client rid
                           fulfillResponse req
                           return ct
                   else numPending client
          return (rc, count)


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
          addPending client rid $ ReadReq (castForeignPtr status) attrs attrs_sz m
          return m

type CWriteFunction = ClientPtr -> CString -> CString -> CULong
                    -> AttributePtr -> CULong -> Ptr CInt -> IO CLong

type WriteFunction = Client -> ByteString -> ByteString -> [Attribute]
                   -> IO (MVar Response)

writeCall :: CWriteFunction -> WriteFunction
writeCall cfun client space key a =
  withClientPtr     client $ \cptr         ->
  B.useAsCString    space  $ \spacep       ->
  B.useAsCStringLen key    $ \(keyp, keyl) ->
  withAttributes    a      $ \(asp, asl)   ->
      do
        let kl = fromIntegral keyl
            al = fromIntegral asl

        statp <- mallocForeignPtr

        rid   <- withForeignPtr statp $ cfun cptr spacep keyp kl asp al
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

{#enum returncode as ReturnCode
 { SUCCESS	as Success
 , NOTFOUND	as NotFound
 , SEARCHDONE	as SearchDone
 , CMPFAIL	as CmpFail
 , READONLY	as ReadOnly

   -- Error conditions
 , UNKNOWNSPACE as UnknownSpace
 , COORDFAIL	as CoordFail
 , SERVERERROR	as ServerError
 , POLLFAILED	as PollFailed
 , OVERFLOW	as Overflow
 , RECONFIGURE	as Reconfigure
 , TIMEOUT	as Timeout
 , UNKNOWNATTR	as UnknownAttr
 , DUPEATTR	as DupeAttr
 , NONEPENDING	as NonePending
 , DONTUSEKEY	as DontUseKey
 , WRONGTYPE	as WrongType
 , NOMEM	as NoMem
 , BADCONFIG	as BadConfig
 , BADSPACE	as BadSpace
 , DUPLICATE	as Duplicate
 , INTERRUPTED	as Interrupted
 , CLUSTER_JUMP as ClusterJump
 , COORD_LOGGED as CoordLogged

   -- This should never happen
 , INTERNAL	as Internal
 , EXCEPTION	as Exception
 , GARBAGE	as Garbage
 } deriving (Show, Eq) #}


{#enum hyperdatatype as Datatype
 {underscoreToCase}  with prefix = "hyper"
 deriving (Show, Eq) #}
