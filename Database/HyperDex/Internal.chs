{-# LANGUAGE ForeignFunctionInterface #-}

module Database.HyperDex.Internal where

import Data.Int
import Data.Word
import Foreign
import Foreign.C.String
import Foreign.C.Types
import Control.Applicative
import Control.Monad

import qualified Data.ByteString as B
import qualified  Data.ByteString.Internal as B

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
    { attrName  :: B.ByteString
    , attrValue :: Value
    } deriving (Eq, Show)

data Value
    = SingleGeneric B.ByteString
    | SingleString  B.ByteString
    | SingleInt     Int64
    | SingleFloat   Double
    | ListGeneric   [B.ByteString]
    | ListString    [B.ByteString]
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
    peek p = peekEnum (castPtr p :: Ptr {#type hyperdatatype#})
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

newtype LString = LString { unLString :: B.ByteString }
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
pokeBS :: Ptr Word8 -> B.ByteString -> IO ()
pokeBS p s = do
  let (fp, off, len) = B.toForeignPtr s
  withForeignPtr fp $ \sp -> do
    B.memcpy p (sp `plusPtr` off) (fromIntegral len)

cToEnum :: (Integral a, Enum b) => a -> b
cToEnum = toEnum . fromIntegral

enumToC :: (Integral a, Enum b) => b -> a
enumToC = fromIntegral . fromEnum

peekEnum :: (Integral a, Storable a, Enum b) => Ptr a -> IO b
peekEnum p = cToEnum <$> peek p

withCStringLenIntConv :: Num n => String -> ((CString, n) -> IO a) -> IO a
withCStringLenIntConv s f = withCStringLen s $ \(p, n) -> f (p, fromIntegral n)

peekCStringLenIntConv :: Integral n => (CString, n) -> IO String
peekCStringLenIntConv (s, n) = peekCStringLen (s, fromIntegral n)

{#fun unsafe create as ^
 { `String', `Int' } -> `ClientPtr' id #}

withClientPtr :: Client -> (ClientPtr -> IO a) -> IO a
withClientPtr (Client fp) action = withForeignPtr fp action

data Client = Client (ForeignPtr ())

connect :: String -> Int -> IO Client
connect ip port = do
  fp <- create ip port >>= newForeignPtr p_destroy
  return $ Client fp

foreign import ccall "hyperclient.h &hyperclient_destroy"
  p_destroy :: FunPtr (Ptr () -> IO ())

{#fun unsafe add_space as ^
 { withClientPtr* `Client'
 , `String' } -> `ReturnCode' cToEnum #}

{#fun unsafe rm_space as ^
 { withClientPtr* `Client'
 , `String' } -> `ReturnCode' cToEnum #}

{#fun unsafe attribute_type as c_attributeType
 { withClientPtr* `Client'
 ,`String' -- space
 ,`String' -- field
 , alloca- `CInt' peek*  -- Only set if the datatype is "Garbage".
 } -> `Datatype' cToEnum #}


attributeType :: Client -> String -> String -> IO (Datatype, ReturnCode)
attributeType c s f = do
  (dt, rc) <- c_attributeType c s f
  let rc' = if dt == DatatypeGarbage
            then cToEnum rc
            else Success
  return (dt, rc')

{#fun unsafe loop
 { withClientPtr* `Client'
 , `Int'
 , alloca- `ReturnCode' peekEnum*
 } -> `Int64' #}

{#fun unsafe get as c_get
 { id `ClientPtr'
 , `String'
 , `String'&                   -- key and key_sz
 , id `Ptr CInt' id            -- status
 , id `Ptr AttributePtr' id    -- attrs
 , id `Ptr CULong' id          -- attrs_sz
 } -> `Int64' #}

get :: ClientPtr -> String -> String
    -> IO (Int64, Ptr CInt, Ptr AttributePtr, Ptr CULong)
get client space key = do
  status <- malloc
  attrs  <- malloc
  attrs_sz <- malloc
  c_get client space key status attrs attrs_sz

type CWriteFunction = ClientPtr -> CString -> CString -> CULong
                    -> AttributePtr -> CULong -> Ptr CInt -> IO CLong

type WriteFunction = ClientPtr -> B.ByteString -> B.ByteString
                   -> [Attribute] -> IO (CLong, Ptr CInt)

writeCall :: CWriteFunction -> WriteFunction
writeCall cfun client space key a =
  B.useAsCString    space $ \spacep       ->
  B.useAsCStringLen key   $ \(keyp, keyl) ->
  withAttributes    a     $ \(asp, asl)   -> do
    statp <- malloc
    rid   <- cfun client spacep keyp (fromIntegral keyl) asp (fromIntegral asl) statp
    return (rid, statp)


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
