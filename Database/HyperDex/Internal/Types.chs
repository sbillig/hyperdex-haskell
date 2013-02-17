{-# LANGUAGE ForeignFunctionInterface #-}

module Database.HyperDex.Internal.Types where

import Data.Int
import Data.Word
import Foreign
import Foreign.C.String
import Foreign.C.Types
import Control.Applicative
import Control.Monad

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B

#include <hyperclient.h>
#include <hyperdex.h>

{#context lib="hyperclient" prefix="hyperclient"#}
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

peekAndDestroy :: Int -> AttributePtr -> IO [Attribute]
peekAndDestroy ac as = do
  attrs <- peekMany peekAttr {#sizeof attribute#} ac as
  {#call destroy_attrs#} as (fromIntegral ac)
  return attrs

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
