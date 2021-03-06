{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
{#pointer *attribute as AttributePtr -> Attribute#}
{#pointer *attribute_check as CheckPtr -> Check#}

#c
typedef struct hyperclient_attribute hyperclient_attribute;
typedef struct hyperclient_map_attribute hyperclient_map_attribute;
typedef struct hyperclient_attribute_check hyperclient_attribute_check;
typedef enum hyperclient_returncode hyperclient_returncode;
typedef enum hyperdatatype hyperdatatype;
typedef enum hyperpredicate hyperpredicate;
#endc

type SizeT = {#type size_t#}
type ReqId = {#type int64_t#}
type Space = ByteString
type Key   = ByteString

type RawStatusPtr = Ptr {#type returncode#}
type RawSizePtr   = Ptr SizeT
type RawAttrPtr   = Ptr (Ptr Attribute)

type StatusPtr = ForeignPtr {#type returncode#}
type SizePtr   = ForeignPtr SizeT
type AttrPtr   = ForeignPtr (Ptr Attribute)

data Request = WriteReq StatusPtr
             | ReadReq Bool StatusPtr AttrPtr SizePtr

data Attribute = Attribute
    { attrName  :: ByteString
    , attrValue :: Value
    } deriving (Eq, Show)

data Check = Check
    { checkAttr :: Attribute
    , checkPred :: Predicate
    } deriving (Eq, Show)

class XStorable a where
    xpeek       :: Ptr a -> IO a
    xpoke       :: Ptr a -> CString -> a -> IO ()
    structSize  :: a -> Int
    backingSize :: a -> Int

instance XStorable Attribute where
    xpeek p = do
      dtype <- cToEnum      <$> {#get attribute.datatype#} p :: IO Datatype
      size  <- fromIntegral <$> {#get attribute.value_sz#} p
      vptr  <- {#get attribute.value#} p
      value <- peekValue dtype size vptr
      name  <- B.packCString =<< {#get attribute.attr#} p
      return $ Attribute name value

    xpoke p buf (Attribute k v) = do
      pokeBS (castPtr buf) k
      pokeByteOff buf klen (0 :: Word8)
      pokeValue valp v
      {#set attribute.attr#}     p $ buf
      {#set attribute.value#}    p $ valp
      {#set attribute.datatype#} p $ enumToC $ datatype v
      {#set attribute.value_sz#} p $ fromIntegral size
          where
            size = sizeOfValue v
            klen = B.length k
            valp = plusPtr buf (klen + 1)

    structSize _ = {#sizeof attribute#}
    backingSize (Attribute k v) = B.length k + 1 + sizeOfValue v


instance XStorable Check where
    xpeek p = do
      attr <- xpeek (castPtr p)
      prd  <- cToEnum <$> {#get attribute_check.predicate#} p
      return $ Check attr prd

    xpoke p buf (Check attr prd) = do
      xpoke (castPtr p) buf attr
      {#set attribute_check.predicate#} p $ enumToC prd

    structSize _ = {#sizeof attribute_check#}
    backingSize (Check attr _) = backingSize attr


peekAndDestroy :: Int -> Ptr Attribute -> IO [Attribute]
peekAndDestroy ac as = do
  attrs <- xpeekArray ac as
  {#call destroy_attrs#} as (fromIntegral ac)
  return attrs

xwithArrayLen :: XStorable a => [a] -> (SizeT -> Ptr a -> IO b) -> IO b
xwithArrayLen xs action =
    allocaBytes (sbytes + bbytes) $ \p -> do
      foldM_ f (p, plusPtr p sbytes) xs
      action (fromIntegral $ length xs) p

    where
      sbytes = sum $ map structSize  xs
      bbytes = sum $ map backingSize xs
      f (sp, bp) a = xpoke sp bp a >>
                     return (plusPtr sp (structSize a),
                             plusPtr bp (backingSize a))

xpeekArray :: forall a . XStorable a => Int -> Ptr a -> IO [a]
xpeekArray count ptr | count <= 0 = return []
                     | otherwise  = f (count - 1) []
    where
      f 0 acc = do e <- xpeek ptr;                    return (e:acc)
      f n acc = do e <- xpeek (plusPtr ptr (n*size)); f (n-1) (e:acc)
      size = structSize (undefined :: a)


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
 {underscoreToCase} with prefix = "hyper"
 deriving (Show, Eq) #}

instance Storable Datatype where
    sizeOf _    = {#sizeof hyperdatatype#}
    alignment _ = 4
    peek p   = cToEnum <$> peek (castPtr p :: Ptr {#type hyperdatatype#})
    poke p x = poke (castPtr p) (enumToC x ::     {#type hyperdatatype#})

{#enum hyperpredicate as Predicate
 {underscoreToCase} with prefix = "hyperpredicate_"
 deriving (Show, Eq) #}

instance Storable Predicate where
    sizeOf _    = {#sizeof hyperpredicate#}
    alignment _ = 4
    peek p   = cToEnum <$> peek (castPtr p :: Ptr {#type hyperpredicate#})
    poke p x = poke (castPtr p) (enumToC x ::     {#type hyperpredicate#})




peekBytes :: Storable a => Int -> Ptr a -> IO [a]
peekBytes 0 _ = return []
peekBytes n p = do
  v <- peek p
  let sz = sizeOf v
  rest <- peekBytes (n - sz) (p `plusPtr` sz)
  return $ v : rest

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
