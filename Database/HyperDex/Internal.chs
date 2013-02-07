{-# LANGUAGE ForeignFunctionInterface #-}

module Database.HyperDex.Internal where

import Data.Int

import Foreign.Ptr
import Foreign.C.String
import Foreign.C.Types
import Foreign.Marshal.Alloc
import Foreign.Storable
import Control.Applicative

cToEnum :: (Integral a, Enum b) => a -> b
cToEnum = toEnum . fromIntegral

peekEnum :: (Integral a, Storable a, Enum b) => Ptr a -> IO b
peekEnum p = cToEnum <$> peek p

withCStringLenIntConv :: Num n => String -> ((CString, n) -> IO a) -> IO a
withCStringLenIntConv s f    = withCStringLen s $ \(p, n) -> f (p, fromIntegral n)

peekCStringLenIntConv :: Integral n => (CString, n) -> IO String
peekCStringLenIntConv (s, n) = peekCStringLen (s, fromIntegral n)


#include <hyperclient.h>
#include <hyperdex.h>

{#context lib="hyperclient" prefix="hyperclient"#}

{#pointer *hyperclient as Client#}
{#pointer *attribute -> Attribute nocode #}

data Attribute = Attribute
    { attrName :: CString
    , attrValue :: CString
    , attrSize :: CULong
    , attrType :: Datatype
    }

instance Storable Attribute where
    sizeOf _    = {#sizeof attribute#}
    alignment _ = 4
    peek p = do
      Attribute <$> ({#get attribute.attr #} p)
                <*> ({#get attribute.value #} p)
                <*> ({#get attribute.value_sz #} p)
                <*> (cToEnum <$> {#get attribute.datatype #} p)
    poke p x = do
      {#set attribute.attr#}  p (attrName x)
      {#set attribute.value#} p (attrValue x)
      {#set attribute.value_sz#} p (attrSize x)
      {#set attribute.datatype#} p (fromIntegral $ fromEnum $ attrType x)

#c
typedef struct hyperclient_attribute hyperclient_attribute;
typedef struct hyperclient_map_attribute hyperclient_map_attribute;
typedef struct hyperclient_attribute_check hyperclient_attribute_check;
#endc

{#fun unsafe create as ^
 { `String', `Int' } -> `Client' id #}

{#fun unsafe destroy as ^
 { id `Client' } -> `()' id #}

{#fun unsafe add_space as ^
 { id `Client', `String' } -> `ReturnCode' cToEnum #}

{#fun unsafe rm_space as ^
 { id `Client', `String' } -> `ReturnCode' cToEnum #}

{#fun unsafe attribute_type as c_attributeType
 { id `Client'
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
 { id `Client'
 , `Int'
 , alloca- `ReturnCode' peekEnum*
 } -> `Int64' #}

{#fun unsafe get as c_get
 { id `Client'
 , `String'
 , `String'&                   -- key and key_sz
 , id `Ptr CInt' id            -- status
 , id `Ptr (Ptr Attribute)' id --
 , id `Ptr CULong' id          -- attrs_sz
 } -> `Int64' #}

get :: Client -> String -> String
    -> IO (Int64, Ptr CInt, Ptr (Ptr Attribute), Ptr CULong)
get client space key = do
  status <- malloc
  attrs  <- malloc
  attrs_sz <- malloc
  c_get client space key status attrs attrs_sz

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
