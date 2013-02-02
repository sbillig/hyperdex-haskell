{-# LANGUAGE ForeignFunctionInterface #-}

module Database.HyperDex.Internal where

import Foreign.Ptr
import Foreign.C.String
import Foreign.C.Types
import Foreign.Marshal.Alloc (alloca)
import Foreign.Storable (peek)
import Control.Applicative ((<$>))

cToEnum :: (Integral a, Enum b) => a -> b
cToEnum = toEnum . fromIntegral

#include <hyperclient.h>
#include <hyperdex.h>

{#context lib="hyperclient" prefix="hyperclient"#}

{#pointer *hyperclient as Client#}

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
 -- The status is only set if the datatype is Garbage,
 -- so we'll only "peek" the value when that's the case. See attributeType below.
 , alloca- `Ptr CInt' id
 } -> `Hyperdatatype' cToEnum #}


attributeType :: Client -> String -> String -> IO (Hyperdatatype, ReturnCode)
attributeType c s f = do
  (dt, rc) <- c_attributeType c s f
  rc' <- if dt == HyperdatatypeGarbage
         then cToEnum <$> peek rc
         else return Success
  return (dt, rc')

{#enum returncode as ReturnCode
 {underscoreToCase} deriving (Show, Eq) #}
{#enum hyperdatatype as ^
 {underscoreToCase} deriving (Show, Eq) #}

