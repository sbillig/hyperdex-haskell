{-# LANGUAGE ForeignFunctionInterface #-}

module Database.HyperDex.Internal where

import Data.Int
import Foreign.Ptr
import Foreign.C.String
import Foreign.C.Types
import Foreign.Marshal.Alloc (alloca)
import Foreign.Storable (peek)

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
 , alloca- `CInt' peek*
 } -> `Int64' #}

{#enum returncode as ReturnCode
 {underscoreToCase} deriving (Show, Eq) #}
{#enum hyperdatatype as Datatype
 {underscoreToCase}  with prefix = "hyper"
 deriving (Show, Eq) #}

