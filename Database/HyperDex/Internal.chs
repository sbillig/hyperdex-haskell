{-# LANGUAGE ForeignFunctionInterface #-}

module Database.HyperDex.Internal where

import Foreign.Ptr
import Foreign.C.String
import Foreign.C.Types

cToEnum :: (Integral a, Enum b) => a -> b
cToEnum = toEnum . fromIntegral

#include <hyperclient.h>

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

{#enum returncode as ReturnCode
 {underscoreToCase} deriving (Show, Eq) #}

