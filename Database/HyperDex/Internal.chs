{-# LANGUAGE ForeignFunctionInterface #-}

module Database.HyperDex.Internal where

import Foreign.Ptr
import Foreign.ForeignPtr
import Foreign.C.String
import Foreign.C.Types

enumToEnum = toEnum . fromIntegral

#include <hyperclient.h>

{#context lib="hyperclient" prefix="hyperclient"#}

{#pointer *hyperclient as Client#}

{#fun unsafe create as ^
 { `String', `Int' } -> `Client' id #}

{#fun unsafe destroy as ^
 { id `Client' } -> `()' #}

{#fun unsafe add_space as ^
 { id `Client', `String' } -> `ReturnCode' enumToEnum #}

{#fun unsafe rm_space as ^
 { id `Client', `String' } -> `ReturnCode' enumToEnum #}

{#enum returncode as ReturnCode
 {underscoreToCase} deriving (Show, Eq) #}

