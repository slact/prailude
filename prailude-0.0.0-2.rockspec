#!/usr/bin/lua
package = "prailude"
local _version = "0.0.0"
version = _version .. "-2"
source = {
  url="git://github.com/slact/prailude"
  --tag="v".._version
}
description = {
  summary = "RaiBlocks cryptocurrency node",
  detailed = "To be detailed",
  homepage = "https://github.com/slact/prailude",
  license = "MIT"
}
dependencies = {
  "lua >= 5.1, < 5.4",
  "luabc >= 1.1-1",
  "luv >= 1.9.1-1",
  "lua-cjson",
  "alt-getopt",
  "lualogging",
  "lua-leveldb",
  "lsqlite3",
  "mm"
}
build = {
  type = "builtin",
  modules = {
    ["prailude.util"] =          "src/util/prailude_util.lua",
    ["prailude.util.timer"] =    "src/util/timer.lua",
    ["prailude.util.lowlevel"] = {
      sources = {
        "src/util/prailude_util.c",
        "src/util/uint256.c",
      },
      incdirs = { "src" }
    },
    ["prailude.util.crypto"] =   {
      sources = {
        --blake2b
        "src/util/crypto/blake2/blake2b.c",
        
        --argon2
        "src/util/crypto/argon2/src/argon2.c",
        "src/util/crypto/argon2/src/core.c",
        "src/util/crypto/argon2/src/encoding.c",
        "src/util/crypto/argon2/src/ref.c",  
        "src/util/crypto/argon2/src/thread.c",  
        
        --monocypher for blake2b and edDSA-blake2b
        --"src/util/crypto/monocypher.c",
        
        --ed25519-donna for a speed comparison
        "src/util/crypto/ed25519-donna/ed25519.c",
        
        "src/util/prailude_crypto.c",
      },
      defines = {
        "ED25519_CUSTOMHASH",
        "ED25519_CUSTOMRNG",
        "ARGON2_NO_THREADS"
      },
      incdirs = {
        "src/util/crypto/argon2/include",
        "src/util/crypto/blake2",
        "src/util/crypto",
        "src"
      }
    },
    
    ["prailude.server"] =        "src/server.lua",
    ["prailude.control"] =       "src/control.lua",
    ["prailude.config"] =        "src/config.lua",
    ["prailude.bus"] =           "src/bus.lua",
    ["prailude.rainet"] =        "src/rainet.lua",
    ["prailude.log"] =           "src/log.lua",
    
    ["prailude.message.parser"] = {
      sources = {
        "src/models/message/prailude_parser.c",
        "src/util/net.c"
      },
      incdirs = { "src" }
    },
    ["prailude.message"] =  "src/models/message.lua",
    ["prailude.peer"] =     "src/models/peer.lua",
    ["prailude.block"] =    "src/models/block.lua",
    ["prailude.transaction"]="src/models/transaction.lua",
    ["prailude.account"] =   "src/models/account.lua",
    
    prailude = "src/prailude.lua"
  },
  install = {
    bin = {
      prailude = "bin/prailude.lua"
    }
  }
}
