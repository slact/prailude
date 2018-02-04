#!/usr/bin/lua
package = "prailude"
local _version = "0.0.0"
version = _version .. "-4"
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
  --"lightningmdb", --nope
  "lsqlite3",
  "mm"
}
build = {
  type = "builtin",
  modules = {
    ["prailude.util"] =          "src/util/util.lua",
    ["prailude.util.timer"] =    "src/util/timer.lua",
    ["prailude.util.coroutine"] ="src/util/coroutine.lua",
    ["prailude.util.lowlevel"] = {
      sources = {
        "src/util/util.c",
        "src/util/uint256.c",
      },
      incdirs = { "src" }
    },
    ["prailude.util.balance"] = "src/util/balance.lua",
    ["prailude.util.balance.lowlevel"] = {
      sources = { "src/util/balance.c", "src/util/uint256.c" },
      incdirs = { "src" }
    },
    ["prailude.util.crypto"] = {
      sources = {
        --blake2b
        "src/util/crypto/blake2/blake2b.c",
        
        --argon2
        "src/util/crypto/argon2/src/argon2.c",
        "src/util/crypto/argon2/src/core.c",
        "src/util/crypto/argon2/src/encoding.c",
        "src/util/crypto/argon2/src/ref.c",  
        "src/util/crypto/argon2/src/thread.c",  
        
        --ed25519-donna
        "src/util/crypto/ed25519-donna/ed25519.c",
        
        "src/util/crypto.c",
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
    ["prailude.util.parser"] = {
      sources = {
        "src/util/parser.c",
        "src/util/net.c"
      },
      incdirs = { "src" }
    },
    
    ["prailude.server"] =        "src/server.lua",
    ["prailude.control"] =       "src/control.lua",
    ["prailude.config"] =        "src/config.lua",
    ["prailude.bus"] =           "src/bus.lua",
    ["prailude.rainet"] =        "src/rainet.lua",
    ["prailude.log"] =           "src/log.lua",
    
    --pluggable db interface
    ["prailude.db"] =            "src/db.lua",
    --nilDB, an erroring-out placeholder
    ["prailude.db.nil"] =        "src/db/nil.lua",    
    --sqlite
    ["prailude.db.sqlite"] =        "src/db/sqlite.lua",
    ["prailude.db.sqlite.peer"] =   "src/db/sqlite/peerdb.lua",
    ["prailude.db.sqlite.block"] =  "src/db/sqlite/blockdb.lua",
    ["prailude.db.sqlite.frontier"]="src/db/sqlite/frontierdb.lua",
    ["prailude.db.sqlite.account"] ="src/db/sqlite/accountdb.lua",
    ["prailude.db.sqlite.kv"] =     "src/db/sqlite/kvdb.lua", --key/value store
    
    ["prailude.message"] =    "src/models/message.lua",
    ["prailude.peer"] =       "src/models/peer.lua",
    ["prailude.TCPsession"] = "src/models/TCPSession.lua",
    ["prailude.block"] =      "src/models/block.lua",
    ["prailude.transaction"]= "src/models/transaction.lua",
    ["prailude.account"] =    "src/models/account.lua",
    ["prailude.frontier"] =   "src/models/frontier.lua",
    
    prailude = "src/prailude.lua"
  },
  install = {
    bin = {
      prailude = "bin/prailude.lua"
    }
  }
}
