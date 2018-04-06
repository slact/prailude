#!/usr/bin/lua
package = "prailude"
local _version = "0.0.0"
version = _version .. "-4"
source = {
  url="git://github.com/slact/prailude"
  --tag="v".._version
}
description = {
  summary = "Nano cryptocurrency node",
  detailed = "To be detailed",
  homepage = "https://github.com/slact/prailude",
  license = "MIT"
}
dependencies = {
  "lua >= 5.1, < 5.4",
  "luabc >= 1.1-1",
  "luv-updated",
  "lua-cjson",
  "alt-getopt",
  "lualogging",
  --"lightningmdb", --nope
  "lsqlite3",
  "mm"
}
build = {
  type = "builtin",
  external_dependencies = {
    ZLIB = {
      header = "lzib.h",
      library = "z",
    },
    PTHREAD = {
      header = "pthread.h"
    },
    BZIP = {
      header = "bzlib.h",
      library = "bz2"
    }
  },
  modules = {
    ["prailude.util"] =          "src/util/util.lua",
    ["prailude.util.timer"] =    "src/util/timer.lua",
    ["prailude.util.coroutine"] ="src/util/coroutine.lua",
    ["prailude.util.tokyocabinet"] = {
      sources = {
        --tokyocabinet
        "src/util/tokyocabinet/md5.c",
        "src/util/tokyocabinet/myconf.c",
        "src/util/tokyocabinet/tcadb.c",
        "src/util/tokyocabinet/tcbdb.c",
        "src/util/tokyocabinet/tcfdb.c",
        "src/util/tokyocabinet/tchdb.c",
        "src/util/tokyocabinet/tctdb.c",
        "src/util/tokyocabinet/tcutil.c",
        -- lua binding
        "src/util/tokyocabinet/tokyocabinet.c"
      },
      incdirs = { "src/util/tokyocabinet"},
      defines = {
        "NDEBUG"
      },
      libraries = {"z", "bz2", "pthread"}
    },
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
    ["prailude.nanonet"] =        "src/nanonet.lua",
    ["prailude.log"] =           "src/log.lua",
    
    --pluggable db interface
    ["prailude.db"] =            "src/db.lua",
    --nilDB, an erroring-out placeholder
    ["prailude.db.nil"] =        "src/db/nil.lua",    
    --sqlite
    ["prailude.db.sqlite"] =        "src/db/sqlite.lua",
    ["prailude.db.sqlite.peer"] =   "src/db/sqlite/peerdb.lua",
    ["prailude.db.sqlite.block"] =  "src/db/sqlite/blockdb.lua",
    ["prailude.db.sqlite.blockwalker"] =  "src/db/sqlite/blockwalkerdb.lua",
    ["prailude.db.sqlite.frontier"]="src/db/sqlite/frontierdb.lua",
    ["prailude.db.sqlite.account"] ="src/db/sqlite/accountdb.lua",
    ["prailude.db.sqlite.kv"] =     "src/db/sqlite/kvdb.lua", --key/value store

    ["prailude.vote"] =       "src/models/vote.lua",
    ["prailude.message"] =    "src/models/message.lua",
    ["prailude.peer"] =       "src/models/peer.lua",
    ["prailude.TCPsession"] = "src/models/TCPSession.lua",
    ["prailude.block"] =      "src/models/block.lua",
    ["prailude.blockwalker"] =      "src/models/blockwalker.lua",
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
