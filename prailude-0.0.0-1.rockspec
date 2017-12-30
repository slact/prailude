#!/usr/bin/lua
package = "prailude"
local _version = "0.0.0"
version = _version .. "-1"
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
  "luazen >= 0.8-2",
  "luabc >= 1.1-1",
  "luv >= 1.9.1-1",
  "mediator_lua",
  "lua-cjson",
  "alt-getopt",
  "lualogging",
  "lua-leveldb",
  "mm"
}
build = {
  type = "builtin",
  modules = {
    ["prailude.util"] =          "src/util/prailude_util.lua",
    ["prailude.util.lowlevel"] = {
      sources = {
        "src/util/prailude_util.c",
        "src/util/uint256.c",
      },
      incdirs = { "src" }
    },
    ["prailude.server"] =        "src/server.lua",
    ["prailude.control"] =       "src/control.lua",
    ["prailude.config"] =        "src/config.lua",
    ["prailude.bus"] =           "src/bus.lua",
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
