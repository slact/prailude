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
  "lua-cjson",
  "alt-getopt",
  "lualogging"
}
build = {
  type = "builtin",
  modules = {
    ["prailude.util.lowlevel"] = {
      sources = {
        "src/util/prailude_util.c",
        "src/util/uint256.c"
      },
      defines = {("DIST_VERSION=\"%s\""):format(_version)},
      variables={CFLAGS="-ggdb"}
    },
    ["prailude.util"] = "src/util/prailude_util.lua",
    ["prailude.server"] = "src/server.lua",
    prailude = "src/prailude.lua"
  },
  install = {
    bin = {
      prailude = "bin/prailude.lua"
    }
  }
}
