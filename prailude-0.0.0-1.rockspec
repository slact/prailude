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
  "luabc >= 1.1-1"
}
build = {
  type = "builtin",
  modules = {
    ["prailude.util"] = {
      sources = {
        "prailude_util.c",
        "util/uint256.c"
      },
      defines = {("DIST_VERSION=\"%s\""):format(_version)},
      variables={CFLAGS="-ggdb"}
    },
    prailude = "prailude.lua"
  }
}
