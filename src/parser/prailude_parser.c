#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <signal.h>
#include <lua.h>
#include <lauxlib.h>
#include <stdbool.h>

#include "prailude_parser.h"
#include "util/uint256.h"

static int prailude_parse_udp_message(lua_State *L) {
  return 0;
}

static const struct luaL_Reg prailude_parser_functions[] = {
  { "parse_udp_message", prailude_parse_udp_message },
  { NULL, NULL }
};

int luaopen_prailude_parser_lowlevel(lua_State* lua) {
  lua_newtable(lua);
#if LUA_VERSION_NUM > 501
  luaL_setfuncs(lua,prailude_parser_functions,0);
#else
  luaL_register(lua, NULL, prailude_parser_functions);
#endif
  return 1;
}
