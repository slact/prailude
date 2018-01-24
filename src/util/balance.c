#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdbool.h>

#include "balance.h"
#include "uint256.h"

typedef uint8_t prailude_balance_unit_t;
#define BALANCE_RAW  0
#define BALANCE_XRB  1
#define BALANCE_KXRB 2
#define BALANCE_MXRB 3

static prailude_balance_unit_t balance_default_unit = BALANCE_MXRB;

typedef struct {
  uint128_t               raw;
  prailude_balance_unit_t unit;
  unsigned                lock:1;
} prailude_balance_t;

static void setunit(lua_State *L, prailude_balance_unit_t *balance_unit, const char *unit) {
  if(      strcmp(unit, "raw") == 0) {
    *balance_unit = BALANCE_RAW;
  }
  else if( strcmp(unit, "xrb") == 0) {
    *balance_unit = BALANCE_XRB;
  }
  else if( strcmp(unit, "Kxrb") == 0 ||
           strcmp(unit, "kxrb") == 0) {
    *balance_unit = BALANCE_KXRB;
  }
  else if( strcmp(unit, "Mxrb") == 0 || 
            strcmp(unit, "XRB") == 0) {
    *balance_unit = BALANCE_MXRB;
  }
  else {
    luaL_error(L, "invalid unit %s", unit);
    return;
  }
}

static void getunitstring(lua_State *L, prailude_balance_unit_t unit) {
  switch(unit) {
    case BALANCE_RAW:
      lua_pushliteral(L, "raw");
      break;
    case BALANCE_XRB:
      lua_pushliteral(L, "xrb");
      break;
    case BALANCE_KXRB:
      lua_pushliteral(L, "Kxrb");
      break;
    case BALANCE_MXRB:
      lua_pushliteral(L, "Mxrb");
      break;
  }
}

static prailude_balance_t *balance_create(lua_State *L) {
  prailude_balance_t  *balance = lua_newuserdata(L, sizeof(*balance));
  if(!balance) {
    luaL_error(L, "Out of memory, can't allocate account balance");
    return NULL;
  }
#if LUA_VERSION_NUM > 501  
  luaL_setmetatable(L, "prailude.balance");
#else
  luaL_getmetatable(L, "prailude.balance");
  lua_setmetatable(L, -2);
#endif
  balance->unit = balance_default_unit;
  balance->lock = 0;
  return balance;
}

static int lua_balance_unpack(lua_State *L) {
  size_t               sz;
  const char          *packed = luaL_checklstring(L, 1, &sz);
  prailude_balance_t  *balance;
  if(sz != 16) {
    luaL_error(L, "packed balance string must be 16 bytes long, but was %u", sz);
  }
  balance = balance_create(L);
  readu128BE((uint8_t *)packed, &balance->raw);
  balance->unit = balance_default_unit;
  return 1;
}

static int lua_balance_new(lua_State *L) {
  int                  nargs = lua_gettop(L);
  int                  argtype;
  bool                 ok;
  const char          *err = NULL;
  const char          *in;
  size_t               len;
  prailude_balance_t  *balance_in;
  prailude_balance_t  *balance = balance_create(L);
  if(nargs == 0) {
    //initialize to 0
    clear128(&balance->raw);
    return 1;
  }
  
  argtype = lua_type(L, 1);
  switch(argtype) {
    case LUA_TNIL:
      clear128(&balance->raw);
      break;
    case LUA_TSTRING:
      in = luaL_checklstring(L, 1, &len);
      ok = fromstring128(&balance->raw, in, len, &err);
      if(!ok) {
        return luaL_error(L, err);
      }
      break;
    case LUA_TUSERDATA:
      balance_in = luaL_checkudata(L, 1, "prailude.balance");
      copy128(&balance->raw, &balance_in->raw);
      balance->unit = balance_in->unit;
    default:
      luaL_error(L, "can only parse strings");
      break;
  }
  
  return 1;
}

static int lua_balance_default_unit(lua_State *L) {
  getunitstring(L, balance_default_unit);
  
  if(lua_gettop(L) > 0) {
    const char *unitstr = luaL_checkstring(L, 1);
    setunit(L, &balance_default_unit, unitstr);
  }
  
  return 1;
}

static void setfield_cfunction(lua_State *L, int tindex, const char *fname, lua_CFunction func) {
  lua_pushcfunction(L, func);
  if(tindex < 0) {
    tindex--;
  }
  lua_setfield(L, tindex, fname);
}

static int lua_balance_add(lua_State *L) {
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  prailude_balance_t *other = luaL_checkudata(L, 2, "prailude.balance");
  if(self->lock) {
    return luaL_error(L, "cannot modify locked balance by adding");
  }
  add128(&self->raw, &other->raw, &self->raw);
  lua_pushvalue(L, 1);
  return 1;
}
static int lua_balance_subtract(lua_State *L) {
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  prailude_balance_t *other = luaL_checkudata(L, 2, "prailude.balance");
  if(self->lock) {
    return luaL_error(L, "cannot modify locked balance by subtracting");
  }
  minus128(&self->raw, &other->raw, &self->raw);
  lua_pushvalue(L, 1);
  return 1;
}

static int lua_balance_pack(lua_State *L) {
  char out[16];
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  torawstring128(&self->raw, out);
  lua_pushlstring(L, out, 16);
  return 1;
}

static int lua_balance_lock(lua_State *L) {
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  self->lock = 1;
  return 1; //return self
}

static int lua_balance_tostring(lua_State *L) {
  char                out[40];
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  tostring128(&self->raw, 10, out, 40);
  lua_pushstring(L, out);
  return 1;
}

static int lua_balance_unit(lua_State *L) {
  prailude_balance_t  *balance = luaL_checkudata(L, 1, "prailude.balance");
  int                            nargs = lua_gettop(L);
  
  getunitstring(L, balance->unit);
  
  if(nargs > 1) {
    const char *str = lua_tostring(L, 2);
    setunit(L, &balance->unit, str);
  }
  
  return 1;
}

static int balance_operator_3(lua_State *L, void op(uint128_t *, uint128_t *, uint128_t *)) {
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  prailude_balance_t *other = luaL_checkudata(L, 2, "prailude.balance");
  prailude_balance_t *result = balance_create(L);
  result->unit = self->unit;
  op(&self->raw, &other->raw, &result->raw);
  return 1;
}


static int lua_balance_add_new(lua_State *L) {
  return balance_operator_3(L, add128);
}
static int lua_balance_subtract_new(lua_State *L) {
  return balance_operator_3(L, minus128);
}
static int lua_balance_multiply_new(lua_State *L) {
  return balance_operator_3(L, mul128);
}
static int lua_balance_divide_new(lua_State *L) {
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  prailude_balance_t *other = luaL_checkudata(L, 2, "prailude.balance");
  prailude_balance_t *result = balance_create(L);
  uint128_t           mod;
  result->unit = self->unit;
  divmod128(&self->raw, &other->raw, &result->raw, &mod);
  return 1;
}
static int lua_balance_modulo_new(lua_State *L) {
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  prailude_balance_t *other = luaL_checkudata(L, 2, "prailude.balance");
  prailude_balance_t *result = balance_create(L);
  uint128_t           div;
  result->unit = self->unit;
  divmod128(&self->raw, &other->raw, &div, &result->raw);
  return 1;
}
  
static int lua_balance_equal(lua_State *L) {
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  prailude_balance_t *other = luaL_checkudata(L, 2, "prailude.balance");
  lua_pushboolean(L, equal128(&self->raw, &other->raw));
  return 1;
}
static int lua_balance_lessthan(lua_State *L) {
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  prailude_balance_t *other = luaL_checkudata(L, 2, "prailude.balance");
  lua_pushboolean(L, !gte128(&self->raw, &other->raw));
  return 1;
}
static int lua_balance_lessthan_or_equal(lua_State *L) {
  prailude_balance_t *self = luaL_checkudata(L, 1, "prailude.balance");
  prailude_balance_t *other = luaL_checkudata(L, 2, "prailude.balance");
  lua_pushboolean(L, !gt128(&self->raw, &other->raw));
  return 1;
}

static int lua_balance_drop_zeroes_past_decimal(lua_State *L) {
  size_t      sz;
  const char *num = luaL_checklstring(L, 1, &sz);
  int         i, end = sz, nonzero_seen = 0;
  
  for(i = sz-1; i >= 0; i--) {
    if(!nonzero_seen && num[i]=='0') {
      end = i;
    }
    else if(num[i]=='.') {
      if(end == i+1) {
        //remove decimal too
        end--;
      }
      lua_pushlstring(L, num, end);
      break;
    }
    else {
      nonzero_seen = 1;
    }
  }
  return 1; //return input
}

static const struct luaL_Reg prailude_balance_functions[] = {
  //{ "initialize", lua_balance_initialize },
  { "new", lua_balance_new },
  { "unpack", lua_balance_unpack },
  { "default_unit", lua_balance_default_unit },
  { "numstring_drop_zeroes_past_decimal", lua_balance_drop_zeroes_past_decimal },
  
  { NULL, NULL }
};


int luaopen_prailude_util_balance_lowlevel(lua_State* L) {
  luaL_newmetatable(L, "prailude.balance");
  
  //__index
  lua_createtable(L, 0, 5);
  setfield_cfunction(L, -1, "lock",     lua_balance_lock);
  setfield_cfunction(L, -1, "add",      lua_balance_add);
  setfield_cfunction(L, -1, "subtract", lua_balance_subtract);
  setfield_cfunction(L, -1, "pack",     lua_balance_pack);
  setfield_cfunction(L, -1, "unit",     lua_balance_unit);
  
  lua_setfield(L, -2, "__index");
  
  setfield_cfunction(L, -1, "__add", lua_balance_add_new);
  setfield_cfunction(L, -1, "__sub", lua_balance_subtract_new);
  setfield_cfunction(L, -1, "__mul", lua_balance_multiply_new);
  setfield_cfunction(L, -1, "__div", lua_balance_divide_new);
  setfield_cfunction(L, -1, "__mod", lua_balance_modulo_new);
  setfield_cfunction(L, -1, "__tostring", lua_balance_tostring);
  
  setfield_cfunction(L, -1, "__eq", lua_balance_equal);
  setfield_cfunction(L, -1, "__lt", lua_balance_lessthan);
  setfield_cfunction(L, -1, "__le", lua_balance_lessthan_or_equal);
  
  
  lua_newtable(L);
  luaL_getmetatable(L, "prailude.balance");
  lua_setfield(L, -2, "mt");
#if LUA_VERSION_NUM > 501
  luaL_setfuncs(L,prailude_balance_functions,0);
#else
  luaL_register(L, NULL, prailude_balance_functions);
#endif
  
  return 1;
}
