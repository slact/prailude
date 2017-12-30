#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <signal.h>
#include <stdbool.h>
#include "prailude_crypto.h"

//#include "monocypher.h"
#include "argon2.h"
#include "blake2.h"

#include "ed25519-donna/ed25519.h"

static int lua_blake2b_hash(lua_State *L) {
  uint8_t             digest[64];
  
  size_t              len;
  const char         *str;
  
  int                 blake2b_hash_size;
  if(lua_gettop(L) > 1) {
    blake2b_hash_size = luaL_checknumber(L, 2);
  }
  else {
    //default to 64-byte blake2b 
    blake2b_hash_size = 64;
  }
  
  str = luaL_checklstring(L, 1, &len);
  blake2b(digest, (size_t )blake2b_hash_size, str, len, NULL, 0);
  
  lua_pushlstring(L, (const char*)digest, blake2b_hash_size);
  return 1;
}

#define kdf_full_work (64 * 1024)
#define kdf_test_work 8

static int lua_argon2d_raiblocks_hash(lua_State *L) {
  char        result[64];
  
  const char *password;
  size_t      password_len;
  
  const char *salt = NULL;
  size_t      salt_len = 0;
  
  unsigned    work = kdf_full_work;
  
  const char *net;
  
  password = luaL_checklstring(L, 1, &password_len);
  if(lua_gettop(L) > 1) {
    //salt
    if(!lua_isnil(L, 2)) {
      salt = luaL_checklstring(L, 2, &salt_len);
    }
  }
  if(lua_gettop(L) > 2) {
    //testnet password hashing
    net = luaL_checkstring(L, 3);
    if(strcmp(net, "test") == 0) {
      work = kdf_test_work;
    }
  }
  
  argon2_hash(1, work, 1, password, password_len, salt, salt_len, result, 64, NULL, 0, Argon2_d, 0x10);

  lua_pushlstring(L, result, 64);
  return 1;
  
}


//static int lua_edDSA_blake2b_get_public_key(lua_State *L) {
//  
//}

static const struct luaL_Reg prailude_crypto_functions[] = {
  //{ "blake2b_init", lua_blake2b_init },
  //{ "blake2b_update", lua_blake2b_update },
  //{ "blake2b_finalize", lua_blake2b_finalize },
  { "blake2b_hash", lua_blake2b_hash }, //(input_str, hash_bytes = 64)
  
  //{ "edDSA_blake2b_get_public_key", lua_edDSA_blake2b_get_public_key },
  //{ "edDSA_blake2b_sign",           lua_edDSA_blake2b_sign },
  //{ "edDSA_blake2b_verify",         lua_edDSA_blake2b_verify },
  
  {"argon2d_raiblocks_hash",        lua_argon2d_raiblocks_hash },
  
  { NULL, NULL }
};

int luaopen_prailude_util_lowlevel(lua_State* lua) {
  lua_newtable(lua);
#if LUA_VERSION_NUM > 501
  luaL_setfuncs(lua,prailude_crypto_functions,0);
#else
  luaL_register(lua, NULL, prailude_crypto_functions);
#endif
  return 1;
}
