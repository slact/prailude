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


//http://burtleburtle.net/bob/rand/smallprng.html
typedef unsigned long int  u4;
typedef struct ranctx { u4 a; u4 b; u4 c; u4 d; } ranctx;
#define rot(x,k) (((x)<<(k))|((x)>>(32-(k))))
u4 ranval( ranctx *x ) {
    u4 e = x->a - rot(x->b, 27);
    x->a = x->b ^ rot(x->c, 17);
    x->b = x->c + x->d;
    x->c = x->d + e;
    x->d = e + x->a;
    return x->d;
}
void raninit( ranctx *x, u4 seed ) {
    u4 i;
    x->a = 0xf1ea5eed, x->b = x->c = x->d = seed;
    for (i=0; i<20; ++i) {
        (void)ranval(x);
    }
}
static ranctx rng_ctx;

#include "ed25519-donna/ed25519.h"
#include <ed25519-donna/ed25519-hash-custom.h>

#define RND_SZ sizeof(u4)
void ed25519_randombytes_unsafe (void * out, size_t outlen) {
  u4      rnd;
  int     i, max = outlen/RND_SZ;
  
  for(i=0; i < max; i++) {
    *(u4 *)out = ranval(&rng_ctx);
    out+=RND_SZ;
  }
  i=outlen % RND_SZ;
  if(i > 0) {
    rnd = ranval(&rng_ctx);
    memcpy(out, &rnd, 1);
  }
}
void ed25519_hash_init(ed25519_hash_context * ctx) {
    blake2b_state  *state = malloc(sizeof(*state));
    ctx->blake2 = state;
    //WHAT IF IT FAILS?!?!!
	blake2b_init(state, 64);
}

void ed25519_hash_update (ed25519_hash_context * ctx, uint8_t const * in, size_t inlen) {
	blake2b_update ((blake2b_state *) ctx->blake2, in, inlen);
}

void ed25519_hash_final (ed25519_hash_context * ctx, uint8_t * out) {
	blake2b_final ((blake2b_state *) (ctx->blake2), out, 64);
	free(ctx->blake2);
}

void ed25519_hash (uint8_t * out, uint8_t const * in, size_t inlen) {
	ed25519_hash_context ctx;
	ed25519_hash_init (&ctx);
	ed25519_hash_update (&ctx, in, inlen);
	ed25519_hash_final (&ctx, out);
}


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

static int lua_blake2b_init(lua_State *L) {
  blake2b_state     *ctx;
  lua_Number         n;
  
  luaL_newmetatable(L, "blake2b_ctx");
  ctx = lua_newuserdata(L, sizeof(*ctx));
  luaL_setmetatable(L, "blake2b_ctx");
  
  if(ctx == NULL) {
    return luaL_error(L, "can't init blaked2b context: no memory");
  }
  if(lua_gettop(L) > 1) {
    n = luaL_checknumber(L, 1);
  }
  else {
    n = 64;
  }
  blake2b_init(ctx, n);
  return 1;
}
static int lua_blake2b_update(lua_State *L) {
  blake2b_state     *ctx = luaL_checkudata(L, 1, "blake2b_ctx");
  size_t             datalen;
  const char        *data = luaL_checklstring(L, 2, &datalen);
  
  blake2b_update(ctx, data, datalen);
  lua_pushvalue(L, 1);
  return 1;
}
static int lua_blake2b_finalize(lua_State *L) {
  blake2b_state     *ctx = luaL_checkudata(L, 1, "blake2b_ctx");
  char               out[64];
  blake2b_final(ctx, out, 64);
  lua_pushlstring(L, out, ctx->outlen);
  return 1;
}



static int lua_edDSA_blake2b_get_public_key(lua_State *L) {
  const char *privkey;
  size_t      len;
  char        pubkey[32];
  
  privkey = luaL_checklstring(L, 1, &len);
  if(len != 32) {
    return luaL_error(L, "input private key must be 32 bytes long");
  }
  ed25519_publickey((unsigned char *)privkey, (unsigned char *)pubkey);
  
  lua_pushlstring(L, pubkey, 32);
  return 1;
}

static int lua_edDSA_blake2b_sign(lua_State *L) { //(message, privkey, pubkey)
  const char *privkey;
  const char *pubkey;
  size_t      len;
  const char *msg;
  size_t      msglen;
  const char *signature[64];
  
  msg = luaL_checklstring(L, 1, &msglen);
  
  privkey = luaL_checklstring(L, 2, &len);
  if(len != 32) {
    return luaL_error(L, "private key must be 32 bytes long");
  }
  
  pubkey = luaL_checklstring(L, 3, &len);
  if(len != 32) {
    return luaL_error(L, "public key must be 32 bytes long");
  }
  
  ed25519_sign((unsigned char *)msg, msglen, (unsigned char *)privkey, (unsigned char *)pubkey, (unsigned char *)signature);
  
  lua_pushlstring(L, (const char *)signature, 64);
  return 1;
}

static int lua_edDSA_blake2b_verify(lua_State *L) { //message, signature, pubkey
  const char     *pubkey;
  const char     *signature;
  const char     *msg;
  size_t          msglen, len;
  int             valid;
  
  msg = luaL_checklstring(L, 1, &msglen);
  
  signature = luaL_checklstring(L, 2, &len);
  if(len != 64) {
    return luaL_error(L, "signature must be 64 bytes long");
  }
  
  pubkey = luaL_checklstring(L, 3, &len);
  if(len != 32) {
    return luaL_error(L, "public key must be 32 bytes long");
  }
  
  valid = 0 == ed25519_sign_open((unsigned char *)msg, msglen, (unsigned char *)pubkey, (unsigned char *)signature);
  lua_pushboolean(L, valid);
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
  { "blake2b_init", lua_blake2b_init },
  { "blake2b_update", lua_blake2b_update },
  { "blake2b_finalize", lua_blake2b_finalize },
  { "blake2b_hash", lua_blake2b_hash }, //(input_str, hash_bytes = 64)
  
  { "edDSA_blake2b_get_public_key", lua_edDSA_blake2b_get_public_key },
  { "edDSA_blake2b_sign",           lua_edDSA_blake2b_sign },
  { "edDSA_blake2b_verify",         lua_edDSA_blake2b_verify },
  
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
  
  //initialize crappy PRNG for bulk ed25519 verification
  raninit(&rng_ctx, 1);
  
  return 1;
}
