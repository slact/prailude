#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <signal.h>
#include <stdbool.h>
#include "crypto.h"

//#include "monocypher.h"
#include "argon2.h"
#include "blake2.h"

#if LUA_VERSION_NUM <= 501
static void luaL_setmetatable (lua_State *L, const char *tname) {
  luaL_checkstack(L, 1, "not enough stack slots");
  luaL_getmetatable(L, tname);
  lua_setmetatable(L, -2);
}
#endif


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
  blake2b_state       state;
  size_t              len;
  const char         *str;
  int                 i, nargs = lua_gettop(L);
  int                 blake2b_hash_size = 32;
  int                 argtype;
  if(nargs == 0) {
    return luaL_error(L, "no arguments?... I disagree!!");
  }
  argtype = lua_type(L, nargs);
  if(argtype == LUA_TNUMBER) {
    blake2b_hash_size = luaL_checknumber(L, nargs);
    nargs--;
    if(blake2b_hash_size < 3 || blake2b_hash_size > 64) {
      luaL_error(L, "invalid hash size %i", blake2b_hash_size);
    }
  }
  
  blake2b_init(&state, blake2b_hash_size);
  for(i=1; i<=nargs; i++) {
    str = luaL_checklstring(L, i, &len);
    blake2b_update(&state, str, len);
  }
  blake2b_final(&state, digest, blake2b_hash_size);
  
  lua_pushlstring(L, (const char*)digest, blake2b_hash_size);
  return 1;
}

static uint64_t const publish_test_threshold = 0xff00000000000000;
static uint64_t const publish_full_threshold = 0xffffffc000000000;

static uint64_t xor_state[1];
static inline uint64_t xorshift64star(void) {
  uint64_t x = xor_state[0];	/* The state must be seeded with a nonzero value. */
  x ^= x >> 12; // a
  x ^= x << 25; // b
  x ^= x >> 27; // c
  xor_state[0] = x;
  return x * 0x2545F4914F6CDD1D;
}

static int raiblocks_internal_work_verify(const char *block_hashable, size_t hashable_len, const char *work, uint64_t threshold) {
  uint64_t            result;
  
  blake2b_state       state;
  
  blake2b_init(&state, sizeof(result));
  blake2b_update(&state, work, 8);
  blake2b_update(&state, block_hashable, hashable_len);
  blake2b_final(&state, &result, sizeof(result));
  /*
  int i;
  unsigned char *in;
  in=(unsigned char *)work;
  printf("\nwork\n");
  for(i=0; i<8; i++) {
    printf("%02x", in[i]);
  }
  printf("\nhashable\n");
  in=(unsigned char *)block_hashable;
  for(i=0; i<hashable_len ; i++) {
    printf("%02x", in[i]);
  }
  printf("\nresult\n");
  in=(unsigned char *)&result;
  for(i=0; i<8 ; i++) {
    printf("%02x", in[i]);
  }
  printf("\nEND\n");
  */
  return result >= threshold;
}

static int raiblocks_generate_proof_of_work(lua_State *L, uint64_t threshold) {
  size_t        hashable_len;
  uint64_t      pow;
  const char   *block_hashable = luaL_checklstring(L, 1, &hashable_len);
  do {
    pow = xorshift64star();
  } while(!raiblocks_internal_work_verify(block_hashable, hashable_len, (const char *)&pow, threshold));
  
  lua_pushlstring(L, (const char *)&pow, 8);
  return 1;
}

static int raiblocks_work_verify(lua_State *L, uint64_t threshold) {
  size_t              len;
  const char         *work, *block_hashable;
  work = luaL_checklstring(L, 2, &len);
  if(len != 8) {
    return luaL_error(L, "wrong length work value");
  }
  block_hashable = luaL_checklstring(L, 1, &len);
  
  
  return raiblocks_internal_work_verify(block_hashable, len, work, threshold);
}

static int lua_raiblocks_work_verify_test(lua_State *L) {
  lua_pushboolean(L, raiblocks_work_verify(L, publish_test_threshold));
  return 1;
}
static int lua_raiblocks_work_verify_full(lua_State *L) {
  lua_pushboolean(L, raiblocks_work_verify(L, publish_full_threshold));
  return 1;
}

static int lua_raiblocks_generate_proof_of_work_full(lua_State *L) {
  raiblocks_generate_proof_of_work(L, publish_full_threshold);
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

#define ED25519_MAX_BATCH_SIZE 64

static int blake2b_batch_verify(lua_State *L, int batchsize, int offset) {
  const unsigned char     *msg[ED25519_MAX_BATCH_SIZE];
  size_t                   msglen[ED25519_MAX_BATCH_SIZE];
  const unsigned char     *pubkey[ED25519_MAX_BATCH_SIZE];
  const unsigned char     *signature[ED25519_MAX_BATCH_SIZE];
  int                      valid[ED25519_MAX_BATCH_SIZE];
  int                      all_valid, i;
  size_t                   sz;
  if(batchsize > ED25519_MAX_BATCH_SIZE) {
    return luaL_error(L, "ed25519_batch_verify batch size cannot exceed 64");
  }
  
  for(i=0; i < batchsize; i++) {
    lua_rawgeti(L, -1, i+offset+1);
    
    //message
    lua_rawgeti(L, -1, 1);
    msg[i] = (const unsigned char *)luaL_checklstring(L, -1, &msglen[i]);
    lua_pop(L, 1);
    
    //sig
    lua_rawgeti(L, -1, 2);
    signature[i] = (const unsigned char *)luaL_checklstring(L, -1, &sz);
    if(sz != 64)  {
      return luaL_error(L, "sig must be length 64, instead it's %i", sz);
    }
    lua_pop(L, 1);
    
    //pubkey
    lua_rawgeti(L, -1, 3);
    pubkey[i] = (const unsigned char *)luaL_checklstring(L, -1, &sz);
    if(sz != 32)  {
      return luaL_error(L, "pubkey must be length 32, instead it's %i", sz);
    }
    lua_pop(L, 1);
    
    lua_pop(L, 1);
  }
  
  all_valid = 0 == ed25519_sign_open_batch(msg, msglen, pubkey, signature, batchsize, valid);
  if(all_valid) {
    return 1;
  }
  else {
    for(i=0; i< batchsize; i++) {
      lua_rawgeti(L, -1, i+offset+1);
      lua_pushliteral(L, "valid");
      lua_pushboolean(L, valid[i]);
      lua_rawset(L, -3);
      lua_pop(L, 1);
    }
    return 0;
  }
}

static int lua_edDSA_blake2b_batch_verify(lua_State *L) { //batch_len, {{message, signature, pubkey}, ...}
  lua_Number               batchsize;
  int                      all_valid = 1, offset, batchleft;
  batchsize = luaL_checknumber(L, 1);
  for(offset=0; offset < batchsize; offset += ED25519_MAX_BATCH_SIZE) {
    batchleft = batchsize - offset;
    all_valid = all_valid && blake2b_batch_verify(L, batchleft > ED25519_MAX_BATCH_SIZE ? 64 : batchleft, offset);
  }
  lua_pushboolean(L, all_valid);
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
  { "blake2b_hash", lua_blake2b_hash }, //(input_str or table, hash_bytes = 32)
  
  { "raiblocks_verify_test_work", lua_raiblocks_work_verify_test },
  { "raiblocks_verify_work", lua_raiblocks_work_verify_full },
  { "raiblocks_generate_work", lua_raiblocks_generate_proof_of_work_full },
  
  { "edDSA_blake2b_get_public_key", lua_edDSA_blake2b_get_public_key },
  { "edDSA_blake2b_sign",           lua_edDSA_blake2b_sign },
  { "edDSA_blake2b_verify",         lua_edDSA_blake2b_verify },
  { "edDSA_blake2b_batch_verify",   lua_edDSA_blake2b_batch_verify },
  
  {"argon2d_raiblocks_hash",        lua_argon2d_raiblocks_hash },
  
  { NULL, NULL }
};

int luaopen_prailude_util_crypto(lua_State* lua) {
  lua_newtable(lua);
#if LUA_VERSION_NUM > 501
  luaL_setfuncs(lua,prailude_crypto_functions,0);
#else
  luaL_register(lua, NULL, prailude_crypto_functions);
#endif
  
  xor_state[0] = 999234;
  //initialize crappy PRNG for bulk ed25519 verification
  raninit(&rng_ctx, 1); // WTF KIND OF SEED IS THAT?!
  
  return 1;
}
