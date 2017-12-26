#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <signal.h>
#include <lua.h>
#include <lauxlib.h>
#include <stdbool.h>

#include "prailude_parser.h"
#include "util/prailude_util.h"

#include <netinet/in.h>
#include <arpa/inet.h>

#define RETURN_FAIL(Lua, errmsg) \
  lua_pushnil(Lua); \
  lua_pushstring(L, errmsg); \
  return 2

  
#define lua_rawgetfield(Lua, tindex, string_literal) \
  lua_pushstring(Lua, string_literal);  \
  lua_rawget(Lua, tindex)
  
#define lua_rawsetfield_string(Lua, tindex, field, str, strlen) \
  lua_pushstring(Lua, field);        \
  lua_pushlstring(Lua, str, strlen); \
  lua_rawset(Lua, tindex)

#define lua_rawsetfield_number(Lua, tindex, field, num) \
  lua_pushliteral(Lua, field);     \
  lua_pushnumber(Lua, num);        \
  lua_rawset(Lua, tindex)

  
static size_t block_decode_unpack(rai_block_type_t blocktype, lua_State *L, const char *buf, size_t buflen, const char **err);
static size_t block_pack_encode  (rai_block_type_t blocktype, lua_State *L, char *buf, size_t buflen);
  
static size_t lua_rawsetfield_string_scanbuf(lua_State *L, int tindex, const char *field, const char *buf, size_t buflen) {
  lua_rawsetfield_string(L, tindex, field, buf, buflen);
  return buflen;
}
  
static const char*lua_tblfield_to_fixedsize_string(lua_State *L, int tblindex, const char *fieldname, size_t expected_strlen) {
  const char    *str;
  size_t         len;
  lua_rawgetfield(L, tblindex, fieldname);
  str = lua_tolstring(L, -1, &len);
  if(str == NULL) {
    luaL_error(L, "expected table field '%s' to be a string", fieldname);
  }
  if(expected_strlen != len) {
    luaL_error(L, "invalid length of table field value '%s'", fieldname);
  }
  lua_remove(L, -1);
  return str;
}

static size_t lua_table_field_fixedsize_string_encode(lua_State *L, int tblindex, const char *fieldname, char *dst_buf, size_t expected_strlen) {
  const char *str = lua_tblfield_to_fixedsize_string(L, tblindex, fieldname, expected_strlen);
  memcpy(dst_buf, str, expected_strlen);
  return expected_strlen;
}
  
static int prailude_parse_udp_message(lua_State *L) {
  return 0;
}

static size_t message_header_pack(lua_State *L, int message_table_index, rai_msg_header_t *header, const char **err) {
  const char       *str;
  int               isnum;
  
  lua_rawgetfield(L, message_table_index, "net");
  str = lua_tostring(L, -1);
  if(!str) { //default
    str="mainnet";
  }
  switch(str[0]) { //yes, this assums strlen > 0
    case 't'://[t]estnet
      header->net = RAI_TESTNET;
      break;
    case 'b'://[b]etanet
      header->net = RAI_BETANET;
      break;
    case 'm'://[m]ainnet
      header->net = RAI_MAINNET;
      break;
  }
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "version_max");
  header->version_max = lua_tonumberx(L, -1, &isnum);
  if(!isnum) {
    *err = "version_max is not a number";
    return 0;
  }
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "version_cur");
  header->version_max = lua_tonumberx(L, -1, &isnum);
  if(!isnum) {
    *err = "version_cur is not a number";
    return 0;
  }
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "version_min");
  header->version_max = lua_tonumberx(L, -1, &isnum);
  if(!isnum) {
    *err = "version_min is not a number";
    return 0;
  }
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "typecode");
  lua_pushvalue(L, message_table_index); 
  lua_call(L, 1, 1); //msg:typecode()
  header->msg_type = lua_tonumber(L, -1);
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "extensions");
  header->extensions = lua_tonumber(L, -1);
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "block_typecode");
  lua_pushvalue(L, message_table_index); 
  lua_call(L, 1, 1); //msg:block_typecode()
  header->block_type = lua_tonumber(L, -1);
  lua_remove(L, -1);
  
  return 8;
}

static size_t message_header_unpack(lua_State *L, int message_table_index, rai_msg_header_t *hdr, const char **err) {
  const char       *str;
  int               isnum;
  
  switch(hdr->net) {
    case RAI_TESTNET:
      lua_rawsetfield_string(L, message_table_index, "net", "test", 4);
      break;
    case RAI_BETANET:
      lua_rawsetfield_string(L, message_table_index, "net", "beta", 4);
      break;
    case RAI_MAINNET:
      lua_rawsetfield_string(L, message_table_index, "net", "main", 4);
      break;
  }
  
  lua_rawsetfield_number(L, message_table_index, "version_max", hdr->version_max);
  lua_rawsetfield_number(L, message_table_index, "version_cur", hdr->version_cur);
  lua_rawsetfield_number(L, message_table_index, "version_min", hdr->version_min);
  
  switch(hdr->msg_type) {
    case RAI_MSG_INVALID:
      lua_rawsetfield_string(L, message_table_index, "type", "invalid", 7);
      break;
    case RAI_MSG_NO_TYPE:
      lua_rawsetfield_string(L, message_table_index, "type", "no_type", 7);
      break;
    case RAI_MSG_KEEPALIVE:
      lua_rawsetfield_string(L, message_table_index, "type", "keepalive", 9);
      break;
    case RAI_MSG_PUBLISH:
      lua_rawsetfield_string(L, message_table_index, "type", "publish", 7);
      break;
    case RAI_MSG_CONFIRM_REQ:
      lua_rawsetfield_string(L, message_table_index, "type", "confirm_req", 11);
      break;
    case RAI_MSG_CONFIRM_ACK:
      lua_rawsetfield_string(L, message_table_index, "type", "confirm_ack", 11);
      break;
    case RAI_MSG_BULK_PULL:
      lua_rawsetfield_string(L, message_table_index, "type", "bulk_pull", 9);
      break;
    case RAI_MSG_BULK_PUSH:
      lua_rawsetfield_string(L, message_table_index, "type", "bulk_push", 9);
      break;
    case RAI_MSG_FRONTIER_REQ:
      lua_rawsetfield_string(L, message_table_index, "type", "frontier_req", 12);
      break;
  }
  
  lua_rawsetfield_number(L, message_table_index, "extensions", hdr->extensions);
  
  switch(hdr->block_type) {
    case RAI_BLOCK_INVALID:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "invalid", 7);
      break;
    case RAI_BLOCK_NOT_A_BLOCK:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "not_a_block", 11);
      break;
    case RAI_BLOCK_SEND:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "send", 4);
      break;
    case RAI_BLOCK_RECEIVE:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "receive", 7);
      break;
    case RAI_BLOCK_OPEN:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "open", 4);
      break;
    case RAI_BLOCK_CHANGE:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "change", 6);
      break;
  }
  
  return 8;
}

static size_t message_header_encode(rai_msg_header_t *hdr, char *buf, const char **err) {
  *buf++ ='R';
  *buf++ ='A' + hdr->net;
  
  *buf++ = hdr->version_max;
  *buf++ = hdr->version_cur;
  *buf++ = hdr->version_min;
  
  *buf++ = hdr->msg_type;
  *buf++ = hdr->extensions;
  *buf++ = hdr->block_type;
  
  return 8;
}

static size_t message_header_decode(rai_msg_header_t *hdr, const char *buf, size_t buflen, const char **errstr) {
  if(buflen < 8) {
    return 0; //not enough header bytes
  }
  if(buf[0] != 'R') {
    *errstr = "Invalid header magic byte";
    return 0; //invalid header
  }
  
  if(buf[1] < 'A' || buf[1] > 'C') {
    *errstr = "Invalid header network-type byte";
  }
  else {
    hdr->net = buf[1]-'A';
  }
  
  hdr->version_max = (uint8_t )buf[2];
  hdr->version_cur = (uint8_t )buf[3];
  hdr->version_min = (uint8_t )buf[4];
  
  hdr->msg_type = (rai_msg_type_t )buf[5];
  hdr->extensions = (uint8_t )buf[6];
  hdr->block_type = (rai_block_type_t )buf[7];
  
  return 8;
}
static size_t message_body_decode_unpack(lua_State *L, rai_msg_header_t *hdr, const char *buf, size_t buflen, const char **errstr) {
  // expects target message table to be at top of stack
  int          i;
  uint8_t      ipv6_addr[16];
  uint16_t     port;
  const char  *buf_start = buf;
  size_t       parsed;
  uint32_t     num;
  switch(hdr->msg_type) {
    case RAI_MSG_INVALID:
    case RAI_MSG_NO_TYPE:
      //these are invalid
      *errstr = "Invalid message type (invalid or no_type)";
      return 0;
    case RAI_MSG_KEEPALIVE:
      if(buflen < 146) {
        return 0;
      }
      for(i=0; i<8; i++) {
        memcpy(ipv6_addr, buf, 16);
        buf+=16;
        port = ntohs(*buf);
        buf+=2;
        //TODO: all this
      }
      //TODO: do this.
      break;
    case RAI_MSG_PUBLISH:
    case RAI_MSG_CONFIRM_REQ:
      lua_createtable(L, 0, 1);
      lua_pushliteral(L, "block");
      parsed = block_decode_unpack(hdr->block_type, L, buf, buflen, errstr); //pushes block table onto stack
      if(parsed == 0) { // need more bytes probably, or maybe there wasa parsing error
        return 0;
      }
      lua_rawset(L, -3);
      buf+= parsed;
      break;
    case  RAI_MSG_CONFIRM_ACK:
      if(buflen < 96) {
        return 0;
      }
      lua_createtable(L, 0, 4);
      lua_rawsetfield_string(L, -1, "account", buf, 32); //vote account
      buf+=32;
      lua_rawsetfield_string(L, -1, "signature", buf, 64); //vote sig
      buf+=64;
      lua_rawsetfield_string(L, -1, "sequence", buf, 8); //vote sequence -- still not sure what this is exactly
      buf+=8;
      
      lua_pushliteral(L, "block");
      parsed = block_decode_unpack(hdr->block_type, L, buf, buflen - (buf - buf_start), errstr); //pushes block table onto stack
      if(parsed == 0) { // need more bytes probably, or maybe there was parsing error
        return 0;
      }
      lua_rawset(L, -3);
      buf += parsed;
      break;
    case RAI_MSG_BULK_PULL:
      if(buflen < 64) return 0;
      lua_createtable(L, 0, 2);
      lua_rawsetfield_string(L, -1, "account", buf, 32); //start_account
      buf+=32;
      lua_rawsetfield_string(L, -1, "block_hash", buf, 32); //end_block
      buf+=32;
      break;
    case RAI_MSG_BULK_PUSH:
      lua_createtable(L, 0, 0);
      //nothing to do, this is an empty message (the data follows the message)
      break;
    case RAI_MSG_FRONTIER_REQ:
      lua_createtable(L, 0, 3);
      lua_rawsetfield_string(L, -1, "account", buf, 32); //start_account
      buf+=32;
      num = ntohl((uint32_t )buf);
      lua_rawsetfield_number(L, -1, "frontier_age", num);
      buf+=8;
      num = ntohl((uint32_t )buf);
      lua_rawsetfield_number(L, -1, "frontier_count", num);
      buf+=8;
      break;
  }
  return buflen - (buf - buf_start);
}

static size_t message_body_pack_encode(lua_State *L, rai_msg_header_t *hdr, char *buf, size_t buflen, const char **errstr) {
  int          i;
  uint8_t      ipv6_addr[16];
  uint16_t     port;
  char        *buf_start = buf;
  size_t       written;
  uint32_t     num;
  switch(hdr->msg_type) {
    case RAI_MSG_INVALID:
    case RAI_MSG_NO_TYPE:
      //these are invalid
      *errstr = "Invalid message type (invalid or no_type)";
      return 0;
    case RAI_MSG_KEEPALIVE:
      if(buflen < 146) return 0;
      //TODO: this mheah
      break;
    case RAI_MSG_PUBLISH:
    case RAI_MSG_CONFIRM_REQ:
      lua_rawgetfield(L, -1, "block");
      buf += block_pack_encode(hdr->block_type, L, buf, buflen);
      if(buf == buf_start) { //not enough space to write block
        return 0;
      }
      lua_remove(L, -1);
      break;
    case  RAI_MSG_CONFIRM_ACK:
      if(buflen < 96) return 0;
      buf += lua_table_field_fixedsize_string_encode(L, -1, "account",     buf, 32); //vote account
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",   buf, 64); //vote sig
      buf += lua_table_field_fixedsize_string_encode(L, -1, "sequence",    buf, 8); //vote sequence -- still not sure what this is exactly
      
      written = block_decode_unpack(hdr->block_type, L, buf, buflen - (buf - buf_start), errstr); //pushes block table onto stack
      if(written == 0) { // need more bytes probably, or maybe there was parsing error
        return 0;
      }
      buf += written;
      break;
    case RAI_MSG_BULK_PULL:
      if(buflen < 64) return 0;
      buf += lua_table_field_fixedsize_string_encode(L, -1, "account",     buf, 32); //start_account
      buf += lua_table_field_fixedsize_string_encode(L, -1, "block_hash",  buf, 32); //end block hash
      break;
    case RAI_MSG_BULK_PUSH:
      lua_createtable(L, 0, 0);
      //nothing to do, this is an empty message (the data follows the message)
      break;
    case RAI_MSG_FRONTIER_REQ:
      if(buflen < 48) return 0;
      buf += lua_table_field_fixedsize_string_encode(L, -1, "account",     buf, 32); //start_account
      
      lua_rawgetfield(L, -1, "frontier_age");
      num = lua_tonumber(L, -1);
      lua_remove(L, -1);
      *(uint16_t *)buf=htonl(num);
      buf+=sizeof(uint16_t);
      
      lua_rawgetfield(L, -1, "frontier_count");
      num = lua_tonumber(L, -1);
      lua_remove(L, -1);
      *(uint16_t *)buf=htonl(num);
      buf+=sizeof(uint16_t);
      
      break;
  }
  return buflen - (buf - buf_start);
}

static size_t block_pack_encode(rai_block_type_t blocktype, lua_State *L, char *buf, size_t buflen) {
  //expects the block table to be at top of the stack
  char          *buf_start = buf;
  
  switch(blocktype) {
    case RAI_BLOCK_INVALID:
    case RAI_BLOCK_NOT_A_BLOCK:
      luaL_error(L, "tried to pack & encode invalid or not-a-block blocktype");
      return 0;
    case RAI_BLOCK_SEND:
      if(buflen < 152)
        luaL_error(L, "buflen too small to encode 'send' block");
      buf += lua_table_field_fixedsize_string_encode(L, -1, "previous",     buf, 32);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "destination",  buf, 32); //destination account
      buf += lua_table_field_fixedsize_string_encode(L, -1, "balance",      buf, 16); //uint128_t encoding?
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",    buf, 64);
      //TODO: generate work
      break;
    case RAI_BLOCK_RECEIVE:
      if(buflen < 136)
        luaL_error(L, "buflen too small to encode 'receive' block");
      buf += lua_table_field_fixedsize_string_encode(L, -1, "previous",     buf, 32);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "source",       buf, 32); //source account
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",    buf, 64);
      //TODO: generate work
      break;
    case RAI_BLOCK_OPEN:
      if(buflen < 168)
        luaL_error(L, "buflen too small to encode 'open' block");
      buf += lua_table_field_fixedsize_string_encode(L, -1, "source",       buf, 32); //source account of first 'send' block
      buf += lua_table_field_fixedsize_string_encode(L, -1, "representative",buf, 32); //voting delegate
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",    buf, 64);
      //TODO: generate work
      break;
    case RAI_BLOCK_CHANGE:
      if(buflen < 136) 
        luaL_error(L, "buflen too small to encode 'open' block");
      buf += lua_table_field_fixedsize_string_encode(L, -1, "previous",     buf, 32);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "representative",buf, 32);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",    buf, 64);
      //TODO: generate work
      break;
  }
  return buf - buf_start;
}

//read in *buf, leaves a table with the unpacked block on top of the stack
//return bytes read, 0 if not enough bytes available to read block
static size_t block_decode_unpack(rai_block_type_t blocktype, lua_State *L, const char *buf, size_t buflen, const char **err) {
  const char      *buf_start = buf;
  switch(blocktype) {
    case RAI_BLOCK_SEND:
      if(buflen < 152)
        return 0; //need moar bytes
      lua_createtable(L, 0, 5);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "previous",     buf, 32);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "destination",  buf, 32); //destination account
      buf += lua_rawsetfield_string_scanbuf(L, -1, "balance",      buf, 16); //uint128_t encoding?
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    buf, 64);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         buf, 8); // is this right?...
      //TODO: generate work
      break;
    case RAI_BLOCK_RECEIVE:
      if(buflen < 136)
        return 0; //moar bytes plz
      lua_createtable(L, 0, 4);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "previous",     buf, 32);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "source",       buf, 32); //source account
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    buf, 64);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         buf, 8); // is this right?...
      break;
    case RAI_BLOCK_OPEN:
      if(buflen < 168)
        return 0; //gib byts nao
      lua_createtable(L, 0, 4);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "source",       buf, 32); //source account of first 'send' block
      buf += lua_rawsetfield_string_scanbuf(L, -1, "representative",buf, 32); //voting delegate
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    buf, 64);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         buf, 8); // is this right?...
      break;
    case RAI_BLOCK_CHANGE:
      if(buflen < 136) 
        return 0; //such bytes, not enough wow
      lua_createtable(L, 0, 4);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "previous",     buf, 32);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "representative",buf, 32);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    buf, 64);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         buf, 8); // is this right?...
      break;
    case RAI_BLOCK_INVALID:
      *err = "tried to unpack 'invalid' type block";
      return 0;
    case RAI_BLOCK_NOT_A_BLOCK:
      *err = "tried to unpack 'not_a_block' type block";
      return 0;
  }
  return buf - buf_start;
}

static int prailude_pack_message(lua_State *L) {
  rai_msg_header_t  header;
  char              msg[256];
  char             *cur = msg;
  size_t            lastsz;
  const char       *err = NULL;
  
  luaL_argcheck(L, lua_gettop(L) == 1, 0, "incorrect number of arguments: must have just the message table");
  
  if(message_header_pack(L, 1, &header, &err) == 0) {
    lua_pushnil(L);
    lua_pushstring(L, err ? err : "error packing message header");
    return 2;
  }
  
  lastsz = message_header_encode(&header, cur, &err);
  if(lastsz == 0) {
    lua_pushnil(L);
    lua_pushstring(L, err ? err : "error encoding message header");
  }
  cur+= lastsz;
  
  lastsz = message_body_pack_encode(L, &header, cur, 256-8, &err);
  if(lastsz == 0) {
    lua_pushnil(L);
    lua_pushstring(L, err ? err : "error packing and encoding message body");
  }
  cur+= lastsz;
  
  //return packed message string
  lua_pushlstring(L, msg, cur - msg);
  return 1;
}

static int prailude_unpack_message(lua_State *L) {
  size_t              sz;
  const char         *packed_msg;
  const char         *cur;
  size_t              msg_sz;
  rai_msg_header_t    header;
  const char         *err;
  
  luaL_argcheck(L, lua_gettop(L) == 1, 0, "incorrect number of arguments: must have just the packed message");
  
  packed_msg = lua_tolstring(L, -1, &msg_sz);
  if(!packed_msg || msg_sz == 0) {
    lua_pushnil(L);
    lua_pushliteral(L, "invalid packed message to unpack: empty or not a string");
    return 2;
  }
  cur = packed_msg;
  
  sz = message_header_decode(&header, cur, msg_sz, &err);
  if(sz == 0) {
    lua_pushnil(L);
    lua_pushstring(L, err ? err : "error decoding message header");
    return 2;
  }
  
  lua_createtable(L, 0, 10);
  sz = message_header_unpack(L, lua_gettop(L), &header, &err);
  if(sz == 0) {
    lua_pushnil(L);
    lua_pushstring(L, err ? err : "error unpacking message header");
    return 2;
  }
  cur+=sz;
  
  sz = message_body_decode_unpack(L, &header, cur, msg_sz - (cur - packed_msg), &err);
  if(sz == 0) {
    lua_pushnil(L);
    lua_pushstring(L, err ? err : "error decoding and unpacking message body");
    return 2;
  }
  cur +=sz;
  
  //message table should be at top of stack right now
  if(msg_sz - (cur - packed_msg) > 0) {
    //refund input buffer leftovers
    lua_pushlstring(L, cur, msg_sz - (cur - packed_msg));
    return 2;
  }
  else {
    return 1;
  }
}

static const struct luaL_Reg prailude_parser_functions[] = {
  { "pack_message", prailude_pack_message },
  { "unpack_message", prailude_unpack_message },
  // { "parse_frontier_stream", prailude_parse_frontier_stream },
  // { "parse_bulk_stream", prailude_parse_bulk_stream },
  { NULL, NULL }
};

int luaopen_prailude_message_parser(lua_State* lua) {
  lua_newtable(lua);
#if LUA_VERSION_NUM > 501
  luaL_setfuncs(lua,prailude_parser_functions,0);
#else
  luaL_register(lua, NULL, prailude_parser_functions);
#endif
  return 1;
}
