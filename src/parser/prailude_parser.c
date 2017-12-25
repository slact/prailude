#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <signal.h>
#include <lua.h>
#include <lauxlib.h>
#include <stdbool.h>

#include "prailude_parser.h"
#include "util/prailude_util.h"

#include <netinet/in.h>

#define RETURN_FAIL(Lua, errmsg) \
  lua_pushnil(Lua); \
  lua_pushstring(L, errmsg); \
  return 2

  
#define lua_rawgetfield(Lua, tindex, string_literal) \
  lua_pushsliteral(Lua, string_literal);  \
  lua_rawget(Lua, tindex)
  
#define lua_rawsetfield_string(Lua, tindex, field, str, strlen) \
  lua_pushstring(Lua, field);        \
  lua_pushlstring(Lua, str, strlen); \
  lua_rawset(Lua, tindex)

#define lua_rawsetfield_number(Lua, tindex, field, num) \
  lua_pushliteral(Lua, field);  \
  lua_pushnumber(Lua, num);        \
  lua_rawset(Lua, tindex)

static size_t lua_rawsetfield_string_scanbuf(lua_State *L, int tindex, const char *field, char *buf, size_t strlen) {
  lua_rawsetfield_string(Lua, tindex, field, str, strlen);
  return strlen;
}
  
static int prailude_parse_udp_message(lua_State *L) {
  return 0;
}

static void message_header_pack(lua_State *L, int message_table_index, rai_msg_header_t *hdr) {
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
  header.version_max = lua_tonumberx(L, -1, &isnum);
  if(!is->um) {
    RETURN_FAIL(L, "version_max is not a number");
  }
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "version_cur");
  header->version_max = lua_tonumberx(L, -1, &isnum);
  if(!isnum) {
    RETURN_FAIL(L, "version_cur is not a number");
  }
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "version_min");
  header->version_max = lua_tonumberx(L, -1, &isnum);
  if(!isnum) {
    RETURN_FAIL(L, "version_min is not a number");
  }
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "typecode");
  lua_pushvalue(L, message_table_index); 
  lua_call(L, 1, 1); //msg:typecode()
  header->msg_type = lua_tonumber(L, -1);
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "extensions")
  header->extensions = lua_tonumber(L, -1);
  lua_remove(L, -1);
  
  lua_rawgetfield(L, message_table_index, "block_typecode");
  lua_pushvalue(L, message_table_index); 
  lua_call(L, 1, 1); //msg:block_typecode()
  header->block_type = lua_tonumber(L, -1);
  lua_remove(L, -1);
}

static void message_header_unpack(lua_State *L, int message_table_index, rai_msg_header_t *hdr) {
  const char       *str;
  int               isnum;
  
  switch(hdr->net) {
    case RAI_TESTNET:
      lua_rawsetfield_string(L, message_table_index, "net", "test");
      break;
    case RAI_BETANET:
      lua_rawsetfield_string(L, message_table_index, "net", "beta");
      break;
    case RAI_MAINNET:
      lua_rawsetfield_string(L, message_table_index, "net", "main");
      break;
  }
  
  lua_rawsetfield_number(L, message_table_index, "version_max", hdr->version_max);
  lua_rawsetfield_number(L, message_table_index, "version_cur", hdr->version_cur);
  lua_rawsetfield_number(L, message_table_index, "version_min", hdr->version_min);
  
  switch(hdr->msg_type) {
    case RAI_MSG_INVALID:
      lua_rawsetfield_string(L, message_table_index, "type", "invalid");
      break;
    case RAI_MSG_NO_TYPE:
      lua_rawsetfield_string(L, message_table_index, "type", "no_type");
      break;
    case RAI_MSG_KEEPALIVE:
      lua_rawsetfield_string(L, message_table_index, "type", "keepalive");
      break;
    case RAI_MSG_PUBLISH:
      lua_rawsetfield_string(L, message_table_index, "type", "publish");
      break;
    case RAI_MSG_CONFIRM_REQ:
      lua_rawsetfield_string(L, message_table_index, "type", "confirm_req");
      break;
    case RAI_MSG_CONFIRM_ACK:
      lua_rawsetfield_string(L, message_table_index, "type", "confirm_ack");
      break;
    case RAI_MSG_BULK_PULL:
      lua_rawsetfield_string(L, message_table_index, "type", "bulk_pull");
      break;
    case RAI_MSG_BULK_PUSH:
      lua_rawsetfield_string(L, message_table_index, "type", "bulk_push");
      break;
    case RAI_MSG_FRONTIER_REQ:
      lua_rawsetfield_string(L, message_table_index, "type", "frontier_req");
      break;
  }
  
  lua_rawsetfield_number(L, message_table_index, "extensions", hdr->extensions);
  
  switch(hdr->msg_type) {
    case RAI_BLOCK_INVALID:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "invalid");
      break;
    case RAI_BLOCK_NOT_A_BLOCK:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "not_a_block");
      break;
    case RAI_BLOCK_SEND:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "send");
      break;
    case RAI_BLOCK_RECEIVE:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "receive");
      break;
    case RAI_BLOCK_OPEN:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "open");
      break;
    case RAI_BLOCK_CHANGE:
      lua_rawsetfield_string(L, message_table_index, "msg_type", "change");
      break;
  }
}

static size_t message_header_encode(rai_msg_header_t *hdr, char *buf) {
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

static size_t message_header_decode(rai_msg_header_t *hdr, char *buf, size_t buflen, const char *errstr) {
  if(buflen < 0) {
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
  hdr->block_type = (ra_block_type_t )buf[7];
  
  return 8;
}

static size_t message_body_decode_unpack(rai_msg_header_t *hdr, char *buf, size_t buflen) {
  
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

static char * lua_table_field_fixedsize_string_encode(lua_State *L, int tblindex, const char *fieldname, size_t expected_strlen, char *dst_buf) {
  const char *str = lua_tblfield_to_fixedsize_string(L, tblindex, fieldname, expected_strlen);
  memcpy(dst_buf, str, expected_strlen);
  return expected_strlen;
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
      buf += lua_table_field_fixedsize_string_encode(L, -1, "previous",     32, buf);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "destination",  32, buf); //destination account
      buf += lua_table_field_fixedsize_string_encode(L, -1, "balance",      16, buf); //uint128_t encoding?
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",    64, buf);
      //TODO: generate work
      break;
    case RAI_BLOCK_RECEIVE:
      if(buflen < 136)
        luaL_error(L, "buflen too small to encode 'receive' block");
      buf += lua_table_field_fixedsize_string_encode(L, -1, "previous",     32, buf);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "source",       32, buf); //source account
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",    64, buf);
      //TODO: generate work
      break;
    case RAI_BLOCK_OPEN:
      if(buflen < 168)
        luaL_error(L, "buflen too small to encode 'open' block");
      buf += lua_table_field_fixedsize_string_encode(L, -1, "source",       32, buf); //source account of first 'send' block
      buf += lua_table_field_fixedsize_string_encode(L, -1, "representative",32, buf); //voting delegate
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",    64, buf);
      //TODO: generate work
      break;
    case RAI_BLOCK_CHANGE:
      if(buflen < 136) 
        luaL_error(L, "buflen too small to encode 'open' block");
      buf += lua_table_field_fixedsize_string_encode(L, -1, "previous",     32, buf);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "representative",32, buf);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",    64, buf);
      //TODO: generate work
      break;
    case RAI_BLOCK_INVALID:
      luaL_error(L, "tried to encode 'invalid' type block");
      break;
      case RAI_BLOCK_NOT_A_BLOCK:
      luaL_error(L, "tried to encode 'not_a_block' type block");
      break;
  }
  return buf - buf_start;
}

//read in *buf, leaves a table with the unpacked block on top of the stack
//return bytes read, 0 if not enough bytes available to read block
static size_t block_decode_unpack(rai_block_type_t blocktype, lua_State *L, char *buf, size_t buflen) {
  char          *buf_start = buf;
  switch(blocktype) {
    case RAI_BLOCK_SEND:
      if(buflen < 152)
        return 0; //need moar bytes
      lua_createtable(L, 0, 5);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "previous",     32, buf);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "destination",  32, buf); //destination account
      buf += lua_rawsetfield_string_scanbuf(L, -1, "balance",      16, buf); //uint128_t encoding?
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    64, buf);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         8, buf); // is this right?...
      //TODO: generate work
      break;
    case RAI_BLOCK_RECEIVE:
      if(buflen < 136)
        return 0; //moar bytes plz
      lua_createtable(L, 0, 4);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "previous",     32, buf);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "source",       32, buf); //source account
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    64, buf);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         8, buf); // is this right?...
      break;
    case RAI_BLOCK_OPEN:
      if(buflen < 168)
        return 0; //gib byts nao
      lua_createtable(L, 0, 4);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "source",       32, buf); //source account of first 'send' block
      buf += lua_rawsetfield_string_scanbuf(L, -1, "representative",32, buf); //voting delegate
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    64, buf);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         8, buf); // is this right?...
      break;
    case RAI_BLOCK_CHANGE:
      if(buflen < 136) 
        return 0; //such bytes, not enough wow
      lua_createtable(L, 0, 4);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "previous",     32, buf);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "representative",32, buf);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    64, buf);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         8, buf); // is this right?...
      break;
    case RAI_BLOCK_INVALID:
      luaL_error(L, "tried to unpack 'invalid' type block");
      return 0;
    case RAI_BLOCK_NOT_A_BLOCK:
      luaL_error(L, "tried to unpack 'not_a_block' type block");
      return 0;
  }
  return buf - buf_start;
}

static int prailude_pack_message(lua_State *L) {
  rai_msg_header_t  header;
  char              msg[256];
  char             *cur;
  
  luaL_argcheck(L, lua_gettop(L) == 1, 0, "incorrect number of arguments: must have just the message table");
  
  message_header_pack(L, 1, &header);
  cur += message_header_encode(&header, msg);
  
  if(header.block_type != RAI_BLOCK_INVALID && header.block_type != RAI_BLOCK_INVALID) {
    lua_rawgetfield(L, 1, "block");
    cur += block_pack_encode(L, cur, 504);
  }
  
  //return packed message string
  lua_pushlstring(L, msg, cur - msg);
  
  //store it 
  lua_pushliteral(L, "packed");
  lua_pushvalue(L, -2); //packed msg string
  lua_rawset(L, 1);
  
  //the packed mesage string is now on top of the stack
  return 1;
}


static const struct luaL_Reg prailude_parser_functions[] = {
  { "pack_message", prailude_pack_message },
  //  { "parse_frontier_stream", prailude_parse_frontier_stream },
  //{ "parse_frontier_stream", prailude_parse_frontier_stream },
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
