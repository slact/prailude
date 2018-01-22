#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <signal.h>
#include <lua.h>
#include <lauxlib.h>
#include <stdbool.h>
#include <limits.h>

#include "parser.h"
#include "util/util.h"
#include "util/net.h"

#include <netinet/in.h>

#define RETURN_FAIL(Lua, errmsg) \
  lua_pushnil(Lua); \
  lua_pushstring(L, errmsg); \
  return 2

#if LUA_VERSION_NUM <= 501
static int lua_absindex( lua_State * const L, int const _idx) {
  // positive or pseudo-index: return the index, else convert to absolute stack index
  return (_idx > 0) ? _idx : (_idx <= LUA_REGISTRYINDEX) ? _idx : (lua_gettop( L) + 1 + _idx);
}
lua_Number lua_tonumberx (lua_State *L, int i, int *isnum) {
  lua_Number n = lua_tonumber(L, i);
  if (isnum != NULL) {
    *isnum = (n != 0 || lua_isnumber(L, i));
  }
  return n;
}
#endif

static int lua_rawgetfield(lua_State *const L, int tindex, const char *field) {
  tindex = lua_absindex(L, tindex);
  lua_pushstring(L, field);
  lua_rawget(L, tindex);
  return 1;
}

static char *lua_dbgval(lua_State *L, int n) {
  static char buf[255];
  int         type = lua_type(L, n);
  const char *typename = lua_typename(L, type);
  const char *str;
  lua_Number  num;
  
  
  switch(type) {
    case LUA_TNUMBER:
      num = lua_tonumber(L, n);
      sprintf(buf, "%s: %f", typename, num);
      break;
    case LUA_TBOOLEAN:
      sprintf(buf, "%s: %s", typename, lua_toboolean(L, n) ? "true" : "false");
      break;
    case LUA_TSTRING:
      str = lua_tostring(L, n);
      sprintf(buf, "%s: %.50s%s", typename, str, strlen(str) > 50 ? "..." : "");
      break;
    default:
      lua_getglobal(L, "tostring");
      lua_pushvalue(L, n);
      lua_call(L, 1, 1);
      str = lua_tostring(L, -1);
      sprintf(buf, "%s", str);
      lua_pop(L, 1);
  }
  return buf;
}

void lua_printstack(lua_State *L, const char *what) {
  int n, top = lua_gettop(L);
  printf("lua stack: %s\n", what);
  for(n=top; n>0; n--) {
    printf("  [%i]: %s\n", n, lua_dbgval(L, n));
  }
}

void lua_inspect_stack(lua_State *L, const char *what) {
  int n, top = lua_gettop(L);
  printf("lua stack: %s\n", what);
  for(n=top; n>0; n--) {
    printf("\n=========== [%3i] ========== [%3i] =\n", n, n-top-1);
    luaL_loadstring(L, "return (require \"mm\")");
    lua_call(L, 0, 1);
    lua_pushvalue(L, n);
    lua_call(L, 1, 0);
  }
  printf("\n======== [end of stack] ============\n");
}

static void lua_rawsetfield_string(lua_State *L, int tindex, const char *field, const char *str, size_t strlen) {
  tindex = lua_absindex(L, tindex);
  lua_pushstring(L, field);
  lua_pushlstring(L, str, strlen);
  lua_rawset(L, tindex);
}

#define lua_rawsetfield_literal(Lua, absindex, field, str) \
  lua_pushliteral(Lua, field);     \
  lua_pushliteral(Lua, str);       \
  lua_rawset(Lua, absindex)

static void lua_rawsetfield_number(lua_State *L, int tindex, const char *field, lua_Number num) {
  tindex = lua_absindex(L, tindex);
  lua_pushstring(L, field);
  lua_pushnumber(L, num);
  lua_rawset(L, tindex);
}

static size_t block_decode_unpack(rai_block_type_t blocktype, lua_State *L, const char *buf, size_t buflen, const char **err);
static size_t block_pack_encode(rai_block_type_t blocktype, lua_State *L, char *buf, size_t buflen);
  
static size_t lua_rawsetfield_string_scanbuf(lua_State *L, int tindex, const char *field, const char *buf, size_t buflen) {
  lua_rawsetfield_string(L, tindex, field, buf, buflen);
  return buflen;
}
  
static const char*lua_tblfield_to_fixedsize_string(lua_State *L, int tblindex, const char *fieldname, size_t expected_strlen) {
  const char    *str;
  size_t         len;
  lua_getfield(L, tblindex, fieldname);
  str = lua_tolstring(L, -1, &len);
  if(str == NULL) {
    luaL_error(L, "expected table field '%s' to be a string", fieldname);
  }
  if(expected_strlen != len) {
    luaL_error(L, "invalid length of table field value '%s'", fieldname);
  }
  lua_pop(L, 1);
  return str;
}

static size_t lua_table_field_fixedsize_string_encode(lua_State *L, int tblindex, const char *fieldname, char *dst_buf, size_t expected_strlen) {
  const char *str = lua_tblfield_to_fixedsize_string(L, tblindex, fieldname, expected_strlen);
  memcpy(dst_buf, str, expected_strlen);
  return expected_strlen;
}

static size_t message_header_pack(lua_State *L, int message_table_index, rai_msg_header_t *header, const char **err) {
  const char       *str;
  int               isnum;
  lua_Number        num;
  
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
  lua_pop(L, 1);
  
  lua_getfield(L, message_table_index, "version_max");
  num = lua_tonumberx(L, -1, &isnum);
  if(!isnum) {
    *err = "version_max is not a number";
    return 0;
  }
  header->version_max = num;
  lua_pop(L, 1);
  
  lua_getfield(L, message_table_index, "version_cur");
  num = lua_tonumberx(L, -1, &isnum);
  if(!isnum) {
    *err = "version_cur is not a number";
    return 0;
  }
  header->version_cur = num;
  lua_pop(L, 1);
  
  lua_getfield(L, message_table_index, "version_min");
  num = lua_tonumberx(L, -1, &isnum);
  if(!isnum) {
    *err = "version_min is not a number";
    return 0;
  }
  header->version_min = num;
  lua_pop(L, 1);
  
  lua_getfield(L, message_table_index, "typecode");
  lua_pushvalue(L, message_table_index); 
  lua_call(L, 1, 1); //msg:typecode()
  header->msg_type = lua_tonumber(L, -1);
  lua_pop(L, 1);
  
  lua_getfield(L, message_table_index, "extensions");
  header->extensions = lua_tonumber(L, -1);
  lua_pop(L, 1);
  
  lua_getfield(L, message_table_index, "block_typecode");
  lua_pushvalue(L, message_table_index); 
  lua_call(L, 1, 1); //msg:block_typecode()
  header->block_type = lua_tonumber(L, -1);
  lua_pop(L, 1);
  
  return 8;
}

static size_t message_header_unpack(lua_State *L, int message_table_index, rai_msg_header_t *hdr, const char **err) {
  
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
      lua_rawsetfield_literal(L, message_table_index, "type", "invalid");
      break;
    case RAI_MSG_NO_TYPE:
      lua_rawsetfield_literal(L, message_table_index, "type", "no_type");
      break;
    case RAI_MSG_KEEPALIVE:
      lua_rawsetfield_literal(L, message_table_index, "type", "keepalive");
      break;
    case RAI_MSG_PUBLISH:
      lua_rawsetfield_literal(L, message_table_index, "type", "publish");
      break;
    case RAI_MSG_CONFIRM_REQ:
      lua_rawsetfield_literal(L, message_table_index, "type", "confirm_req");
      break;
    case RAI_MSG_CONFIRM_ACK:
      lua_rawsetfield_literal(L, message_table_index, "type", "confirm_ack");
      break;
    case RAI_MSG_BULK_PULL:
      lua_rawsetfield_literal(L, message_table_index, "type", "bulk_pull");
      break;
    case RAI_MSG_BULK_PUSH:
      lua_rawsetfield_literal(L, message_table_index, "type", "bulk_push");
      break;
    case RAI_MSG_FRONTIER_REQ:
      lua_rawsetfield_literal(L, message_table_index, "type", "frontier_req");
      break;
  }
  
  lua_rawsetfield_number(L, message_table_index, "extensions", hdr->extensions);
  
  switch(hdr->block_type) {
    case RAI_BLOCK_INVALID:
      lua_rawsetfield_string(L, message_table_index, "block_type", "invalid", 7);
      break;
    case RAI_BLOCK_NOT_A_BLOCK:
      lua_rawsetfield_string(L, message_table_index, "block_type", "not_a_block", 11);
      break;
    case RAI_BLOCK_SEND:
      lua_rawsetfield_string(L, message_table_index, "block_type", "send", 4);
      break;
    case RAI_BLOCK_RECEIVE:
      lua_rawsetfield_string(L, message_table_index, "block_type", "receive", 7);
      break;
    case RAI_BLOCK_OPEN:
      lua_rawsetfield_string(L, message_table_index, "block_type", "open", 4);
      break;
    case RAI_BLOCK_CHANGE:
      lua_rawsetfield_string(L, message_table_index, "block_type", "change", 6);
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
  int          i, j;
  
  uint16_t     port;
  
  char         ipv6addr_str[INET6_ADDRSTRLEN];
  
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
      if(buflen < 144) {
        raise(SIGSTOP);
        return 0;
      }
      lua_pushliteral(L, "peers");
      lua_createtable(L, 8, 0); //table to hold peers
      for(i=0, j=0; i<8; i++) {
        
        if((uint64_t )buf[0] == 0 && (uint64_t )buf[8]==0 && (uint16_t )buf[16] == 0) {
          //zero-filled
          buf+=18;;
        }
        else {
          inet_ntop6((const unsigned char *)buf, ipv6addr_str, INET6_ADDRSTRLEN);
          buf+=16;
          
          //port = ntohs(*buf); //nothing is network-order here
          port = *(uint16_t *)buf;
          buf+=2;
          
          lua_createtable(L, 0, 2); //for the peer address and port
          lua_pushliteral(L, "address");
          lua_pushstring(L, ipv6addr_str);
          lua_rawset(L, -3);
          
          lua_pushliteral(L, "port");
          lua_pushnumber(L, port);
          lua_rawset(L, -3);
          
          lua_rawseti(L, -2, ++j); //store in peers table
        }
      }
      lua_rawset(L, -3);
      break;
    case RAI_MSG_PUBLISH:
    case RAI_MSG_CONFIRM_REQ:
      lua_pushliteral(L, "block");
      parsed = block_decode_unpack(hdr->block_type, L, buf, buflen, errstr); //pushes block table onto stack
      if(parsed == 0) { // need more bytes probably, or maybe there wasa parsing error
        raise(SIGSTOP);
        return 0;
      }
      
      lua_rawset(L, -3);
      buf+= parsed;
      break;
    case  RAI_MSG_CONFIRM_ACK:
      if(buflen < 96) {
        raise(SIGSTOP);
        return 0;
      }
      lua_rawsetfield_string(L, -1, "account", buf, 32); //vote account
      buf+=32;
      lua_rawsetfield_string(L, -1, "signature", buf, 64); //vote sig
      buf+=64;
      lua_rawsetfield_string(L, -1, "sequence", buf, 8); //vote sequence -- still not sure what this is exactly
      buf+=8;
      
      lua_pushliteral(L, "block");
      parsed = block_decode_unpack(hdr->block_type, L, buf, buflen - (buf - buf_start), errstr); //pushes block table onto stack
      if(parsed == 0) { // need more bytes probably, or maybe there was parsing error
        raise(SIGSTOP);
        return 0;
      }
      lua_rawset(L, -3);
      buf += parsed;
      break;
    case RAI_MSG_BULK_PULL:
      if(buflen < 64) {raise(SIGSTOP); return 0;}
      lua_rawsetfield_string(L, -1, "account", buf, 32); //start_account
      buf+=32;
      lua_rawsetfield_string(L, -1, "block_hash", buf, 32); //end_block
      buf+=32;
      break;
    case RAI_MSG_BULK_PUSH:
      //nothing to do, this is an empty message (the data follows the message)
      break;
    case RAI_MSG_FRONTIER_REQ:
      if(buflen < 48) { raise(SIGSTOP); return 0;}
      lua_rawsetfield_string(L, -1, "account", buf, 32); //start_account
      buf+=32;
      //num = ntohl(*(uint32_t *)buf); no network-ordering on the wire
      lua_inspect_stack(L, "got FRONTIER_REQ");
      num = *(uint32_t *)buf;
      lua_rawsetfield_number(L, -1, "frontier_age", num);
      buf+=4;
      //num = ntohl(*(uint32_t *)buf); no network-ordering on the wire
      num = *(uint32_t *)buf;
      lua_rawsetfield_number(L, -1, "frontier_count", num);
      buf+=4;
      break;
  }
  return buf - buf_start;
}

static size_t message_body_pack_encode(lua_State *L, rai_msg_header_t *hdr, char *buf, size_t buflen, const char **err) {
  int          i;
  const char  *peer_addr;
  uint16_t     peer_port;
  char        *buf_start = buf;
  size_t       written;
  uint32_t     num;
  switch(hdr->msg_type) {
    case RAI_MSG_INVALID:
    case RAI_MSG_NO_TYPE:
      //these are invalid
      *err = "Invalid message type (invalid or no_type)";
      return 0;
    case RAI_MSG_KEEPALIVE:
      if(buflen < 144){
        *err = "not enough space to write keepalive message";
        return 0;
      }
      
      lua_rawgetfield(L, -1, "peers");
      if(!lua_istable(L, -1)) {
        //no peers table? no problem. zero-fill our peers
        memset(buf, 0, 18*8);
        buf += 18*8;
      }
      else {
        for(i=1; i<=8; i++) {
          lua_rawgeti(L, -1, i);
          if(lua_istable(L, -1)) {
            lua_rawgetfield(L, -1, "address");
            peer_addr = lua_tostring(L, -1);
            lua_pop(L, 1);
            
            lua_rawgetfield(L, -1, "port");
            peer_port = lua_tonumber(L, -1);
            lua_pop(L, 1);
            
            inet_pton6(peer_addr, (unsigned char *)buf);
            
            //endinanness bug right here, just like in the original implementation
            memcpy(&buf[16], &peer_port, sizeof(peer_port));
          }
          else {
            //probably nil
            memset(buf, 0, 18);
            lua_pop(L, 1);
          }
          buf += 18;
        }
      }
      lua_pop(L, 1);
      break;
    case RAI_MSG_PUBLISH:
    case RAI_MSG_CONFIRM_REQ:
      lua_rawgetfield(L, -1, "block");
      if(!lua_istable(L, -1)) {
        *err = "expected a 'block' table in 'publish' or 'confirm_req' message";
        return 0;
      }
      buf += block_pack_encode(hdr->block_type, L, buf, buflen);
      if(buf == buf_start) { //not enough space to write block
        *err = "not enough space to write confirm_req message";
        return 0;
      }
      lua_pop(L, 1);
      break;
    case  RAI_MSG_CONFIRM_ACK:
      if(buflen < 96) {
        *err = "not enough space to write 'confirm_ack' message";
        return 0;
      }
      buf += lua_table_field_fixedsize_string_encode(L, -1, "account",     buf, 32); //vote account
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",   buf, 64); //vote sig
      buf += lua_table_field_fixedsize_string_encode(L, -1, "sequence",    buf, 8); //vote sequence -- still not sure what this is exactly
      
      written = block_decode_unpack(hdr->block_type, L, buf, buflen - (buf - buf_start), err); //pushes block table onto stack
      if(written == 0) { // need more bytes probably, or maybe there was parsing error
        //err should have already been set
        return 0;
      }
      buf += written;
      break;
    case RAI_MSG_BULK_PULL:
      if(buflen < 64) {
        *err = "not enough space to write 'bulk_pull' message";
        return 0;
      }
      buf += lua_table_field_fixedsize_string_encode(L, -1, "account",     buf, 32); //start_account
      buf += lua_table_field_fixedsize_string_encode(L, -1, "block_hash",  buf, 32); //end block hash
      break;
    case RAI_MSG_BULK_PUSH:
      //nothing to do, this is an empty message (the data follows the message)
      break;
    case RAI_MSG_FRONTIER_REQ:
      if(buflen < 48) {
        *err = "not enough space to write 'frontier_req' message";
        return 0;
      }
      lua_rawgetfield(L, -1, "account");
      if(lua_isnil(L, -1)) {
        lua_pop(L, 1);
        memset(buf, '\0', 32);
        buf += 32;
      }
      else {
        lua_pop(L, 1);
        buf += lua_table_field_fixedsize_string_encode(L, -1, "account",     buf, 32); //start_account
      }
      lua_rawgetfield(L, -1, "frontier_age");
      if(lua_isnil(L, -1)) {
        //default is max age
        lua_pop(L, 1);
        strcpy(buf, "\xff\xff\xff\xff");
      }
      else {
        num = lua_tonumber(L, -1);
        lua_pop(L, 1);
        //*(uint64_t *)buf=htonl(num); // no network-byte order on the wire
        *(uint32_t *)buf=num;
      }
      buf+=4;
      
      lua_rawgetfield(L, -1, "frontier_count");
      if(lua_isnil(L, -1)) {
        //default is max count
        lua_pop(L, 1);
        strcpy(buf, "\xff\xff\xff\xff");
      }
      else {
        num = lua_tonumber(L, -1);
        lua_pop(L, 1);
        //*(uint64_t *)buf=htonl(num); // no network-byte order on the wire
        *(uint32_t *)buf=num;
      }
      buf+=4;
      
      break;
  }
  return buf - buf_start;
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
      buf += lua_table_field_fixedsize_string_encode(L, -1, "work",         buf, 8);
      break;
    case RAI_BLOCK_RECEIVE:
      if(buflen < 136)
        luaL_error(L, "buflen too small to encode 'receive' block");
      buf += lua_table_field_fixedsize_string_encode(L, -1, "previous",     buf, 32);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "source",       buf, 32); //source block
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",    buf, 64);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "work",         buf, 8);
      break;
    case RAI_BLOCK_OPEN:
      if(buflen < 168)
        luaL_error(L, "buflen too small to encode 'open' block");
      buf += lua_table_field_fixedsize_string_encode(L, -1, "source",       buf, 32); //source block of first 'send' block
      buf += lua_table_field_fixedsize_string_encode(L, -1, "representative",buf, 32); //voting delegate
      buf += lua_table_field_fixedsize_string_encode(L, -1, "account",       buf, 32); //opening account (pubkey)
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",     buf, 64);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "work",          buf, 8);
      break;
    case RAI_BLOCK_CHANGE:
      if(buflen < 136) 
        luaL_error(L, "buflen too small to encode 'change' block");
      buf += lua_table_field_fixedsize_string_encode(L, -1, "previous",     buf, 32);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "representative",buf, 32);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "signature",    buf, 64);
      buf += lua_table_field_fixedsize_string_encode(L, -1, "work",         buf, 8);
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
      if(buflen < 152) {
        raise(SIGSTOP);
        return 0; //need moar bytes
      }
      lua_createtable(L, 0, 5);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "previous",     buf, 32);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "destination",  buf, 32); //destination account
      buf += lua_rawsetfield_string_scanbuf(L, -1, "balance",      buf, 16); //uint128_t encoding?
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    buf, 64);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         buf, 8); // is this right?...
      //TODO: generate work
      break;
    case RAI_BLOCK_RECEIVE:
      if(buflen < 136) {
        raise(SIGSTOP);
        return 0; //moar bytes plz
      }
      lua_createtable(L, 0, 4);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "previous",     buf, 32);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "source",       buf, 32); //source block
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    buf, 64);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         buf, 8); // is this right?...
      break;
    case RAI_BLOCK_OPEN:
      if(buflen < 168) {
        raise(SIGSTOP);
        return 0; //gib byts nao
      }
      lua_createtable(L, 0, 4);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "source",       buf, 32); //source account of first 'send' block
      buf += lua_rawsetfield_string_scanbuf(L, -1, "representative",buf, 32); //voting delegate
      buf += lua_rawsetfield_string_scanbuf(L, -1, "account",      buf, 32); //own acct (pubkey)
      buf += lua_rawsetfield_string_scanbuf(L, -1, "signature",    buf, 64);
      buf += lua_rawsetfield_string_scanbuf(L, -1, "work",         buf, 8); // is this right?...
      break;
    case RAI_BLOCK_CHANGE:
      if(buflen < 136) {
        raise(SIGSTOP);
        return 0; //such bytes, not enough wow
      }
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
  lua_rawsetfield_string_scanbuf(L, -1, "raw",     buf_start, buf - buf_start);
  return buf - buf_start;
}

static int prailude_pack_message(lua_State *L) {
  rai_msg_header_t  header;
  char              msg[512];
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
    return 2;
  }
  cur+= lastsz;
  lastsz = message_body_pack_encode(L, &header, cur, 512-8, &err);
  if(lastsz == 0) {
    lua_pushnil(L);
    lua_pushstring(L, err ? err : "error packing and encoding message body");
    return 2;
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
  const char         *err = NULL;
  
  luaL_argcheck(L, lua_gettop(L) == 1, 0, "incorrect number of arguments: must have just the packed message");
  
  
  packed_msg = lua_tolstring(L, -1, &msg_sz);
  if(!packed_msg || msg_sz == 0) {
    //raise(SIGSTOP);
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

static void bin_to_strhex(const unsigned char *bin, size_t bin_len, unsigned char *out) {
  unsigned char     hex_str[]= "0123456789abcdef";
  unsigned int      i;
  for (i = 0; i < bin_len; i++) {
    out[i * 2 + 0] = hex_str[(bin[i] >> 4) & 0x0F];
    out[i * 2 + 1] = hex_str[(bin[i]     ) & 0x0F];
  }
  out[bin_len * 2] = 0;
}

void print_chunk(const unsigned char *bin, size_t bin_len) {
  unsigned char buf[512];
  bin_to_strhex(bin, bin_len, buf);
  printf("%s\n", buf);
}


static int prailude_unpack_frontiers(lua_State *L) {
  size_t      sz;
  const char *buf = luaL_checklstring(L, 1, &sz);
  const char *end = &buf[sz];
  const char *cur = buf;
  int         i = 0;
  int         done = 0;
  const char *last_acct = NULL;
  float       progress = 0;
  
  const char *zero_frontier = "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
                              "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
                              "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
                              "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
  
  lua_newtable(L);
  
  for(cur = buf; cur + 64 <= end; cur += 64) {
    //print_chunk(cur, 64);
    if(memcmp(cur, zero_frontier, 64) == 0) {
      //last frontier
      done = 1;
      break;
    }
    
    lua_createtable(L, 0, 2);
    
    lua_pushliteral(L, "account");
    lua_pushlstring(L, cur, 32);
    last_acct = cur;
    lua_rawset(L, -3);
    
    lua_pushliteral(L, "hash");
    lua_pushlstring(L, &cur[32], 32);
    lua_rawset(L, -3);
    lua_rawseti(L, -2, ++i);
  }
  
  //leftovers
  if(cur >= end) {
    lua_pushnil(L);
  }
  else {
    //printf("leftovers [%d]: ", end - cur);
    //print_chunk(cur, end - cur);
    lua_pushlstring(L, cur, (end - cur));
  }
  
  //done?
  lua_pushboolean(L, done);
  
  //progress
  if(last_acct) {
    uint64_t topbytes = 0;
    int i;
    for(i=0; i<8; i++) {
      topbytes <<= 8;
      topbytes += (uint8_t )last_acct[0];
    }
    
    progress = (double)topbytes / UINT64_MAX;
    
    lua_pushnumber(L, progress);
    return 4;
  }
  else {
    return 3;
  }
}


static int prailude_pack_frontiers(lua_State *L) {
  return luaL_error(L, "Not yet implemented");
}

static int prailude_pack_block(lua_State *L) {
  char               buf[512];
  size_t             len;
  rai_block_type_t   blocktype;
#if LUA_VERSION_NUM > 501
  luaL_checktype(L, 1, LUA_TTABLE);
#else
  lua_checktype(L, 1, LUA_TTABLE);
#endif
  lua_getfield(L, 1, "typecode");
  lua_pushvalue(L, 1); 
  lua_call(L, 1, 1); //block:typecode()
  blocktype = lua_tonumber(L, -1);
  lua_pop(L, 1);
  
  lua_inspect_stack(L, "encoding block");
  
  len = block_pack_encode(blocktype, L, buf, 512);
  if(len == 0) {
    luaL_error(L, "failed to encode block for some reason");
  }
  lua_pushlstring(L, buf, len);
  return 1;
}

static int prailude_unpack_block(lua_State *L) {
  const char          *raw;
  size_t               sz;
  size_t               bytes_read;
  const char          *err = NULL;
  rai_block_type_t     blocktype;
  blocktype = luaL_checkinteger(L, 1);
  if(blocktype < 2 || blocktype > 5) {
    luaL_error(L, "invalid block type code %i", blocktype);
  }
  raw = luaL_checklstring(L, 2, &sz);
  
  bytes_read = block_decode_unpack(blocktype, L, raw, sz, &err);
  if(bytes_read == 0) {
    luaL_error(L, "Failed to unpack block: %s", err != NULL ? err : "incomplete block data");
  }
  return 1;
}

static const struct luaL_Reg prailude_parser_functions[] = {
  { "pack_message", prailude_pack_message },
  { "unpack_message", prailude_unpack_message },
  
  { "pack_block", prailude_pack_block },
  { "unpack_block", prailude_unpack_block },
  
  { "unpack_frontiers", prailude_unpack_frontiers },
  { "pack_frontiers", prailude_pack_frontiers },
  
  // { "parse_bulk_stream", prailude_parse_bulk_stream },
  { NULL, NULL }
};

int luaopen_prailude_util_parser(lua_State* lua) {
  lua_newtable(lua);
#if LUA_VERSION_NUM > 501
  luaL_setfuncs(lua,prailude_parser_functions,0);
#else
  luaL_register(lua, NULL, prailude_parser_functions);
#endif
  return 1;
}
