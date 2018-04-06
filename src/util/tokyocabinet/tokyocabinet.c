/*************************************************************************************************
 * Lua binding of Tokyo Cabinet
 *                                                               Copyright (C) 2006-2010 FAL Labs
 * This file is part of Tokyo Cabinet.
 * Tokyo Cabinet is free software; you can redistribute it and/or modify it under the terms of
 * the GNU Lesser General Public License as published by the Free Software Foundation; either
 * version 2.1 of the License or any later version.  Tokyo Cabinet is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * You should have received a copy of the GNU Lesser General Public License along with Tokyo
 * Cabinet; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307 USA.
 *************************************************************************************************/


#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"

#include <string.h>
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

#if LUA_VERSION_NUM > 501
#include <lauxlib.h>
#define lua_objlen(L, index) luaL_len(L, index)
#endif

#include "tcutil.h"
#include "tchdb.h"
#include "tcbdb.h"
#include "tcfdb.h"
#include "tctdb.h"
#include "tcadb.h"
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#define SWAB16(TC_num) \
  ( \
   ((TC_num & 0x00ffU) << 8) | \
   ((TC_num & 0xff00U) >> 8) \
  )

#define SWAB32(TC_num) \
  ( \
   ((TC_num & 0x000000ffUL) << 24) | \
   ((TC_num & 0x0000ff00UL) << 8) | \
   ((TC_num & 0x00ff0000UL) >> 8) | \
   ((TC_num & 0xff000000UL) >> 24) \
  )

#define SWAB64(TC_num) \
  ( \
   ((TC_num & 0x00000000000000ffULL) << 56) | \
   ((TC_num & 0x000000000000ff00ULL) << 40) | \
   ((TC_num & 0x0000000000ff0000ULL) << 24) | \
   ((TC_num & 0x00000000ff000000ULL) << 8) | \
   ((TC_num & 0x000000ff00000000ULL) >> 8) | \
   ((TC_num & 0x0000ff0000000000ULL) >> 24) | \
   ((TC_num & 0x00ff000000000000ULL) >> 40) | \
   ((TC_num & 0xff00000000000000ULL) >> 56) \
  )

#if defined(_MYBIGEND) || defined(_MYSWAB)
#define HTONS(TC_num)   (TC_num)
#define HTONL(TC_num)   (TC_num)
#define HTONLL(TC_num)  (TC_num)
#define NTOHS(TC_num)   (TC_num)
#define NTOHL(TC_num)   (TC_num)
#define NTOHLL(TC_num)  (TC_num)
#else
#define HTONS(TC_num)   SWAB16(TC_num)
#define HTONL(TC_num)   SWAB32(TC_num)
#define HTONLL(TC_num)  SWAB64(TC_num)
#define NTOHS(TC_num)   SWAB16(TC_num)
#define NTOHL(TC_num)   SWAB32(TC_num)
#define NTOHLL(TC_num)  SWAB64(TC_num)
#endif

#define NUMBUFSIZ       32
#define HDBDATAVAR      "_hdbdata_"
#define BDBDATAVAR      "_bdbdata_"
#define BDBCURDATAVAR   "_bdbcurdata_"
#define FDBDATAVAR      "_fdbdata_"
#define TDBDATAVAR      "_tdbdata_"
#define TDBQRYDATAVAR   "_tdbqrydata_"
#define ADBDATAVAR      "_adbdata_"

typedef struct {
  lua_State *lua;
  char *fname;
} FUNCOP;

typedef struct {
  TCHDB *hdb;
} HDBDATA;

typedef struct {
  TCBDB *bdb;
} BDBDATA;

typedef struct {
  BDBCUR *cur;
} BDBCURDATA;

typedef struct {
  TCFDB *fdb;
} FDBDATA;

typedef struct {
  TCTDB *tdb;
} TDBDATA;

typedef struct {
  TDBQRY *qry;
} TDBQRYDATA;

typedef struct {
  TCADB *adb;
} ADBDATA;


/* function prototypes */
int luaopen_tokyocabinet(lua_State *lua);
static TCLIST *tabletotclist(lua_State *lua, int index);
static void tclisttotable(lua_State *lua, TCLIST *list);
static TCMAP *tabletotcmap(lua_State *lua, int index);
static void tcmaptotable(lua_State *lua, TCMAP *map);

static void util_init(lua_State *lua);
static int util_tablenew(lua_State *lua);
static int util_pack(lua_State *lua);
static int util_unpack(lua_State *lua);
static int util_split(lua_State *lua);
static int util_codec(lua_State *lua);
static int util_hash(lua_State *lua);
static int util_bit(lua_State *lua);
static int util_strstr(lua_State *lua);
static int util_regex(lua_State *lua);
static int util_ucs(lua_State *lua);
static int util_dist(lua_State *lua);
static int util_isect(lua_State *lua);
static int util_union(lua_State *lua);
static int util_time(lua_State *lua);
static int util_sleep(lua_State *lua);
static int util_stat(lua_State *lua);
static int util_glob(lua_State *lua);
static int util_remove(lua_State *lua);
static int util_mkdir(lua_State *lua);
static int util_chdir(lua_State *lua);
static bool util_iterrec(const void *kbuf, int ksiz, const void *vbuf, int vsiz, FUNCOP *funcop);
static int util_cmpobj(const char *aptr, int asiz, const char *bptr, int bsiz, FUNCOP *funcop);

static void hdb_init(lua_State *lua);
static int hdb_new(lua_State *lua);
static int hdb_del(lua_State *lua);
static int hdb_errmsg(lua_State *lua);
static int hdb_ecode(lua_State *lua);
static int hdb_tune(lua_State *lua);
static int hdb_setcache(lua_State *lua);
static int hdb_setxmsiz(lua_State *lua);
static int hdb_setdfunit(lua_State *lua);
static int hdb_open(lua_State *lua);
static int hdb_close(lua_State *lua);
static int hdb_put(lua_State *lua);
static int hdb_putkeep(lua_State *lua);
static int hdb_putcat(lua_State *lua);
static int hdb_putasync(lua_State *lua);
static int hdb_out(lua_State *lua);
static int hdb_get(lua_State *lua);
static int hdb_vsiz(lua_State *lua);
static int hdb_iterinit(lua_State *lua);
static int hdb_iternext(lua_State *lua);
static int hdb_fwmkeys(lua_State *lua);
static int hdb_addint(lua_State *lua);
static int hdb_adddouble(lua_State *lua);
static int hdb_sync(lua_State *lua);
static int hdb_optimize(lua_State *lua);
static int hdb_vanish(lua_State *lua);
static int hdb_copy(lua_State *lua);
static int hdb_tranbegin(lua_State *lua);
static int hdb_trancommit(lua_State *lua);
static int hdb_tranabort(lua_State *lua);
static int hdb_path(lua_State *lua);
static int hdb_rnum(lua_State *lua);
static int hdb_fsiz(lua_State *lua);
static int hdb_foreach(lua_State *lua);
static int hdb_pairs(lua_State *lua);
static int hdb_next(lua_State *lua);

static void bdb_init(lua_State *lua);
static int bdb_new(lua_State *lua);
static int bdb_del(lua_State *lua);
static int bdb_errmsg(lua_State *lua);
static int bdb_ecode(lua_State *lua);
static int bdb_setcmpfunc(lua_State *lua);
static int bdb_tune(lua_State *lua);
static int bdb_setcache(lua_State *lua);
static int bdb_setxmsiz(lua_State *lua);
static int bdb_setdfunit(lua_State *lua);
static int bdb_open(lua_State *lua);
static int bdb_close(lua_State *lua);
static int bdb_put(lua_State *lua);
static int bdb_putkeep(lua_State *lua);
static int bdb_putcat(lua_State *lua);
static int bdb_putdup(lua_State *lua);
static int bdb_putlist(lua_State *lua);
static int bdb_out(lua_State *lua);
static int bdb_outlist(lua_State *lua);
static int bdb_get(lua_State *lua);
static int bdb_getlist(lua_State *lua);
static int bdb_vnum(lua_State *lua);
static int bdb_vsiz(lua_State *lua);
static int bdb_range(lua_State *lua);
static int bdb_fwmkeys(lua_State *lua);
static int bdb_addint(lua_State *lua);
static int bdb_adddouble(lua_State *lua);
static int bdb_sync(lua_State *lua);
static int bdb_optimize(lua_State *lua);
static int bdb_vanish(lua_State *lua);
static int bdb_copy(lua_State *lua);
static int bdb_tranbegin(lua_State *lua);
static int bdb_trancommit(lua_State *lua);
static int bdb_tranabort(lua_State *lua);
static int bdb_path(lua_State *lua);
static int bdb_rnum(lua_State *lua);
static int bdb_fsiz(lua_State *lua);
static int bdb_foreach(lua_State *lua);
static int bdb_pairs(lua_State *lua);
static int bdb_next(lua_State *lua);

static void bdbcur_init(lua_State *lua);
static int bdbcur_new(lua_State *lua);
static int bdbcur_del(lua_State *lua);
static int bdbcur_first(lua_State *lua);
static int bdbcur_last(lua_State *lua);
static int bdbcur_jump(lua_State *lua);
static int bdbcur_prev(lua_State *lua);
static int bdbcur_next(lua_State *lua);
static int bdbcur_put(lua_State *lua);
static int bdbcur_out(lua_State *lua);
static int bdbcur_key(lua_State *lua);
static int bdbcur_val(lua_State *lua);

static void fdb_init(lua_State *lua);
static int fdb_new(lua_State *lua);
static int fdb_del(lua_State *lua);
static int fdb_errmsg(lua_State *lua);
static int fdb_ecode(lua_State *lua);
static int fdb_tune(lua_State *lua);
static int fdb_open(lua_State *lua);
static int fdb_close(lua_State *lua);
static int fdb_put(lua_State *lua);
static int fdb_putkeep(lua_State *lua);
static int fdb_putcat(lua_State *lua);
static int fdb_out(lua_State *lua);
static int fdb_get(lua_State *lua);
static int fdb_vsiz(lua_State *lua);
static int fdb_iterinit(lua_State *lua);
static int fdb_iternext(lua_State *lua);
static int fdb_range(lua_State *lua);
static int fdb_addint(lua_State *lua);
static int fdb_adddouble(lua_State *lua);
static int fdb_sync(lua_State *lua);
static int fdb_optimize(lua_State *lua);
static int fdb_vanish(lua_State *lua);
static int fdb_copy(lua_State *lua);
static int fdb_tranbegin(lua_State *lua);
static int fdb_trancommit(lua_State *lua);
static int fdb_tranabort(lua_State *lua);
static int fdb_path(lua_State *lua);
static int fdb_rnum(lua_State *lua);
static int fdb_fsiz(lua_State *lua);
static int fdb_foreach(lua_State *lua);
static int fdb_pairs(lua_State *lua);
static int fdb_next(lua_State *lua);

static void tdb_init(lua_State *lua);
static int tdb_new(lua_State *lua);
static int tdb_del(lua_State *lua);
static int tdb_errmsg(lua_State *lua);
static int tdb_ecode(lua_State *lua);
static int tdb_tune(lua_State *lua);
static int tdb_setcache(lua_State *lua);
static int tdb_setxmsiz(lua_State *lua);
static int tdb_setdfunit(lua_State *lua);
static int tdb_open(lua_State *lua);
static int tdb_close(lua_State *lua);
static int tdb_put(lua_State *lua);
static int tdb_putkeep(lua_State *lua);
static int tdb_putcat(lua_State *lua);
static int tdb_out(lua_State *lua);
static int tdb_get(lua_State *lua);
static int tdb_vsiz(lua_State *lua);
static int tdb_iterinit(lua_State *lua);
static int tdb_iternext(lua_State *lua);
static int tdb_fwmkeys(lua_State *lua);
static int tdb_addint(lua_State *lua);
static int tdb_adddouble(lua_State *lua);
static int tdb_sync(lua_State *lua);
static int tdb_optimize(lua_State *lua);
static int tdb_vanish(lua_State *lua);
static int tdb_copy(lua_State *lua);
static int tdb_tranbegin(lua_State *lua);
static int tdb_trancommit(lua_State *lua);
static int tdb_tranabort(lua_State *lua);
static int tdb_path(lua_State *lua);
static int tdb_rnum(lua_State *lua);
static int tdb_fsiz(lua_State *lua);
static int tdb_setindex(lua_State *lua);
static int tdb_genuid(lua_State *lua);
static int tdb_foreach(lua_State *lua);
static int tdb_pairs(lua_State *lua);
static int tdb_next(lua_State *lua);

static void tdbqry_init(lua_State *lua);
static int tdbqry_procrec(const void *pkbuf, int pksiz, TCMAP *cols, FUNCOP *funcop);
static int tdbqry_new(lua_State *lua);
static int tdbqry_del(lua_State *lua);
static int tdbqry_addcond(lua_State *lua);
static int tdbqry_setorder(lua_State *lua);
static int tdbqry_setlimit(lua_State *lua);
static int tdbqry_search(lua_State *lua);
static int tdbqry_searchout(lua_State *lua);
static int tdbqry_proc(lua_State *lua);
static int tdbqry_hint(lua_State *lua);
static int tdbqry_metasearch(lua_State *lua);
static int tdbqry_kwic(lua_State *lua);

static void adb_init(lua_State *lua);
static int adb_new(lua_State *lua);
static int adb_del(lua_State *lua);
static int adb_open(lua_State *lua);
static int adb_close(lua_State *lua);
static int adb_put(lua_State *lua);
static int adb_putkeep(lua_State *lua);
static int adb_putcat(lua_State *lua);
static int adb_out(lua_State *lua);
static int adb_get(lua_State *lua);
static int adb_vsiz(lua_State *lua);
static int adb_iterinit(lua_State *lua);
static int adb_iternext(lua_State *lua);
static int adb_fwmkeys(lua_State *lua);
static int adb_addint(lua_State *lua);
static int adb_adddouble(lua_State *lua);
static int adb_sync(lua_State *lua);
static int adb_optimize(lua_State *lua);
static int adb_vanish(lua_State *lua);
static int adb_copy(lua_State *lua);
static int adb_tranbegin(lua_State *lua);
static int adb_trancommit(lua_State *lua);
static int adb_tranabort(lua_State *lua);
static int adb_path(lua_State *lua);
static int adb_rnum(lua_State *lua);
static int adb_size(lua_State *lua);
static int adb_foreach(lua_State *lua);
static int adb_pairs(lua_State *lua);
static int adb_next(lua_State *lua);


/* initialization when the library is loaded */
int luaopen_tokyocabinet(lua_State *lua){
  lua_newtable(lua);
  util_init(lua);
  hdb_init(lua);
  bdb_init(lua);
  bdbcur_init(lua);
  fdb_init(lua);
  tdb_init(lua);
  tdbqry_init(lua);
  adb_init(lua);
  return 1;
}


/* convert a table of Lua into a list object of TC */
static TCLIST *tabletotclist(lua_State *lua, int index){
  int len = lua_objlen(lua, index);
  TCLIST *list = tclistnew2(len);
  for(int i = 1; i <= len; i++){
    lua_rawgeti(lua, index, i);
    size_t vsiz;
    const char *vbuf = lua_tolstring(lua, -1, &vsiz);
    if(vbuf) tclistpush(list, vbuf, vsiz);
    lua_pop(lua, 1);
  }
  return list;
}


/* convert a list object of TC into a table of Lua */
static void tclisttotable(lua_State *lua, TCLIST *list){
  int num = tclistnum(list);
  lua_createtable(lua, num, 0);
  for(int i = 0; i < num; i++){
    int size;
    const char *buf = tclistval(list, i, &size);
    lua_pushlstring(lua, buf, size);
    lua_rawseti(lua, -2, i + 1);
  }
}


/* convert a table of Lua into a map object of TC */
static TCMAP *tabletotcmap(lua_State *lua, int index){
  TCMAP *map = tcmapnew2(31);
  char knbuf[NUMBUFSIZ], vnbuf[NUMBUFSIZ];
  lua_pushnil(lua);
  while(lua_next(lua, index) != 0){
    const char *kbuf = NULL;
    size_t ksiz = 0;
    switch(lua_type(lua, -2)){
    case LUA_TNUMBER:
      ksiz = sprintf(knbuf, "%lld", (long long)lua_tonumber(lua, -2));
      kbuf = knbuf;
      break;
    case LUA_TSTRING:
      kbuf = lua_tolstring(lua, -2, &ksiz);
      break;
    }
    if(kbuf){
      const char *vbuf = NULL;
      size_t vsiz = 0;
      switch(lua_type(lua, -1)){
      case LUA_TNUMBER:
        vsiz = sprintf(vnbuf, "%lld", (long long)lua_tonumber(lua, -1));
        if(vsiz > sizeof(vnbuf)) vsiz = sizeof(vnbuf);
        vbuf = vnbuf;
        break;
      case LUA_TSTRING:
        vbuf = lua_tolstring(lua, -1, &vsiz);
        break;
      }
      if(vbuf) tcmapput(map, kbuf, ksiz, vbuf, vsiz);
    }
    lua_pop(lua, 1);
  }
  lua_pop(lua, 1);
  return map;
}


/* convert a map object of TC into a table of Lua */
static void tcmaptotable(lua_State *lua, TCMAP *map){
  int num = tcmaprnum(map);
  lua_createtable(lua, 0, num);
  tcmapiterinit(map);
  const char *kbuf;
  int ksiz;
  while((kbuf = tcmapiternext(map, &ksiz)) != NULL){
    int vsiz;
    const char *vbuf = tcmapiterval(kbuf, &vsiz);
    lua_pushlstring(lua, vbuf, vsiz);
    lua_setfield(lua, -2, kbuf);
  }
}


static const struct luaL_Reg util_functions[] = {
  {"tablenew", util_tablenew},
  {"pack", util_pack},
  {"unpack", util_unpack},
  {"split", util_split},
  {"codec", util_codec},
  {"hash", util_hash},
  {"bit", util_bit},
  {"strstr", util_strstr},
  {"regex", util_regex},
  {"ucs", util_ucs},
  {"dist", util_dist},
  {"isect", util_isect},
  {"union", util_union},
  {"time", util_time},
  {"sleep", util_sleep},
  {"stat", util_stat},
  {"glob", util_glob},
  {"remove", util_remove},
  {"mkdir", util_mkdir},
  {"chdir", util_chdir},
  {NULL, NULL} 
};

/* initialization of utility */
static void util_init(lua_State *lua) {
  
  lua_pushstring(lua, tcversion);
  lua_setfield(lua, -2, "version");
  
#if LUA_VERSION_NUM > 501
  luaL_setfuncs(lua,util_functions,0);
#else
  luaL_register(lua, NULL, util_functions);
#endif
}


/* for tablenew function */
static int util_tablenew(lua_State *lua){
  int argc = lua_gettop(lua);
  int anum = argc > 0 ? lua_tointeger(lua, 1) : 0;
  int rnum = argc > 1 ? lua_tointeger(lua, 2) : 0;
  if(anum < 0) anum = 0;
  if(rnum < 0) rnum = 0;
  lua_settop(lua, 0);
  lua_createtable(lua, anum, rnum);
  return 1;
}


/* for pack function */
static int util_pack(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1){
    lua_pushstring(lua, "pack: invalid arguments");
    lua_error(lua);
  }
  const char *format = lua_tostring(lua, 1);
  if(!format){
    lua_pushstring(lua, "pack: invalid arguments");
    lua_error(lua);
  }
  lua_newtable(lua);
  int aidx = argc + 1;
  int eidx = 1;
  for(int i = 2; i <= argc; i++){
    int len;
    switch(lua_type(lua, i)){
    case LUA_TNUMBER:
    case LUA_TSTRING:
      lua_pushvalue(lua, i);
      lua_rawseti(lua, aidx, eidx++);
      break;
    case LUA_TTABLE:
      len = lua_objlen(lua, i);
      for(int j = 1; j <= len; j++){
        lua_rawgeti(lua, i, j);
        lua_rawseti(lua, aidx, eidx++);
      }
      break;
    default:
      lua_pushnumber(lua, 0);
      lua_rawseti(lua, aidx, eidx++);
      break;
    }
  }
  lua_replace(lua, 2);
  lua_settop(lua, 2);
  TCXSTR *xstr = tcxstrnew();
  int emax = eidx - 1;
  eidx = 1;
  while(*format != '\0'){
    int c = *format;
    int loop = 1;
    if(format[1] == '*'){
      loop = INT_MAX;
      format++;
    } else if(format[1] >= '0' && format[1] <= '9'){
      char *suffix;
      loop = strtol(format + 1, &suffix, 10);
      format = suffix - 1;
    }
    loop = tclmin(loop, emax);
    int end = tclmin(eidx + loop - 1, emax);
    while(eidx <= end){
      lua_rawgeti(lua, 2, eidx);
      double num = lua_tonumber(lua, 3);
      lua_pop(lua, 1);
      uint8_t cnum;
      uint16_t snum;
      uint32_t inum;
      uint64_t lnum;
      double dnum;
      float fnum;
      uint64_t wnum;
      char wbuf[NUMBUFSIZ], *wp;
      switch(c){
      case 'c':
      case 'C':
        cnum = num;
        tcxstrcat(xstr, &cnum, sizeof(cnum));
        break;
      case 's':
      case 'S':
        snum = num;
        tcxstrcat(xstr, &snum, sizeof(snum));
        break;
      case 'i':
      case 'I':
        inum = num;
        tcxstrcat(xstr, &inum, sizeof(inum));
        break;
      case 'l':
      case 'L':
        lnum = num;
        tcxstrcat(xstr, &lnum, sizeof(lnum));
        break;
      case 'f':
      case 'F':
        fnum = num;
        tcxstrcat(xstr, &fnum, sizeof(fnum));
        break;
      case 'd':
      case 'D':
        dnum = num;
        tcxstrcat(xstr, &dnum, sizeof(dnum));
        break;
      case 'n':
        snum = num;
        snum = HTONS(snum);
        tcxstrcat(xstr, &snum, sizeof(snum));
        break;
      case 'N':
        inum = num;
        inum = HTONL(inum);
        tcxstrcat(xstr, &inum, sizeof(inum));
        break;
      case 'M':
        lnum = num;
        lnum = HTONLL(lnum);
        tcxstrcat(xstr, &lnum, sizeof(lnum));
        break;
      case 'w':
      case 'W':
        wnum = num;
        wp = wbuf;
        if(wnum < (1ULL << 7)){
          *(wp++) = wnum;
        } else if(wnum < (1ULL << 14)){
          *(wp++) = (wnum >> 7) | 0x80;
          *(wp++) = wnum & 0x7f;
        } else if(wnum < (1ULL << 21)){
          *(wp++) = (wnum >> 14) | 0x80;
          *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
          *(wp++) = wnum & 0x7f;
        } else if(wnum < (1ULL << 28)){
          *(wp++) = (wnum >> 21) | 0x80;
          *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
          *(wp++) = wnum & 0x7f;
        } else if(wnum < (1ULL << 35)){
          *(wp++) = (wnum >> 28) | 0x80;
          *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
          *(wp++) = wnum & 0x7f;
        } else if(wnum < (1ULL << 42)){
          *(wp++) = (wnum >> 35) | 0x80;
          *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
          *(wp++) = wnum & 0x7f;
        } else if(wnum < (1ULL << 49)){
          *(wp++) = (wnum >> 42) | 0x80;
          *(wp++) = ((wnum >> 35) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
          *(wp++) = wnum & 0x7f;
        } else if(wnum < (1ULL << 56)){
          *(wp++) = (wnum >> 49) | 0x80;
          *(wp++) = ((wnum >> 42) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 35) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
          *(wp++) = wnum & 0x7f;
        } else {
          *(wp++) = (wnum >> 63) | 0x80;
          *(wp++) = ((wnum >> 49) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 42) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 35) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
          *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
          *(wp++) = wnum & 0x7f;
        }
        tcxstrcat(xstr, wbuf, wp - wbuf);
        break;
      }
      eidx++;
    }
    format++;
    if(eidx > emax) break;
  }
  lua_settop(lua, 0);
  lua_pushlstring(lua, tcxstrptr(xstr), tcxstrsize(xstr));
  tcxstrdel(xstr);
  return 1;
}


/* for unpack function */
static int util_unpack(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "unpack: invalid arguments");
    lua_error(lua);
  }
  const char *format = lua_tostring(lua, 1);
  size_t size;
  const char *buf = lua_tolstring(lua, 2, &size);
  if(!format){
    lua_pushstring(lua, "unpack: invalid arguments");
    lua_error(lua);
  }
  if(!buf){
    buf = "";
    size = 0;
  }
  lua_newtable(lua);
  const char *rp = buf;
  int eidx = 1;
  while(*format != '\0'){
    int c = *format;
    int loop = 1;
    if(format[1] == '*'){
      loop = INT_MAX;
      format++;
    } else if(format[1] >= '0' && format[1] <= '9'){
      char *suffix;
      loop = strtol(format + 1, &suffix, 10);
      format = suffix - 1;
    }
    loop = tclmin(loop, size);
    for(int i = 0; i < loop && size > 0; i++){
      uint8_t cnum;
      uint16_t snum;
      uint32_t inum;
      uint64_t lnum;
      float fnum;
      double dnum;
      uint64_t wnum;
      switch(c){
      case 'c':
        if(size >= sizeof(cnum)){
          memcpy(&cnum, rp, sizeof(cnum));
          lua_pushnumber(lua, (int8_t)cnum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(cnum);
          size -= sizeof(cnum);
        } else {
          size = 0;
        }
        break;
      case 'C':
        if(size >= sizeof(cnum)){
          memcpy(&cnum, rp, sizeof(cnum));
          lua_pushnumber(lua, (uint8_t)cnum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(cnum);
          size -= sizeof(cnum);
        } else {
          size = 0;
        }
        break;
      case 's':
        if(size >= sizeof(snum)){
          memcpy(&snum, rp, sizeof(snum));
          lua_pushnumber(lua, (int16_t)snum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(snum);
          size -= sizeof(snum);
        } else {
          size = 0;
        }
        break;
      case 'S':
        if(size >= sizeof(snum)){
          memcpy(&snum, rp, sizeof(snum));
          lua_pushnumber(lua, (uint16_t)snum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(snum);
          size -= sizeof(snum);
        } else {
          size = 0;
        }
        break;
      case 'i':
        if(size >= sizeof(inum)){
          memcpy(&inum, rp, sizeof(inum));
          lua_pushnumber(lua, (int32_t)inum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(inum);
          size -= sizeof(inum);
        } else {
          size = 0;
        }
        break;
      case 'I':
        if(size >= sizeof(inum)){
          memcpy(&inum, rp, sizeof(inum));
          lua_pushnumber(lua, (uint32_t)inum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(inum);
          size -= sizeof(inum);
        } else {
          size = 0;
        }
        break;
      case 'l':
        if(size >= sizeof(lnum)){
          memcpy(&lnum, rp, sizeof(lnum));
          lua_pushnumber(lua, (int64_t)lnum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(lnum);
          size -= sizeof(lnum);
        } else {
          size = 0;
        }
        break;
      case 'L':
        if(size >= sizeof(lnum)){
          memcpy(&lnum, rp, sizeof(lnum));
          lua_pushnumber(lua, (uint64_t)lnum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(lnum);
          size -= sizeof(lnum);
        } else {
          size = 0;
        }
        break;
      case 'f':
      case 'F':
        if(size >= sizeof(fnum)){
          memcpy(&fnum, rp, sizeof(fnum));
          lua_pushnumber(lua, (float)fnum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(fnum);
          size -= sizeof(fnum);
        } else {
          size = 0;
        }
        break;
      case 'd':
      case 'D':
        if(size >= sizeof(dnum)){
          memcpy(&dnum, rp, sizeof(dnum));
          lua_pushnumber(lua, (double)dnum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(dnum);
          size -= sizeof(dnum);
        } else {
          size = 0;
        }
        break;
      case 'n':
        if(size >= sizeof(snum)){
          memcpy(&snum, rp, sizeof(snum));
          snum = NTOHS(snum);
          lua_pushnumber(lua, (uint16_t)snum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(snum);
          size -= sizeof(snum);
        } else {
          size = 0;
        }
        break;
      case 'N':
        if(size >= sizeof(inum)){
          memcpy(&inum, rp, sizeof(inum));
          inum = NTOHL(inum);
          lua_pushnumber(lua, (uint32_t)inum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(inum);
          size -= sizeof(inum);
        } else {
          size = 0;
        }
        break;
      case 'M':
        if(size >= sizeof(lnum)){
          memcpy(&lnum, rp, sizeof(lnum));
          lnum = NTOHLL(lnum);
          lua_pushnumber(lua, (uint64_t)lnum);
          lua_rawseti(lua, 3, eidx++);
          rp += sizeof(lnum);
          size -= sizeof(lnum);
        } else {
          size = 0;
        }
        break;
      case 'w':
      case 'W':
        wnum = 0;
        do {
          inum = *(unsigned char *)rp;
          wnum = wnum * 0x80 + (inum & 0x7f);
          rp++;
          size--;
        } while(inum >= 0x80 && size > 0);
        lua_pushnumber(lua, (uint64_t)wnum);
        lua_rawseti(lua, 3, eidx++);
        break;
      }
    }
    format++;
    if(size < 1) break;
  }
  lua_replace(lua, 1);
  lua_settop(lua, 1);
  return 1;
}


/* for split function */
static int util_split(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1){
    lua_pushstring(lua, "split: invalid arguments");
    lua_error(lua);
  }
  size_t isiz;
  const char *ibuf = lua_tolstring(lua, 1, &isiz);
  if(!ibuf){
    lua_pushstring(lua, "split: invalid arguments");
    lua_error(lua);
  }
  const char *delims = argc > 1 ? lua_tostring(lua, 2) : NULL;
  lua_newtable(lua);
  int lnum = 1;
  if(delims){
    const char *str = ibuf;
    while(true){
      const char *sp = str;
      while(*str != '\0' && !strchr(delims, *str)){
        str++;
      }
      lua_pushlstring(lua, sp, str - sp);
      lua_rawseti(lua, -2, lnum++);
      if(*str == '\0') break;
      str++;
    }
  } else {
    const char *ptr = ibuf;
    int size = isiz;
    while(size >= 0){
      const char *rp = ptr;
      const char *ep = ptr + size;
      while(rp < ep){
        if(*rp == '\0') break;
        rp++;
      }
      lua_pushlstring(lua, ptr, rp - ptr);
      lua_rawseti(lua, -2, lnum++);
      rp++;
      size -= rp - ptr;
      ptr = rp;
    }
  }
  lua_replace(lua, 1);
  lua_settop(lua, 1);
  return 1;
}


/* for codec function */
static int util_codec(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "codec: invalid arguments");
    lua_error(lua);
  }
  const char *mode = lua_tostring(lua, 1);
  size_t isiz;
  const char *ibuf = lua_tolstring(lua, 2, &isiz);
  if(!mode || !ibuf){
    lua_pushstring(lua, "codec: invalid arguments");
    lua_error(lua);
  }
  char *obuf = NULL;
  int osiz = 0;
  if(*mode == '~'){
    mode++;
    if(!tcstricmp(mode, "url")){
      obuf = tcurldecode(ibuf, &osiz);
    } else if(!tcstricmp(mode, "base")){
      obuf = tcbasedecode(ibuf, &osiz);
    } else if(!tcstricmp(mode, "quote")){
      obuf = tcquotedecode(ibuf, &osiz);
    } else if(!tcstricmp(mode, "hex")){
      obuf = tchexdecode(ibuf, &osiz);
    } else if(!tcstricmp(mode, "pack")){
      obuf = tcpackdecode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "tcbs")){
      obuf = tcbsdecode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "deflate")){
      obuf = tcinflate(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "gzip")){
      obuf = tcgzipdecode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "bzip")){
      obuf = tcbzipdecode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "xml")){
      obuf = tcxmlunescape(ibuf);
      osiz = obuf ? strlen(obuf) : 0;
    }
  } else {
    if(!tcstricmp(mode, "url")){
      obuf = tcurlencode(ibuf, isiz);
      osiz = obuf ? strlen(obuf) : 0;
    } else if(!tcstricmp(mode, "base")){
      obuf = tcbaseencode(ibuf, isiz);
      osiz = obuf ? strlen(obuf) : 0;
    } else if(!tcstricmp(mode, "quote")){
      obuf = tcquoteencode(ibuf, isiz);
      osiz = obuf ? strlen(obuf) : 0;
    } else if(!tcstricmp(mode, "hex")){
      obuf = tchexencode(ibuf, isiz);
      osiz = obuf ? strlen(obuf) : 0;
    } else if(!tcstricmp(mode, "pack")){
      obuf = tcpackencode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "tcbs")){
      obuf = tcbsencode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "deflate")){
      obuf = tcdeflate(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "gzip")){
      obuf = tcgzipencode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "bzip")){
      obuf = tcbzipencode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "xml")){
      obuf = tcxmlescape(ibuf);
      osiz = obuf ? strlen(obuf) : 0;
    }
  }
  lua_settop(lua, 0);
  if(obuf){
    lua_pushlstring(lua, obuf, osiz);
    tcfree(obuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for hash function */
static int util_hash(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "hash: invalid arguments");
    lua_error(lua);
  }
  const char *mode = lua_tostring(lua, 1);
  size_t isiz;
  const char *ibuf = lua_tolstring(lua, 2, &isiz);
  if(!mode || !ibuf){
    lua_pushstring(lua, "hash: invalid arguments");
    lua_error(lua);
  }
  if(!tcstricmp(mode, "md5")){
    char obuf[48];
    tcmd5hash(ibuf, isiz, obuf);
    lua_settop(lua, 0);
    lua_pushstring(lua, obuf);
  } else if(!tcstricmp(mode, "md5raw")){
    char obuf[48];
    tcmd5hash(ibuf, isiz, obuf);
    int esiz;
    char *ebuf = tchexdecode(obuf, &esiz);
    lua_settop(lua, 0);
    lua_pushlstring(lua, ebuf, esiz);
    tcfree(ebuf);
  } else if(!tcstricmp(mode, "crc32")){
    uint32_t crc = tcgetcrc(ibuf, isiz);
    lua_settop(lua, 0);
    lua_pushnumber(lua, crc);
  } else {
    lua_settop(lua, 0);
    lua_pushnil(lua);
  }
  return 1;
}


/* for bit function */
static int util_bit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2){
    lua_pushstring(lua, "bit: invalid arguments");
    lua_error(lua);
  }
  const char *mode = lua_tostring(lua, 1);
  uint32_t num = lua_tonumber(lua, 2);
  uint32_t aux = argc > 2 ? lua_tonumber(lua, 3) : 0;
  if(!mode){
    lua_pushstring(lua, "bit: invalid arguments");
    lua_error(lua);
  } else if(!tcstricmp(mode, "and")){
    num &= aux;
  } else if(!tcstricmp(mode, "or")){
    num |= aux;
  } else if(!tcstricmp(mode, "xor")){
    num ^= aux;
  } else if(!tcstricmp(mode, "not")){
    num = ~num;
  } else if(!tcstricmp(mode, "left")){
    num <<= aux;
  } else if(!tcstricmp(mode, "right")){
    num >>= aux;
  } else {
    lua_pushstring(lua, "bit: invalid arguments");
    lua_error(lua);
  }
  lua_settop(lua, 0);
  lua_pushnumber(lua, num);
  return 1;
}


/* for strstr function */
static int util_strstr(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2){
    lua_pushstring(lua, "strstr: invalid arguments");
    lua_error(lua);
  }
  const char *str = lua_tostring(lua, 1);
  const char *pat = lua_tostring(lua, 2);
  if(!str || !pat){
    lua_pushstring(lua, "strstr: invalid arguments");
    lua_error(lua);
  }
  const char *alt = argc > 2 ? lua_tostring(lua, 3) : NULL;
  if(alt){
    TCXSTR *xstr = tcxstrnew();
    int plen = strlen(pat);
    int alen = strlen(alt);
    if(plen > 0){
      char *pv;
      while((pv = strstr(str, pat)) != NULL){
        tcxstrcat(xstr, str, pv - str);
        tcxstrcat(xstr, alt, alen);
        str = pv + plen;
      }
    }
    tcxstrcat2(xstr, str);
    lua_settop(lua, 0);
    lua_pushstring(lua, tcxstrptr(xstr));
    tcxstrdel(xstr);
  } else {
    char *pv = strstr(str, pat);
    if(pv){
      int idx = pv - str + 1;
      lua_settop(lua, 0);
      lua_pushinteger(lua, idx);
    } else {
      lua_settop(lua, 0);
      lua_pushinteger(lua, 0);
    }
  }
  return 1;
}


/* for regex function */
static int util_regex(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2){
    lua_pushstring(lua, "regex: invalid arguments");
    lua_error(lua);
  }
  const char *str = lua_tostring(lua, 1);
  const char *regex = lua_tostring(lua, 2);
  if(!str || !regex){
    lua_pushstring(lua, "regex: invalid arguments");
    lua_error(lua);
  }
  const char *alt = argc > 2 ? lua_tostring(lua, 3) : NULL;
  if(alt){
    char *res = tcregexreplace(str, regex, alt);
    lua_settop(lua, 0);
    lua_pushstring(lua, res);
    tcfree(res);
  } else {
    if(tcregexmatch(str, regex)){
      lua_settop(lua, 0);
      lua_pushboolean(lua, true);
    } else {
      lua_settop(lua, 0);
      lua_pushboolean(lua, false);
    }
  }
  return 1;
}


/* for ucs function */
static int util_ucs(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "ucs: invalid arguments");
    lua_error(lua);
  }
  if(lua_type(lua, 1) == LUA_TTABLE){
    int anum = lua_objlen(lua, 1);
    uint16_t *ary = tcmalloc(sizeof(*ary) * anum + 1);
    for(int i = 1; i <= anum; i++){
      lua_rawgeti(lua, 1, i);
      ary[i-1] = lua_tointeger(lua, 2);
      lua_pop(lua, 1);
    }
    char *str = tcmalloc(anum * 3 + 1);
    tcstrucstoutf(ary, anum, str);
    lua_settop(lua, 0);
    lua_pushstring(lua, str);
    tcfree(str);
    tcfree(ary);
  } else {
    size_t len;
    const char *str = lua_tolstring(lua, 1, &len);
    if(!str){
      lua_pushstring(lua, "ucs: invalid arguments");
      lua_error(lua);
    }
    uint16_t *ary = tcmalloc(sizeof(*ary) * len + 1);
    int anum;
    tcstrutftoucs(str, ary, &anum);
    lua_settop(lua, 0);
    lua_createtable(lua, anum, 0);
    for(int i = 0; i < anum; i++){
      lua_pushinteger(lua, ary[i]);
      lua_rawseti(lua, 1, i + 1);
    }
    tcfree(ary);
  }
  return 1;
}


/* for dist function */
static int util_dist(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2){
    lua_pushstring(lua, "dist: invalid arguments");
    lua_error(lua);
  }
  const char *astr = lua_tostring(lua, 1);
  const char *bstr = lua_tostring(lua, 2);
  bool utf = argc > 2 ? lua_toboolean(lua, 3) : false;
  if(!astr || !bstr){
    lua_pushstring(lua, "dist: invalid arguments");
    lua_error(lua);
  }
  int rv = utf ? tcstrdistutf(astr, bstr) : tcstrdist(astr, bstr);
  lua_settop(lua, 0);
  lua_pushnumber(lua, rv);
  return 1;
}


/* for isect function */
static int util_isect(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc == 1 && lua_type(lua, 1) == LUA_TTABLE){
    int len = lua_objlen(lua, 1);
    for(int i = 1; i <= len; i++){
      lua_rawgeti(lua, 1, i);
      if(lua_type(lua, -1) == LUA_TTABLE){
        argc++;
      } else {
        lua_pop(lua, 1);
        break;
      }
    }
    if(argc > 1){
      lua_remove(lua, 1);
      argc--;
    }
  }
  int tnum = 0;
  int rnum = 0;
  for(int i = 1; i <= argc; i++){
    if(lua_type(lua, i) != LUA_TTABLE) continue;
    int len = lua_objlen(lua, i);
    if(len < 1){
      lua_settop(lua, 0);
      lua_newtable(lua);
      return 1;
    }
    tnum++;
    rnum += len;
  }
  if(tnum == 2){
    TCMAP *former = NULL;
    TCMAP *latter = NULL;
    for(int i = 1; i <= argc; i++){
      if(lua_type(lua, i) != LUA_TTABLE) continue;
      int len = lua_objlen(lua, i);
      if(former){
        latter = tcmapnew2(tclmin(len, tcmaprnum(former)));
        for(int j = 1; j <= len; j++){
          lua_rawgeti(lua, i, j);
          size_t size;
          const char *ptr = lua_tolstring(lua, -1, &size);
          if(ptr){
            int vsiz;
            if(tcmapget(former, ptr, size, &vsiz)) tcmapput(latter, ptr, size, "", 0);
          }
          lua_pop(lua, 1);
        }
        break;
      } else {
        former = tcmapnew2(len);
        for(int j = 1; j <= len; j++){
          lua_rawgeti(lua, i, j);
          size_t size;
          const char *ptr = lua_tolstring(lua, -1, &size);
          if(ptr) tcmapput(former, ptr, size, "", 0);
          lua_pop(lua, 1);
        }
      }
    }
    lua_settop(lua, 0);
    if(latter){
      lua_createtable(lua, (int)tcmaprnum(latter), 0);
      tcmapiterinit(latter);
      int ridx = 1;
      const char *kbuf;
      int ksiz;
      while((kbuf = tcmapiternext(latter, &ksiz)) != NULL){
        lua_pushlstring(lua, kbuf, ksiz);
        lua_rawseti(lua, 1, ridx++);
      }
      tcmapdel(latter);
    } else {
      lua_newtable(lua);
    }
    if(former) tcmapdel(former);
  } else {
    TCMAP *freq = tcmapnew2(rnum);
    for(int i = 1; i <= argc; i++){
      if(lua_type(lua, i) != LUA_TTABLE) continue;
      int len = lua_objlen(lua, i);
      TCMAP *uniq = tcmapnew2(len);
      for(int j = 1; j <= len; j++){
        lua_rawgeti(lua, i, j);
        size_t size;
        const char *ptr = lua_tolstring(lua, -1, &size);
        if(ptr){
          int vsiz;
          if(!tcmapget(uniq, ptr, size, &vsiz)){
            tcmapaddint(freq, ptr, size, 1);
            tcmapput(uniq, ptr, size, "", 0);
          }
        }
        lua_pop(lua, 1);
      }
      tcmapdel(uniq);
    }
    lua_settop(lua, 0);
    lua_createtable(lua, (int)tcmaprnum(freq), 0);
    tcmapiterinit(freq);
    int ridx = 1;
    const char *kbuf;
    int ksiz;
    while((kbuf = tcmapiternext(freq, &ksiz)) != NULL){
      int vsiz;
      const char *vbuf = tcmapiterval(kbuf, &vsiz);
      int num = *(int *)vbuf;
      if(num != tnum) continue;
      lua_pushlstring(lua, kbuf, ksiz);
      lua_rawseti(lua, 1, ridx++);
    }
    tcmapdel(freq);
  }
  return 1;
}


/* for union function */
static int util_union(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc == 1 && lua_type(lua, 1) == LUA_TTABLE){
    int len = lua_objlen(lua, 1);
    for(int i = 1; i <= len; i++){
      lua_rawgeti(lua, 1, i);
      if(lua_type(lua, -1) == LUA_TTABLE){
        argc++;
      } else {
        lua_pop(lua, 1);
        break;
      }
    }
    if(argc > 1){
      lua_remove(lua, 1);
      argc--;
    }
  }
  int rnum = 0;
  for(int i = 1; i <= argc; i++){
    if(lua_type(lua, i) != LUA_TTABLE) continue;
    rnum += lua_objlen(lua, i);
  }
  TCMAP *result = tcmapnew2(rnum);
  for(int i = 1; i <= argc; i++){
    if(lua_type(lua, i) != LUA_TTABLE) continue;
    int len = lua_objlen(lua, i);
    for(int j = 1; j <= len; j++){
      lua_rawgeti(lua, i, j);
      size_t size;
      const char *ptr = lua_tolstring(lua, -1, &size);
      if(ptr) tcmapput(result, ptr, size, "", 0);
      lua_pop(lua, 1);
    }
  }
  lua_settop(lua, 0);
  lua_createtable(lua, (int)tcmaprnum(result), 0);
  tcmapiterinit(result);
  int ridx = 1;
  const char *kbuf;
  int ksiz;
  while((kbuf = tcmapiternext(result, &ksiz)) != NULL){
    lua_pushlstring(lua, kbuf, ksiz);
    lua_rawseti(lua, 1, ridx++);
  }
  tcmapdel(result);
  return 1;
}


/* for time function */
static int util_time(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "time: invalid arguments");
    lua_error(lua);
  }
  lua_pushnumber(lua, tctime());
  return 1;
}


/* for sleep function */
static int util_sleep(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "sleep: invalid arguments");
    lua_error(lua);
  }
  double sec = lua_tonumber(lua, 1);
  if(!lua_isnumber(lua, 1)){
    lua_pushstring(lua, "sleep: invalid arguments");
    lua_error(lua);
  }
  lua_pushboolean(lua, tcsleep(sec));
  return 1;
}


/* for stat function */
static int util_stat(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "stat: invalid arguments");
    lua_error(lua);
  }
  const char *path = lua_tostring(lua, 1);
  if(!path){
    lua_pushstring(lua, "stat: invalid arguments");
    lua_error(lua);
  }
  struct stat sbuf;
  if(stat(path, &sbuf) == 0){
    lua_newtable(lua);
    lua_pushnumber(lua, sbuf.st_dev);
    lua_setfield(lua, -2, "dev");
    lua_pushnumber(lua, sbuf.st_ino);
    lua_setfield(lua, -2, "ino");
    lua_pushnumber(lua, sbuf.st_mode);
    lua_setfield(lua, -2, "mode");
    lua_pushnumber(lua, sbuf.st_nlink);
    lua_setfield(lua, -2, "nlink");
    lua_pushnumber(lua, sbuf.st_uid);
    lua_setfield(lua, -2, "uid");
    lua_pushnumber(lua, sbuf.st_gid);
    lua_setfield(lua, -2, "gid");
    lua_pushnumber(lua, sbuf.st_rdev);
    lua_setfield(lua, -2, "rdev");
    lua_pushnumber(lua, sbuf.st_size);
    lua_setfield(lua, -2, "size");
    lua_pushnumber(lua, sbuf.st_blksize);
    lua_setfield(lua, -2, "blksize");
    lua_pushnumber(lua, sbuf.st_blocks);
    lua_setfield(lua, -2, "blocks");
    lua_pushnumber(lua, sbuf.st_atime);
    lua_setfield(lua, -2, "atime");
    lua_pushnumber(lua, sbuf.st_mtime);
    lua_setfield(lua, -2, "mtime");
    lua_pushnumber(lua, sbuf.st_ctime);
    lua_setfield(lua, -2, "ctime");
    bool readable = false;
    bool writable = false;
    bool executable = false;
    if(sbuf.st_uid == geteuid()){
      if(sbuf.st_mode & S_IRUSR) readable = true;
      if(sbuf.st_mode & S_IWUSR) writable = true;
      if(sbuf.st_mode & S_IXUSR) executable = true;
    }
    if(sbuf.st_gid == getegid()){
      if(sbuf.st_mode & S_IRGRP) readable = true;
      if(sbuf.st_mode & S_IWGRP) writable = true;
      if(sbuf.st_mode & S_IXGRP) executable = true;
    }
    if(sbuf.st_mode & S_IROTH) readable = true;
    if(sbuf.st_mode & S_IWOTH) writable = true;
    if(sbuf.st_mode & S_IXOTH) executable = true;
    lua_pushboolean(lua, readable);
    lua_setfield(lua, -2, "_readable");
    lua_pushboolean(lua, writable);
    lua_setfield(lua, -2, "_writable");
    lua_pushboolean(lua, executable);
    lua_setfield(lua, -2, "_executable");
    char *rpath = tcrealpath(path);
    if(rpath){
      lua_pushstring(lua, rpath);
      lua_setfield(lua, -2, "_realpath");
      tcfree(rpath);
    }
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for glob function */
static int util_glob(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "glob: invalid arguments");
    lua_error(lua);
  }
  const char *pattern = lua_tostring(lua, 1);
  if(!pattern){
    lua_pushstring(lua, "glob: invalid arguments");
    lua_error(lua);
  }
  TCLIST *paths = tcglobpat(pattern);
  tclisttotable(lua, paths);
  tclistdel(paths);
  return 1;
}


/* for remove function */
static int util_remove(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "remove: invalid arguments");
    lua_error(lua);
  }
  const char *path = lua_tostring(lua, 1);
  if(!path){
    lua_pushstring(lua, "remove: invalid arguments");
    lua_error(lua);
  }
  if(tcremovelink(path)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for mkdir function */
static int util_mkdir(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "mkdir: invalid arguments");
    lua_error(lua);
  }
  const char *path = lua_tostring(lua, 1);
  if(!path){
    lua_pushstring(lua, "mkdir: invalid arguments");
    lua_error(lua);
  }
  if(mkdir(path, 00755) == 0){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for chdir function */
static int util_chdir(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "chdir: invalid arguments");
    lua_error(lua);
  }
  const char *path = lua_tostring(lua, 1);
  if(!path){
    lua_pushstring(lua, "chdir: invalid arguments");
    lua_error(lua);
  }
  if(chdir(path) == 0){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* call function for each record */
static bool util_iterrec(const void *kbuf, int ksiz, const void *vbuf, int vsiz, FUNCOP *funcop){
  lua_State *lua = funcop->lua;
  int top = lua_gettop(lua);
  lua_getglobal(lua, funcop->fname);
  lua_pushlstring(lua, kbuf, ksiz);
  lua_pushlstring(lua, vbuf, vsiz);
  bool rv = lua_pcall(lua, 2, 1, 0) == 0 ? lua_toboolean(lua, -1) : false;
  lua_settop(lua, top);
  return rv;
}


/* compare two keys calling a function of Lua */
static int util_cmpobj(const char *aptr, int asiz, const char *bptr, int bsiz, FUNCOP *funcop){
  lua_State *lua = funcop->lua;
  int top = lua_gettop(lua);
  lua_getglobal(lua, funcop->fname);
  lua_pushlstring(lua, aptr, asiz);
  lua_pushlstring(lua, bptr, bsiz);
  int rv = lua_pcall(lua, 2, 1, 0) == 0 ? lua_tointeger(lua, -1) : 0;
  lua_settop(lua, top);
  return rv;
}


/* initialization of hash database */
static void hdb_init(lua_State *lua){
  lua_pushcfunction(lua, hdb_new);
  lua_setfield(lua, -2, "hdbnew");
}


/* for new function */
static int hdb_new(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "hdbnew: invalid arguments");
    lua_error(lua);
  }
  lua_newtable(lua);
  HDBDATA *data = lua_newuserdata(lua, sizeof(*data));
  data->hdb = tchdbnew();
  tchdbsetmutex(data->hdb);
  lua_newtable(lua);
  lua_pushcfunction(lua, hdb_del);
  lua_setfield(lua, -2, "__gc");
  lua_setmetatable(lua, -2);
  lua_setfield(lua, -2, HDBDATAVAR);
  lua_pushnumber(lua, TCESUCCESS);
  lua_setfield(lua, -2, "ESUCCESS");
  lua_pushnumber(lua, TCETHREAD);
  lua_setfield(lua, -2, "ETHREAD");
  lua_pushnumber(lua, TCEINVALID);
  lua_setfield(lua, -2, "EINVALID");
  lua_pushnumber(lua, TCENOFILE);
  lua_setfield(lua, -2, "ENOFILE");
  lua_pushnumber(lua, TCENOPERM);
  lua_setfield(lua, -2, "ENOPERM");
  lua_pushnumber(lua, TCEMETA);
  lua_setfield(lua, -2, "EMETA");
  lua_pushnumber(lua, TCERHEAD);
  lua_setfield(lua, -2, "ERHEAD");
  lua_pushnumber(lua, TCEOPEN);
  lua_setfield(lua, -2, "EOPEN");
  lua_pushnumber(lua, TCECLOSE);
  lua_setfield(lua, -2, "ECLOSE");
  lua_pushnumber(lua, TCETRUNC);
  lua_setfield(lua, -2, "ETRUNC");
  lua_pushnumber(lua, TCESYNC);
  lua_setfield(lua, -2, "ESYNC");
  lua_pushnumber(lua, TCESTAT);
  lua_setfield(lua, -2, "ESTAT");
  lua_pushnumber(lua, TCESEEK);
  lua_setfield(lua, -2, "ESEEK");
  lua_pushnumber(lua, TCEREAD);
  lua_setfield(lua, -2, "EREAD");
  lua_pushnumber(lua, TCEWRITE);
  lua_setfield(lua, -2, "EWRITE");
  lua_pushnumber(lua, TCEMMAP);
  lua_setfield(lua, -2, "EMMAP");
  lua_pushnumber(lua, TCELOCK);
  lua_setfield(lua, -2, "ELOCK");
  lua_pushnumber(lua, TCEUNLINK);
  lua_setfield(lua, -2, "EUNLINK");
  lua_pushnumber(lua, TCERENAME);
  lua_setfield(lua, -2, "ERENAME");
  lua_pushnumber(lua, TCEMKDIR);
  lua_setfield(lua, -2, "EMKDIR");
  lua_pushnumber(lua, TCERMDIR);
  lua_setfield(lua, -2, "ERMDIR");
  lua_pushnumber(lua, TCEKEEP);
  lua_setfield(lua, -2, "EKEEP");
  lua_pushnumber(lua, TCENOREC);
  lua_setfield(lua, -2, "ENOREC");
  lua_pushnumber(lua, TCEMISC);
  lua_setfield(lua, -2, "EMISC");
  lua_pushnumber(lua, HDBTLARGE);
  lua_setfield(lua, -2, "TLARGE");
  lua_pushnumber(lua, HDBTDEFLATE);
  lua_setfield(lua, -2, "TDEFLATE");
  lua_pushnumber(lua, HDBTBZIP);
  lua_setfield(lua, -2, "TBZIP");
  lua_pushnumber(lua, HDBTTCBS);
  lua_setfield(lua, -2, "TTCBS");
  lua_pushnumber(lua, HDBOREADER);
  lua_setfield(lua, -2, "OREADER");
  lua_pushnumber(lua, HDBOWRITER);
  lua_setfield(lua, -2, "OWRITER");
  lua_pushnumber(lua, HDBOCREAT);
  lua_setfield(lua, -2, "OCREAT");
  lua_pushnumber(lua, HDBOTRUNC);
  lua_setfield(lua, -2, "OTRUNC");
  lua_pushnumber(lua, HDBONOLCK);
  lua_setfield(lua, -2, "ONOLCK");
  lua_pushnumber(lua, HDBOLCKNB);
  lua_setfield(lua, -2, "OLCKNB");
  lua_pushnumber(lua, HDBOTSYNC);
  lua_setfield(lua, -2, "OTSYNC");
  
  lua_pushcfunction(lua, hdb_errmsg);
  lua_setfield(lua, -2, "errmsg");
  lua_pushcfunction(lua, hdb_ecode);
  lua_setfield(lua, -2, "ecode");
  lua_pushcfunction(lua, hdb_tune);
  lua_setfield(lua, -2, "tune");
  lua_pushcfunction(lua, hdb_setcache);
  lua_setfield(lua, -2, "setcache");
  lua_pushcfunction(lua, hdb_setxmsiz);
  lua_setfield(lua, -2, "setxmsiz");
  lua_pushcfunction(lua, hdb_setdfunit);
  lua_setfield(lua, -2, "setdfunit");
  lua_pushcfunction(lua, hdb_open);
  lua_setfield(lua, -2, "open");
  lua_pushcfunction(lua, hdb_close);
  lua_setfield(lua, -2, "close");
  lua_pushcfunction(lua, hdb_put);
  lua_setfield(lua, -2, "put");
  lua_pushcfunction(lua, hdb_putkeep);
  lua_setfield(lua, -2, "putkeep");
  lua_pushcfunction(lua, hdb_putcat);
  lua_setfield(lua, -2, "putcat");
  lua_pushcfunction(lua, hdb_putasync);
  lua_setfield(lua, -2, "putasync");
  lua_pushcfunction(lua, hdb_out);
  lua_setfield(lua, -2, "out");
  lua_pushcfunction(lua, hdb_get);
  lua_setfield(lua, -2, "get");
  lua_pushcfunction(lua, hdb_vsiz);
  lua_setfield(lua, -2, "vsiz");
  lua_pushcfunction(lua, hdb_iterinit);
  lua_setfield(lua, -2, "iterinit");
  lua_pushcfunction(lua, hdb_iternext);
  lua_setfield(lua, -2, "iternext");
  lua_pushcfunction(lua, hdb_fwmkeys);
  lua_setfield(lua, -2, "fwmkeys");
  lua_pushcfunction(lua, hdb_addint);
  lua_setfield(lua, -2, "addint");
  lua_pushcfunction(lua, hdb_adddouble);
  lua_setfield(lua, -2, "adddouble");
  lua_pushcfunction(lua, hdb_sync);
  lua_setfield(lua, -2, "sync");
  lua_pushcfunction(lua, hdb_optimize);
  lua_setfield(lua, -2, "optimize");
  lua_pushcfunction(lua, hdb_vanish);
  lua_setfield(lua, -2, "vanish");
  lua_pushcfunction(lua, hdb_copy);
  lua_setfield(lua, -2, "copy");
  lua_pushcfunction(lua, hdb_tranbegin);
  lua_setfield(lua, -2, "tranbegin");
  lua_pushcfunction(lua, hdb_trancommit);
  lua_setfield(lua, -2, "trancommit");
  lua_pushcfunction(lua, hdb_tranabort);
  lua_setfield(lua, -2, "tranabort");
  lua_pushcfunction(lua, hdb_path);
  lua_setfield(lua, -2, "path");
  lua_pushcfunction(lua, hdb_rnum);
  lua_setfield(lua, -2, "rnum");
  lua_pushcfunction(lua, hdb_fsiz);
  lua_setfield(lua, -2, "fsiz");
  lua_pushcfunction(lua, hdb_foreach);
  lua_setfield(lua, -2, "foreach");
  lua_pushcfunction(lua, hdb_pairs);
  lua_setfield(lua, -2, "pairs");
  lua_pushcfunction(lua, hdb_next);
  lua_setfield(lua, -2, "next");
  lua_newtable(lua);
  lua_pushcfunction(lua, hdb_rnum);
  lua_setfield(lua, -2, "__len");
  lua_pushcfunction(lua, hdb_get);
  lua_setfield(lua, -2, "__index");
  lua_pushcfunction(lua, hdb_put);
  lua_setfield(lua, -2, "__newindex");
  lua_setmetatable(lua, -2);
  return 1;
}


/* for del method */
static int hdb_del(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_isuserdata(lua, 1)){
    lua_pushstring(lua, "__gc: invalid arguments");
    lua_error(lua);
  }
  HDBDATA *data = lua_touserdata(lua, 1);
  TCHDB *hdb = data->hdb;
  tchdbdel(hdb);
  return 0;
}


/* for errmsg method */
static int hdb_errmsg(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "errmsg: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  int ecode = argc > 1 ? lua_tointeger(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "errmsg: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(ecode == -1) ecode = tchdbecode(hdb);
  lua_pushstring(lua, tchdberrmsg(ecode));
  return 1;
}


/* for ecode method */
static int hdb_ecode(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "ecode: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "ecode: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  lua_pushnumber(lua, tchdbecode(hdb));
  return 1;
}


/* for tune method */
static int hdb_tune(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 5 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tune: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  int64_t bnum = argc > 1 ? lua_tonumber(lua, 2) : -1;
  int32_t apow = argc > 2 ? lua_tonumber(lua, 3) : -1;
  int32_t fpow = argc > 3 ? lua_tonumber(lua, 4) : -1;
  int32_t opts = argc > 4 ? lua_tointeger(lua, 5) : 0;
  if(!data){
    lua_pushstring(lua, "tune: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbtune(hdb, bnum, apow, fpow, opts)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for setcache method */
static int hdb_setcache(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setcache: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  int32_t rcnum = argc > 1 ? lua_tonumber(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "setcache: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbsetcache(hdb, rcnum)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for setxmsiz method */
static int hdb_setxmsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setxmsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  int64_t xmsiz = argc > 1 ? lua_tonumber(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "setxmsiz: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbsetxmsiz(hdb, xmsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for setdfunit method */
static int hdb_setdfunit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setdfunit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  int32_t dfunit = argc > 1 ? lua_tonumber(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "setdfunit: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbsetdfunit(hdb, dfunit)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for open method */
static int hdb_open(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  const char *path = lua_tostring(lua, 2);
  int omode = argc > 2 ? lua_tointeger(lua, 3) : HDBOREADER;
  if(!data || !path){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbopen(hdb, path, omode)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for close method */
static int hdb_close(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "close: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "close: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbclose(hdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for put method */
static int hdb_put(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbput(hdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putkeep method */
static int hdb_putkeep(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putkeep: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "putkeep: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbputkeep(hdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putcat method */
static int hdb_putcat(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putcat: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "putcat: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbputcat(hdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putasync method */
static int hdb_putasync(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putasync: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "putasync: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbputasync(hdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for out method */
static int hdb_out(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbout(hdb, kbuf, ksiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for get method */
static int hdb_get(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "get: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "get: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  int vsiz;
  char *vbuf = tchdbget(hdb, kbuf, ksiz, &vsiz);
  if(vbuf){
    lua_pushlstring(lua, vbuf, vsiz);
    tcfree(vbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for vsiz method */
static int hdb_vsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "vsiz: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  lua_pushnumber(lua, tchdbvsiz(hdb, kbuf, ksiz));
  return 1;
}


/* for iterinit method */
static int hdb_iterinit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "iterinit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "iterinit: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbiterinit(hdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for iternext method */
static int hdb_iternext(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "iternext: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "iternext: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  int ksiz;
  char *kbuf = tchdbiternext(hdb, &ksiz);
  if(kbuf){
    lua_pushlstring(lua, kbuf, ksiz);
    tcfree(kbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for fwmkeys method */
static int hdb_fwmkeys(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "fwmkeys: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  size_t psiz;
  const char *pbuf = lua_tolstring(lua, 2, &psiz);
  int max = argc > 2 && lua_isnumber(lua, 3) ? lua_tonumber(lua, 3) : -1;
  if(!data || !pbuf){
    lua_pushstring(lua, "fwmkeys: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  TCLIST *keys = tchdbfwmkeys(hdb, pbuf, psiz, max);
  tclisttotable(lua, keys);
  tclistdel(keys);
  return 1;
}


/* for addint method */
static int hdb_addint(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "addint: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  int num = lua_tonumber(lua, 3);
  if(!data){
    lua_pushstring(lua, "addint: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  num = tchdbaddint(hdb, kbuf, ksiz, num);
  if(num == INT_MIN){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/* for adddouble method */
static int hdb_adddouble(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "adddouble: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  double num = lua_tonumber(lua, 3);
  if(!data){
    lua_pushstring(lua, "adddouble: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  num = tchdbadddouble(hdb, kbuf, ksiz, num);
  if(isnan(num)){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/* for sync method */
static int hdb_sync(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "sync: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "sync: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbsync(hdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for optimize method */
static int hdb_optimize(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 5 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "optimize: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  int64_t bnum = argc > 1 ? lua_tonumber(lua, 2) : -1;
  int32_t apow = argc > 2 ? lua_tonumber(lua, 3) : -1;
  int32_t fpow = argc > 3 ? lua_tonumber(lua, 4) : -1;
  int32_t opts = argc > 4 ? lua_tointeger(lua, 5) : UINT8_MAX;
  if(!data){
    lua_pushstring(lua, "optimize: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdboptimize(hdb, bnum, apow, fpow, opts)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for vanish method */
static int hdb_vanish(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vanish: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "vanish: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbvanish(hdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for copy method */
static int hdb_copy(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "copy: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  const char *path = lua_tostring(lua, 2);
  if(!data || !path){
    lua_pushstring(lua, "copy: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbcopy(hdb, path)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for tranbegin method */
static int hdb_tranbegin(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tranbegin: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "tranbegin: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbtranbegin(hdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for trancommit method */
static int hdb_trancommit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "trancommit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "trancommit: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbtrancommit(hdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for tranabort method */
static int hdb_tranabort(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tranabort: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "tranabort: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(tchdbtranabort(hdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for path method */
static int hdb_path(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "path: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "path: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  const char *path = tchdbpath(hdb);
  if(path){
    lua_pushstring(lua, path);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for rnum method */
static int hdb_rnum(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "rnum: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "rnum: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  lua_pushnumber(lua, tchdbrnum(hdb));
  return 1;
}


/* for fsiz method */
static int hdb_fsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "fsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "fsiz: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  lua_pushnumber(lua, tchdbfsiz(hdb));
  return 1;
}


/* for foreach method */
static int hdb_foreach(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "foreach: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data || !lua_isfunction(lua, 2)){
    lua_pushstring(lua, "foreach: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  FUNCOP *funcop = tcmalloc(sizeof(*funcop));
  funcop->lua = lua;
  funcop->fname = tcsprintf("_iterrec_%llX",
                            (unsigned long long)((tctime() - 1195786800) * 1000000));
  lua_pushvalue(lua, 2);
  lua_setglobal(lua, funcop->fname);
  if(tchdbforeach(hdb, (TCITER)util_iterrec, funcop)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  lua_pushnil(lua);
  lua_setglobal(lua, funcop->fname);
  tcfree(funcop->fname);
  tcfree(funcop);
  return 1;
}


/* for pairs method */
static int hdb_pairs(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "pairs: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "pairs: invalid arguments");
    lua_error(lua);
  }
  lua_pushcfunction(lua, hdb_next);
  lua_pushvalue(lua, 1);
  lua_pushnil(lua);
  return 3;
}


/* for next method */
static int hdb_next(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, HDBDATAVAR);
  HDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  TCHDB *hdb = data->hdb;
  if(lua_isnil(lua, 2) && !tchdbiterinit(hdb)){
    lua_pushnil(lua);
    return 1;
  }
  int ksiz;
  char *kbuf = tchdbiternext(hdb, &ksiz);
  if(kbuf){
    lua_pushlstring(lua, kbuf, ksiz);
    int vsiz;
    char *vbuf = tchdbget(hdb, kbuf, ksiz, &vsiz);
    if(vbuf){
      lua_pushlstring(lua, vbuf, vsiz);
      tcfree(vbuf);
    } else {
      lua_pushnil(lua);
    }
    tcfree(kbuf);
  } else {
    lua_pushnil(lua);
    lua_pushnil(lua);
  }
  return 2;
}


/* initialization of B+ tree database */
static void bdb_init(lua_State *lua){
  lua_pushcfunction(lua, bdb_new);
  lua_setfield(lua, -2, "bdbnew");
}


/* for new function */
static int bdb_new(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "bdbnew: invalid arguments");
    lua_error(lua);
  }
  lua_newtable(lua);
  BDBDATA *data = lua_newuserdata(lua, sizeof(*data));
  data->bdb = tcbdbnew();
  tcbdbsetmutex(data->bdb);
  lua_newtable(lua);
  lua_pushcfunction(lua, bdb_del);
  lua_setfield(lua, -2, "__gc");
  lua_setmetatable(lua, -2);
  lua_setfield(lua, -2, BDBDATAVAR);
  lua_pushnumber(lua, TCESUCCESS);
  lua_setfield(lua, -2, "ESUCCESS");
  lua_pushnumber(lua, TCETHREAD);
  lua_setfield(lua, -2, "ETHREAD");
  lua_pushnumber(lua, TCEINVALID);
  lua_setfield(lua, -2, "EINVALID");
  lua_pushnumber(lua, TCENOFILE);
  lua_setfield(lua, -2, "ENOFILE");
  lua_pushnumber(lua, TCENOPERM);
  lua_setfield(lua, -2, "ENOPERM");
  lua_pushnumber(lua, TCEMETA);
  lua_setfield(lua, -2, "EMETA");
  lua_pushnumber(lua, TCERHEAD);
  lua_setfield(lua, -2, "ERHEAD");
  lua_pushnumber(lua, TCEOPEN);
  lua_setfield(lua, -2, "EOPEN");
  lua_pushnumber(lua, TCECLOSE);
  lua_setfield(lua, -2, "ECLOSE");
  lua_pushnumber(lua, TCETRUNC);
  lua_setfield(lua, -2, "ETRUNC");
  lua_pushnumber(lua, TCESYNC);
  lua_setfield(lua, -2, "ESYNC");
  lua_pushnumber(lua, TCESTAT);
  lua_setfield(lua, -2, "ESTAT");
  lua_pushnumber(lua, TCESEEK);
  lua_setfield(lua, -2, "ESEEK");
  lua_pushnumber(lua, TCEREAD);
  lua_setfield(lua, -2, "EREAD");
  lua_pushnumber(lua, TCEWRITE);
  lua_setfield(lua, -2, "EWRITE");
  lua_pushnumber(lua, TCEMMAP);
  lua_setfield(lua, -2, "EMMAP");
  lua_pushnumber(lua, TCELOCK);
  lua_setfield(lua, -2, "ELOCK");
  lua_pushnumber(lua, TCEUNLINK);
  lua_setfield(lua, -2, "EUNLINK");
  lua_pushnumber(lua, TCERENAME);
  lua_setfield(lua, -2, "ERENAME");
  lua_pushnumber(lua, TCEMKDIR);
  lua_setfield(lua, -2, "EMKDIR");
  lua_pushnumber(lua, TCERMDIR);
  lua_setfield(lua, -2, "ERMDIR");
  lua_pushnumber(lua, TCEKEEP);
  lua_setfield(lua, -2, "EKEEP");
  lua_pushnumber(lua, TCENOREC);
  lua_setfield(lua, -2, "ENOREC");
  lua_pushnumber(lua, TCEMISC);
  lua_setfield(lua, -2, "EMISC");
  lua_pushstring(lua, "CMPLEXICAL");
  lua_setfield(lua, -2, "CMPLEXICAL");
  lua_pushstring(lua, "CMPDECIMAL");
  lua_setfield(lua, -2, "CMPDECIMAL");
  lua_pushstring(lua, "CMPINT32");
  lua_setfield(lua, -2, "CMPINT32");
  lua_pushstring(lua, "CMPINT64");
  lua_setfield(lua, -2, "CMPINT64");
  lua_pushnumber(lua, BDBTLARGE);
  lua_setfield(lua, -2, "TLARGE");
  lua_pushnumber(lua, BDBTDEFLATE);
  lua_setfield(lua, -2, "TDEFLATE");
  lua_pushnumber(lua, BDBTBZIP);
  lua_setfield(lua, -2, "TBZIP");
  lua_pushnumber(lua, BDBTTCBS);
  lua_setfield(lua, -2, "TTCBS");
  lua_pushnumber(lua, BDBOREADER);
  lua_setfield(lua, -2, "OREADER");
  lua_pushnumber(lua, BDBOWRITER);
  lua_setfield(lua, -2, "OWRITER");
  lua_pushnumber(lua, BDBOCREAT);
  lua_setfield(lua, -2, "OCREAT");
  lua_pushnumber(lua, BDBOTRUNC);
  lua_setfield(lua, -2, "OTRUNC");
  lua_pushnumber(lua, BDBONOLCK);
  lua_setfield(lua, -2, "ONOLCK");
  lua_pushnumber(lua, BDBOLCKNB);
  lua_setfield(lua, -2, "OLCKNB");
  lua_pushnumber(lua, BDBOTSYNC);
  lua_setfield(lua, -2, "OTSYNC");
  lua_pushcfunction(lua, bdb_errmsg);
  lua_setfield(lua, -2, "errmsg");
  lua_pushcfunction(lua, bdb_ecode);
  lua_setfield(lua, -2, "ecode");
  lua_pushcfunction(lua, bdb_setcmpfunc);
  lua_setfield(lua, -2, "setcmpfunc");
  lua_pushcfunction(lua, bdb_tune);
  lua_setfield(lua, -2, "tune");
  lua_pushcfunction(lua, bdb_setcache);
  lua_setfield(lua, -2, "setcache");
  lua_pushcfunction(lua, bdb_setxmsiz);
  lua_setfield(lua, -2, "setxmsiz");
  lua_pushcfunction(lua, bdb_setdfunit);
  lua_setfield(lua, -2, "setdfunit");
  lua_pushcfunction(lua, bdb_open);
  lua_setfield(lua, -2, "open");
  lua_pushcfunction(lua, bdb_close);
  lua_setfield(lua, -2, "close");
  lua_pushcfunction(lua, bdb_put);
  lua_setfield(lua, -2, "put");
  lua_pushcfunction(lua, bdb_putkeep);
  lua_setfield(lua, -2, "putkeep");
  lua_pushcfunction(lua, bdb_putcat);
  lua_setfield(lua, -2, "putcat");
  lua_pushcfunction(lua, bdb_putdup);
  lua_setfield(lua, -2, "putdup");
  lua_pushcfunction(lua, bdb_putlist);
  lua_setfield(lua, -2, "putlist");
  lua_pushcfunction(lua, bdb_out);
  lua_setfield(lua, -2, "out");
  lua_pushcfunction(lua, bdb_outlist);
  lua_setfield(lua, -2, "outlist");
  lua_pushcfunction(lua, bdb_get);
  lua_setfield(lua, -2, "get");
  lua_pushcfunction(lua, bdb_getlist);
  lua_setfield(lua, -2, "getlist");
  lua_pushcfunction(lua, bdb_vnum);
  lua_setfield(lua, -2, "vnum");
  lua_pushcfunction(lua, bdb_vsiz);
  lua_setfield(lua, -2, "vsiz");
  lua_pushcfunction(lua, bdb_range);
  lua_setfield(lua, -2, "range");
  lua_pushcfunction(lua, bdb_fwmkeys);
  lua_setfield(lua, -2, "fwmkeys");
  lua_pushcfunction(lua, bdb_addint);
  lua_setfield(lua, -2, "addint");
  lua_pushcfunction(lua, bdb_adddouble);
  lua_setfield(lua, -2, "adddouble");
  lua_pushcfunction(lua, bdb_sync);
  lua_setfield(lua, -2, "sync");
  lua_pushcfunction(lua, bdb_optimize);
  lua_setfield(lua, -2, "optimize");
  lua_pushcfunction(lua, bdb_vanish);
  lua_setfield(lua, -2, "vanish");
  lua_pushcfunction(lua, bdb_tranbegin);
  lua_setfield(lua, -2, "tranbegin");
  lua_pushcfunction(lua, bdb_trancommit);
  lua_setfield(lua, -2, "trancommit");
  lua_pushcfunction(lua, bdb_tranabort);
  lua_setfield(lua, -2, "tranabort");
  lua_pushcfunction(lua, bdb_copy);
  lua_setfield(lua, -2, "copy");
  lua_pushcfunction(lua, bdb_path);
  lua_setfield(lua, -2, "path");
  lua_pushcfunction(lua, bdb_rnum);
  lua_setfield(lua, -2, "rnum");
  lua_pushcfunction(lua, bdb_fsiz);
  lua_setfield(lua, -2, "fsiz");
  lua_pushcfunction(lua, bdb_foreach);
  lua_setfield(lua, -2, "foreach");
  lua_pushcfunction(lua, bdb_pairs);
  lua_setfield(lua, -2, "pairs");
  lua_pushcfunction(lua, bdb_next);
  lua_setfield(lua, -2, "next");
  lua_newtable(lua);
  lua_pushcfunction(lua, bdb_rnum);
  lua_setfield(lua, -2, "__len");
  lua_pushcfunction(lua, bdb_get);
  lua_setfield(lua, -2, "__index");
  lua_pushcfunction(lua, bdb_put);
  lua_setfield(lua, -2, "__newindex");
  lua_setmetatable(lua, -2);
  return 1;
}


/* for del method */
static int bdb_del(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_isuserdata(lua, 1)){
    lua_pushstring(lua, "__gc: invalid arguments");
    lua_error(lua);
  }
  BDBDATA *data = lua_touserdata(lua, 1);
  TCBDB *bdb = data->bdb;
  FUNCOP *funcop = tcbdbcmpop(bdb);
  if(funcop){
    lua_pushnil(lua);
    lua_setglobal(lua, funcop->fname);
    tcfree(funcop->fname);
    tcfree(funcop);
  }
  tcbdbdel(bdb);
  return 0;
}


/* for errmsg method */
static int bdb_errmsg(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "errmsg: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  int ecode = argc > 1 ? lua_tointeger(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "errmsg: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(ecode == -1) ecode = tcbdbecode(bdb);
  lua_pushstring(lua, tcbdberrmsg(ecode));
  return 1;
}


/* for ecode method */
static int bdb_ecode(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "ecode: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "ecode: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  lua_pushnumber(lua, tcbdbecode(bdb));
  return 1;
}


/* for setcmpfunc method */
static int bdb_setcmpfunc(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setcmpfunc: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "setcmpfunc: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  FUNCOP *funcop = tcbdbcmpop(bdb);
  if(funcop){
    lua_pushnil(lua);
    lua_setglobal(lua, funcop->fname);
    tcfree(funcop->fname);
    tcfree(funcop);
  }
  if(lua_isfunction(lua, 2)){
    funcop = tcmalloc(sizeof(*funcop));
    funcop->lua = lua;
    funcop->fname = tcsprintf("_cmpobj_%llX",
                             (unsigned long long)((tctime() - 1195786800) * 1000000));
    lua_pushvalue(lua, 2);
    lua_setglobal(lua, funcop->fname);
    if(tcbdbsetcmpfunc(bdb, (TCCMP)util_cmpobj, funcop)){
      lua_pushboolean(lua, true);
    } else {
      lua_pushboolean(lua, false);
    }
  } else if(lua_isstring(lua, 2)){
    TCCMP cmp = NULL;
    const char *fname = lua_tostring(lua, 2);
    if(!strcmp(fname, "CMPLEXICAL")){
      cmp = tccmplexical;
    } else if(!strcmp(fname, "CMPDECIMAL")){
      cmp = tccmpdecimal;
    } else if(!strcmp(fname, "CMPINT32")){
      cmp = tccmpint32;
    } else if(!strcmp(fname, "CMPINT64")){
      cmp = tccmpint64;
    } else {
      cmp = NULL;
    }
    if(cmp){
      if(tcbdbsetcmpfunc(bdb, cmp, NULL)){
        lua_pushboolean(lua, true);
      } else {
        lua_pushboolean(lua, false);
      }
    } else {
      lua_pushstring(lua, "setcmpfunc: invalid arguments");
      lua_error(lua);
    }
  } else {
    lua_pushstring(lua, "setcmpfunc: invalid arguments");
    lua_error(lua);
  }
  return 1;
}


/* for tune method */
static int bdb_tune(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 7 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tune: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  int32_t lmemb = argc > 1 ? lua_tonumber(lua, 2) : -1;
  int32_t nmemb = argc > 2 ? lua_tonumber(lua, 3) : -1;
  int64_t bnum = argc > 3 ? lua_tonumber(lua, 4) : -1;
  int32_t apow = argc > 4 ? lua_tonumber(lua, 5) : -1;
  int32_t fpow = argc > 5 ? lua_tonumber(lua, 6) : -1;
  int32_t opts = argc > 6 ? lua_tointeger(lua, 7) : 0;
  if(!data){
    lua_pushstring(lua, "tune: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbtune(bdb, lmemb, nmemb, bnum, apow, fpow, opts)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for setcache method */
static int bdb_setcache(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setcache: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  int32_t lcnum = argc > 1 ? lua_tonumber(lua, 2) : -1;
  int32_t ncnum = argc > 2 ? lua_tonumber(lua, 3) : -1;
  if(!data){
    lua_pushstring(lua, "setcache: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbsetcache(bdb, lcnum, ncnum)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for setxmsiz method */
static int bdb_setxmsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setxmsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  int64_t xmsiz = argc > 1 ? lua_tonumber(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "setxmsiz: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbsetxmsiz(bdb, xmsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for setdfunit method */
static int bdb_setdfunit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setdfunit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  int32_t dfunit = argc > 1 ? lua_tonumber(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "setdfunit: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbsetdfunit(bdb, dfunit)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for open method */
static int bdb_open(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  const char *path = lua_tostring(lua, 2);
  int omode = argc > 2 ? lua_tointeger(lua, 3) : BDBOREADER;
  if(!data || !path){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbopen(bdb, path, omode)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for close method */
static int bdb_close(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "close: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "close: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbclose(bdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for put method */
static int bdb_put(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbput(bdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putkeep method */
static int bdb_putkeep(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putkeep: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "putkeep: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbputkeep(bdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putcat method */
static int bdb_putcat(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putcat: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "putcat: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbputcat(bdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putdup method */
static int bdb_putdup(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putdup: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "putdup: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbputdup(bdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putlist method */
static int bdb_putlist(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putlist: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf || !lua_istable(lua, 3)){
    lua_pushstring(lua, "putlist: invalid arguments");
    lua_error(lua);
  }
  TCLIST *vals = tabletotclist(lua, 3);
  TCBDB *bdb = data->bdb;
  if(tcbdbputdup3(bdb, kbuf, ksiz, vals)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  tclistdel(vals);
  return 1;
}


/* for out method */
static int bdb_out(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbout(bdb, kbuf, ksiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for outlist method */
static int bdb_outlist(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "outlist: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "outlist: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbout3(bdb, kbuf, ksiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for get method */
static int bdb_get(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "get: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "get: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  int vsiz;
  char *vbuf = tcbdbget(bdb, kbuf, ksiz, &vsiz);
  if(vbuf){
    lua_pushlstring(lua, vbuf, vsiz);
    tcfree(vbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for getlist method */
static int bdb_getlist(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "getlist: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "getlist: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  TCLIST *vals = tcbdbget4(bdb, kbuf, ksiz);
  if(vals){
    tclisttotable(lua, vals);
    tclistdel(vals);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for vnum method */
static int bdb_vnum(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vnum: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "vnum: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  lua_pushnumber(lua, tcbdbvnum(bdb, kbuf, ksiz));
  return 1;
}


/* for vsiz method */
static int bdb_vsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "vsiz: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  lua_pushnumber(lua, tcbdbvsiz(bdb, kbuf, ksiz));
  return 1;
}


/* for range method */
static int bdb_range(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 6 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "range: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t bksiz = 0;
  const char *bkbuf = argc > 1 ? lua_tolstring(lua, 2, &bksiz) : NULL;
  bool binc = argc > 2 ? lua_toboolean(lua, 3) : false;
  size_t eksiz = 0;
  const char *ekbuf = argc > 3 ? lua_tolstring(lua, 4, &eksiz) : NULL;
  bool einc = argc > 4 ? lua_toboolean(lua, 5) : false;
  int max = argc > 5 ? lua_tonumber(lua, 6) : -1;
  if(!data){
    lua_pushstring(lua, "range: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  TCLIST *keys = tcbdbrange(bdb, bkbuf, bksiz, binc, ekbuf, eksiz, einc, max);
  tclisttotable(lua, keys);
  tclistdel(keys);
  return 1;
}


/* for fwmkeys method */
static int bdb_fwmkeys(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "fwmkeys: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t psiz;
  const char *pbuf = lua_tolstring(lua, 2, &psiz);
  int max = argc > 2 && lua_isnumber(lua, 3) ? lua_tonumber(lua, 3) : -1;
  if(!data || !pbuf){
    lua_pushstring(lua, "fwmkeys: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  TCLIST *keys = tcbdbfwmkeys(bdb, pbuf, psiz, max);
  tclisttotable(lua, keys);
  tclistdel(keys);
  return 1;
}


/* for addint method */
static int bdb_addint(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "addint: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  int num = lua_tonumber(lua, 3);
  if(!data){
    lua_pushstring(lua, "addint: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  num = tcbdbaddint(bdb, kbuf, ksiz, num);
  if(num == INT_MIN){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/* for adddouble method */
static int bdb_adddouble(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "adddouble: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  double num = lua_tonumber(lua, 3);
  if(!data){
    lua_pushstring(lua, "adddouble: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  num = tcbdbadddouble(bdb, kbuf, ksiz, num);
  if(isnan(num)){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/* for sync method */
static int bdb_sync(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "sync: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "sync: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbsync(bdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for optimize method */
static int bdb_optimize(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 7 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "optimize: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  int32_t lmemb = argc > 1 ? lua_tonumber(lua, 2) : -1;
  int32_t nmemb = argc > 2 ? lua_tonumber(lua, 3) : -1;
  int64_t bnum = argc > 3 ? lua_tonumber(lua, 4) : -1;
  int32_t apow = argc > 4 ? lua_tonumber(lua, 5) : -1;
  int32_t fpow = argc > 5 ? lua_tonumber(lua, 6) : -1;
  int32_t opts = argc > 6 ? lua_tointeger(lua, 7) : UINT8_MAX;
  if(!data){
    lua_pushstring(lua, "optimize: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdboptimize(bdb, lmemb, nmemb, bnum, apow, fpow, opts)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for vanish method */
static int bdb_vanish(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vanish: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "vanish: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbvanish(bdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for copy method */
static int bdb_copy(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "copy: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  const char *path = lua_tostring(lua, 2);
  if(!data || !path){
    lua_pushstring(lua, "copy: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbcopy(bdb, path)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for tranbegin method */
static int bdb_tranbegin(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tranbegin: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "tranbegin: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbtranbegin(bdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for trancommit method */
static int bdb_trancommit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "trancommit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "trancommit: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbtrancommit(bdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for tranabort method */
static int bdb_tranabort(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tranabort: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "tranabort: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  if(tcbdbtranabort(bdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for path method */
static int bdb_path(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "path: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "path: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  const char *path = tcbdbpath(bdb);
  if(path){
    lua_pushstring(lua, path);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for rnum method */
static int bdb_rnum(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "rnum: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "rnum: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  lua_pushnumber(lua, tcbdbrnum(bdb));
  return 1;
}


/* for fsiz method */
static int bdb_fsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "fsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "fsiz: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  lua_pushnumber(lua, tcbdbfsiz(bdb));
  return 1;
}


/* for foreach method */
static int bdb_foreach(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "foreach: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *data = lua_touserdata(lua, -1);
  if(!data || !lua_isfunction(lua, 2)){
    lua_pushstring(lua, "foreach: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = data->bdb;
  FUNCOP *funcop = tcmalloc(sizeof(*funcop));
  funcop->lua = lua;
  funcop->fname = tcsprintf("_iterrec_%llX",
                            (unsigned long long)((tctime() - 1195786800) * 1000000));
  lua_pushvalue(lua, 2);
  lua_setglobal(lua, funcop->fname);
  if(tcbdbforeach(bdb, (TCITER)util_iterrec, funcop)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  lua_pushnil(lua);
  lua_setglobal(lua, funcop->fname);
  tcfree(funcop->fname);
  tcfree(funcop);
  return 1;
}


/* for pairs method */
static int bdb_pairs(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "pairs: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *bdbdata = lua_touserdata(lua, -1);
  if(!bdbdata){
    lua_pushstring(lua, "pairs: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = bdbdata->bdb;
  lua_newtable(lua);
  BDBCURDATA *curdata = lua_newuserdata(lua, sizeof(*curdata));
  curdata->cur = tcbdbcurnew(bdb);
  lua_newtable(lua);
  lua_pushcfunction(lua, bdbcur_del);
  lua_setfield(lua, -2, "__gc");
  lua_setmetatable(lua, -2);
  lua_setfield(lua, -2, BDBCURDATAVAR);
  lua_pushcfunction(lua, bdb_next);
  lua_pushvalue(lua, -2);
  lua_pushnil(lua);
  return 3;
}


/* for next method */
static int bdb_next(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBCURDATAVAR);
  BDBCURDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  BDBCUR *cur = data->cur;
  if(lua_isnil(lua, 2) && !tcbdbcurfirst(cur)){
    lua_pushnil(lua);
    return 1;
  }
  int ksiz;
  char *kbuf = tcbdbcurkey(cur, &ksiz);
  if(kbuf){
    lua_pushlstring(lua, kbuf, ksiz);
    int vsiz;
    char *vbuf = tcbdbcurval(cur, &vsiz);
    if(vbuf){
      lua_pushlstring(lua, vbuf, vsiz);
      tcfree(vbuf);
    } else {
      lua_pushnil(lua);
    }
    tcfree(kbuf);
    tcbdbcurnext(cur);
  } else {
    lua_pushnil(lua);
    lua_pushnil(lua);
  }
  return 2;
}


/* for init method */
static void bdbcur_init(lua_State *lua){
  lua_pushcfunction(lua, bdbcur_new);
  lua_setfield(lua, -2, "bdbcurnew");
}


/* for new method */
static int bdbcur_new(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "bdbcur: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBDATAVAR);
  BDBDATA *bdbdata = lua_touserdata(lua, -1);
  if(!bdbdata){
    lua_pushstring(lua, "bdbcur: invalid arguments");
    lua_error(lua);
  }
  TCBDB *bdb = bdbdata->bdb;
  lua_newtable(lua);
  BDBCURDATA *curdata = lua_newuserdata(lua, sizeof(*curdata));
  curdata->cur = tcbdbcurnew(bdb);
  lua_newtable(lua);
  lua_pushcfunction(lua, bdbcur_del);
  lua_setfield(lua, -2, "__gc");
  lua_setmetatable(lua, -2);
  lua_setfield(lua, -2, BDBCURDATAVAR);
  lua_pushvalue(lua, 1);
  lua_setfield(lua, -2, BDBDATAVAR);
  lua_pushnumber(lua, BDBCPCURRENT);
  lua_setfield(lua, -2, "CPCURRENT");
  lua_pushnumber(lua, BDBCPBEFORE);
  lua_setfield(lua, -2, "CPBEFORE");
  lua_pushnumber(lua, BDBCPAFTER);
  lua_setfield(lua, -2, "CPAFTER");
  lua_pushcfunction(lua, bdbcur_first);
  lua_setfield(lua, -2, "first");
  lua_pushcfunction(lua, bdbcur_last);
  lua_setfield(lua, -2, "last");
  lua_pushcfunction(lua, bdbcur_jump);
  lua_setfield(lua, -2, "jump");
  lua_pushcfunction(lua, bdbcur_prev);
  lua_setfield(lua, -2, "prev");
  lua_pushcfunction(lua, bdbcur_next);
  lua_setfield(lua, -2, "next");
  lua_pushcfunction(lua, bdbcur_put);
  lua_setfield(lua, -2, "put");
  lua_pushcfunction(lua, bdbcur_out);
  lua_setfield(lua, -2, "out");
  lua_pushcfunction(lua, bdbcur_key);
  lua_setfield(lua, -2, "key");
  lua_pushcfunction(lua, bdbcur_val);
  lua_setfield(lua, -2, "val");
  return 1;
}


/* for del method */
static int bdbcur_del(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_isuserdata(lua, 1)){
    lua_pushstring(lua, "__gc: invalid arguments");
    lua_error(lua);
  }
  BDBCURDATA *data = lua_touserdata(lua, 1);
  BDBCUR *cur = data->cur;
  tcbdbcurdel(cur);
  return 0;
}


/* for first method */
static int bdbcur_first(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "first: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBCURDATAVAR);
  BDBCURDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "first: invalid arguments");
    lua_error(lua);
  }
  BDBCUR *cur = data->cur;
  if(tcbdbcurfirst(cur)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for last method */
static int bdbcur_last(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "last: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBCURDATAVAR);
  BDBCURDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "last: invalid arguments");
    lua_error(lua);
  }
  BDBCUR *cur = data->cur;
  if(tcbdbcurlast(cur)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for jump method */
static int bdbcur_jump(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "jump: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBCURDATAVAR);
  BDBCURDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "jump: invalid arguments");
    lua_error(lua);
  }
  BDBCUR *cur = data->cur;
  if(tcbdbcurjump(cur, kbuf, ksiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for prev method */
static int bdbcur_prev(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "prev: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBCURDATAVAR);
  BDBCURDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "prev: invalid arguments");
    lua_error(lua);
  }
  BDBCUR *cur = data->cur;
  if(tcbdbcurprev(cur)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for next method */
static int bdbcur_next(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBCURDATAVAR);
  BDBCURDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  BDBCUR *cur = data->cur;
  if(tcbdbcurnext(cur)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for put method */
static int bdbcur_put(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBCURDATAVAR);
  BDBCURDATA *data = lua_touserdata(lua, -1);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 2, &vsiz);
  int cpmode = argc > 2 ? lua_tonumber(lua, 3) : BDBCPCURRENT;
  if(!data || !vbuf){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  BDBCUR *cur = data->cur;
  if(tcbdbcurput(cur, vbuf, vsiz, cpmode)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for out method */
static int bdbcur_out(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBCURDATAVAR);
  BDBCURDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  BDBCUR *cur = data->cur;
  if(tcbdbcurout(cur)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for key method */
static int bdbcur_key(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "key: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBCURDATAVAR);
  BDBCURDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "key: invalid arguments");
    lua_error(lua);
  }
  BDBCUR *cur = data->cur;
  int ksiz;
  char *kbuf = tcbdbcurkey(cur, &ksiz);
  if(kbuf){
    lua_pushlstring(lua, kbuf, ksiz);
    tcfree(kbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for val method */
static int bdbcur_val(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "val: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, BDBCURDATAVAR);
  BDBCURDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "val: invalid arguments");
    lua_error(lua);
  }
  BDBCUR *cur = data->cur;
  int vsiz;
  char *vbuf = tcbdbcurval(cur, &vsiz);
  if(vbuf){
    lua_pushlstring(lua, vbuf, vsiz);
    tcfree(vbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* initialization of fixed-length database */
static void fdb_init(lua_State *lua){
  lua_pushcfunction(lua, fdb_new);
  lua_setfield(lua, -2, "fdbnew");
}


/* for new function */
static int fdb_new(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "fdbnew: invalid arguments");
    lua_error(lua);
  }
  lua_newtable(lua);
  FDBDATA *data = lua_newuserdata(lua, sizeof(*data));
  data->fdb = tcfdbnew();
  tcfdbsetmutex(data->fdb);
  lua_newtable(lua);
  lua_pushcfunction(lua, fdb_del);
  lua_setfield(lua, -2, "__gc");
  lua_setmetatable(lua, -2);
  lua_setfield(lua, -2, FDBDATAVAR);
  lua_pushnumber(lua, TCESUCCESS);
  lua_setfield(lua, -2, "ESUCCESS");
  lua_pushnumber(lua, TCETHREAD);
  lua_setfield(lua, -2, "ETHREAD");
  lua_pushnumber(lua, TCEINVALID);
  lua_setfield(lua, -2, "EINVALID");
  lua_pushnumber(lua, TCENOFILE);
  lua_setfield(lua, -2, "ENOFILE");
  lua_pushnumber(lua, TCENOPERM);
  lua_setfield(lua, -2, "ENOPERM");
  lua_pushnumber(lua, TCEMETA);
  lua_setfield(lua, -2, "EMETA");
  lua_pushnumber(lua, TCERHEAD);
  lua_setfield(lua, -2, "ERHEAD");
  lua_pushnumber(lua, TCEOPEN);
  lua_setfield(lua, -2, "EOPEN");
  lua_pushnumber(lua, TCECLOSE);
  lua_setfield(lua, -2, "ECLOSE");
  lua_pushnumber(lua, TCETRUNC);
  lua_setfield(lua, -2, "ETRUNC");
  lua_pushnumber(lua, TCESYNC);
  lua_setfield(lua, -2, "ESYNC");
  lua_pushnumber(lua, TCESTAT);
  lua_setfield(lua, -2, "ESTAT");
  lua_pushnumber(lua, TCESEEK);
  lua_setfield(lua, -2, "ESEEK");
  lua_pushnumber(lua, TCEREAD);
  lua_setfield(lua, -2, "EREAD");
  lua_pushnumber(lua, TCEWRITE);
  lua_setfield(lua, -2, "EWRITE");
  lua_pushnumber(lua, TCEMMAP);
  lua_setfield(lua, -2, "EMMAP");
  lua_pushnumber(lua, TCELOCK);
  lua_setfield(lua, -2, "ELOCK");
  lua_pushnumber(lua, TCEUNLINK);
  lua_setfield(lua, -2, "EUNLINK");
  lua_pushnumber(lua, TCERENAME);
  lua_setfield(lua, -2, "ERENAME");
  lua_pushnumber(lua, TCEMKDIR);
  lua_setfield(lua, -2, "EMKDIR");
  lua_pushnumber(lua, TCERMDIR);
  lua_setfield(lua, -2, "ERMDIR");
  lua_pushnumber(lua, TCEKEEP);
  lua_setfield(lua, -2, "EKEEP");
  lua_pushnumber(lua, TCENOREC);
  lua_setfield(lua, -2, "ENOREC");
  lua_pushnumber(lua, TCEMISC);
  lua_setfield(lua, -2, "EMISC");
  lua_pushnumber(lua, FDBOREADER);
  lua_setfield(lua, -2, "OREADER");
  lua_pushnumber(lua, FDBOWRITER);
  lua_setfield(lua, -2, "OWRITER");
  lua_pushnumber(lua, FDBOCREAT);
  lua_setfield(lua, -2, "OCREAT");
  lua_pushnumber(lua, FDBOTRUNC);
  lua_setfield(lua, -2, "OTRUNC");
  lua_pushnumber(lua, FDBONOLCK);
  lua_setfield(lua, -2, "ONOLCK");
  lua_pushnumber(lua, FDBOLCKNB);
  lua_setfield(lua, -2, "OLCKNB");
  lua_pushcfunction(lua, fdb_errmsg);
  lua_setfield(lua, -2, "errmsg");
  lua_pushcfunction(lua, fdb_ecode);
  lua_setfield(lua, -2, "ecode");
  lua_pushcfunction(lua, fdb_tune);
  lua_setfield(lua, -2, "tune");
  lua_pushcfunction(lua, fdb_open);
  lua_setfield(lua, -2, "open");
  lua_pushcfunction(lua, fdb_close);
  lua_setfield(lua, -2, "close");
  lua_pushcfunction(lua, fdb_put);
  lua_setfield(lua, -2, "put");
  lua_pushcfunction(lua, fdb_putkeep);
  lua_setfield(lua, -2, "putkeep");
  lua_pushcfunction(lua, fdb_putcat);
  lua_setfield(lua, -2, "putcat");
  lua_pushcfunction(lua, fdb_out);
  lua_setfield(lua, -2, "out");
  lua_pushcfunction(lua, fdb_get);
  lua_setfield(lua, -2, "get");
  lua_pushcfunction(lua, fdb_vsiz);
  lua_setfield(lua, -2, "vsiz");
  lua_pushcfunction(lua, fdb_iterinit);
  lua_setfield(lua, -2, "iterinit");
  lua_pushcfunction(lua, fdb_iternext);
  lua_setfield(lua, -2, "iternext");
  lua_pushcfunction(lua, fdb_range);
  lua_setfield(lua, -2, "range");
  lua_pushcfunction(lua, fdb_addint);
  lua_setfield(lua, -2, "addint");
  lua_pushcfunction(lua, fdb_adddouble);
  lua_setfield(lua, -2, "adddouble");
  lua_pushcfunction(lua, fdb_sync);
  lua_setfield(lua, -2, "sync");
  lua_pushcfunction(lua, fdb_optimize);
  lua_setfield(lua, -2, "optimize");
  lua_pushcfunction(lua, fdb_vanish);
  lua_setfield(lua, -2, "vanish");
  lua_pushcfunction(lua, fdb_copy);
  lua_setfield(lua, -2, "copy");
  lua_pushcfunction(lua, fdb_tranbegin);
  lua_setfield(lua, -2, "tranbegin");
  lua_pushcfunction(lua, fdb_trancommit);
  lua_setfield(lua, -2, "trancommit");
  lua_pushcfunction(lua, fdb_tranabort);
  lua_setfield(lua, -2, "tranabort");
  lua_pushcfunction(lua, fdb_path);
  lua_setfield(lua, -2, "path");
  lua_pushcfunction(lua, fdb_rnum);
  lua_setfield(lua, -2, "rnum");
  lua_pushcfunction(lua, fdb_fsiz);
  lua_setfield(lua, -2, "fsiz");
  lua_pushcfunction(lua, fdb_foreach);
  lua_setfield(lua, -2, "foreach");
  lua_pushcfunction(lua, fdb_pairs);
  lua_setfield(lua, -2, "pairs");
  lua_pushcfunction(lua, fdb_next);
  lua_setfield(lua, -2, "next");
  lua_newtable(lua);
  lua_pushcfunction(lua, fdb_rnum);
  lua_setfield(lua, -2, "__len");
  lua_pushcfunction(lua, fdb_get);
  lua_setfield(lua, -2, "__index");
  lua_pushcfunction(lua, fdb_put);
  lua_setfield(lua, -2, "__newindex");
  lua_setmetatable(lua, -2);
  return 1;
}


/* for del method */
static int fdb_del(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_isuserdata(lua, 1)){
    lua_pushstring(lua, "__gc: invalid arguments");
    lua_error(lua);
  }
  FDBDATA *data = lua_touserdata(lua, 1);
  TCFDB *fdb = data->fdb;
  tcfdbdel(fdb);
  return 0;
}


/* for errmsg method */
static int fdb_errmsg(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "errmsg: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  int ecode = argc > 1 ? lua_tointeger(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "errmsg: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(ecode == -1) ecode = tcfdbecode(fdb);
  lua_pushstring(lua, tcfdberrmsg(ecode));
  return 1;
}


/* for ecode method */
static int fdb_ecode(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "ecode: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "ecode: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  lua_pushnumber(lua, tcfdbecode(fdb));
  return 1;
}


/* for tune method */
static int fdb_tune(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tune: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  int32_t width = argc > 1 ? lua_tonumber(lua, 2) : -1;
  int64_t limsiz = argc > 2 ? lua_tonumber(lua, 3) : -1;
  if(!data){
    lua_pushstring(lua, "tune: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbtune(fdb, width, limsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for open method */
static int fdb_open(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  const char *path = lua_tostring(lua, 2);
  int omode = argc > 2 ? lua_tointeger(lua, 3) : FDBOREADER;
  if(!data || !path){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbopen(fdb, path, omode)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for close method */
static int fdb_close(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "close: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "close: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbclose(fdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for put method */
static int fdb_put(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbput2(fdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putkeep method */
static int fdb_putkeep(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putkeep: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "putkeep: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbputkeep2(fdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putcat method */
static int fdb_putcat(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putcat: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "putcat: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbputcat2(fdb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for out method */
static int fdb_out(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbout2(fdb, kbuf, ksiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for get method */
static int fdb_get(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "get: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "get: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  int vsiz;
  char *vbuf = tcfdbget2(fdb, kbuf, ksiz, &vsiz);
  if(vbuf){
    lua_pushlstring(lua, vbuf, vsiz);
    tcfree(vbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for vsiz method */
static int fdb_vsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "vsiz: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  lua_pushnumber(lua, tcfdbvsiz2(fdb, kbuf, ksiz));
  return 1;
}


/* for iterinit method */
static int fdb_iterinit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "iterinit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "iterinit: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbiterinit(fdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for iternext method */
static int fdb_iternext(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "iternext: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "iternext: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  int ksiz;
  char *kbuf = tcfdbiternext2(fdb, &ksiz);
  if(kbuf){
    lua_pushlstring(lua, kbuf, ksiz);
    tcfree(kbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for range method */
static int fdb_range(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "range: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  size_t isiz;
  const char *ibuf = lua_tolstring(lua, 2, &isiz);
  int max = argc > 2 ? lua_tonumber(lua, 3) : -1;
  if(!data || !ibuf){
    lua_pushstring(lua, "range: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  TCLIST *keys = tcfdbrange4(fdb, ibuf, isiz, max);
  tclisttotable(lua, keys);
  tclistdel(keys);
  return 1;
}


/* for addint method */
static int fdb_addint(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "addint: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  int num = lua_tonumber(lua, 3);
  if(!data){
    lua_pushstring(lua, "addint: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  num = tcfdbaddint(fdb, tcfdbkeytoid(kbuf, ksiz), num);
  if(num == INT_MIN){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/* for adddouble method */
static int fdb_adddouble(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "adddouble: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  double num = lua_tonumber(lua, 3);
  if(!data){
    lua_pushstring(lua, "adddouble: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  num = tcfdbadddouble(fdb, tcfdbkeytoid(kbuf, ksiz), num);
  if(isnan(num)){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/* for sync method */
static int fdb_sync(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "sync: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "sync: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbsync(fdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for optimize method */
static int fdb_optimize(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "optimize: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  int32_t width = argc > 1 ? lua_tonumber(lua, 2) : -1;
  int64_t limsiz = argc > 2 ? lua_tonumber(lua, 3) : -1;
  if(!data){
    lua_pushstring(lua, "optimize: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdboptimize(fdb, width, limsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for vanish method */
static int fdb_vanish(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vanish: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "vanish: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbvanish(fdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for copy method */
static int fdb_copy(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "copy: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  const char *path = lua_tostring(lua, 2);
  if(!data || !path){
    lua_pushstring(lua, "copy: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbcopy(fdb, path)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for tranbegin method */
static int fdb_tranbegin(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tranbegin: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "tranbegin: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbtranbegin(fdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for trancommit method */
static int fdb_trancommit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "trancommit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "trancommit: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbtrancommit(fdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for tranabort method */
static int fdb_tranabort(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tranabort: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "tranabort: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(tcfdbtranabort(fdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for path method */
static int fdb_path(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "path: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "path: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  const char *path = tcfdbpath(fdb);
  if(path){
    lua_pushstring(lua, path);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for rnum method */
static int fdb_rnum(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "rnum: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "rnum: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  lua_pushnumber(lua, tcfdbrnum(fdb));
  return 1;
}


/* for fsiz method */
static int fdb_fsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "fsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "fsiz: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  lua_pushnumber(lua, tcfdbfsiz(fdb));
  return 1;
}


/* for foreach method */
static int fdb_foreach(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "foreach: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data || !lua_isfunction(lua, 2)){
    lua_pushstring(lua, "foreach: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  FUNCOP *funcop = tcmalloc(sizeof(*funcop));
  funcop->lua = lua;
  funcop->fname = tcsprintf("_iterrec_%llX",
                            (unsigned long long)((tctime() - 1195786800) * 1000000));
  lua_pushvalue(lua, 2);
  lua_setglobal(lua, funcop->fname);
  if(tcfdbforeach(fdb, (TCITER)util_iterrec, funcop)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  lua_pushnil(lua);
  lua_setglobal(lua, funcop->fname);
  tcfree(funcop->fname);
  tcfree(funcop);
  return 1;
}


/* for pairs method */
static int fdb_pairs(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "pairs: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "pairs: invalid arguments");
    lua_error(lua);
  }
  lua_pushcfunction(lua, fdb_next);
  lua_pushvalue(lua, 1);
  lua_pushnil(lua);
  return 3;
}


/* for next method */
static int fdb_next(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, FDBDATAVAR);
  FDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  TCFDB *fdb = data->fdb;
  if(lua_isnil(lua, 2) && !tcfdbiterinit(fdb)){
    lua_pushnil(lua);
    return 1;
  }
  int ksiz;
  char *kbuf = tcfdbiternext2(fdb, &ksiz);
  if(kbuf){
    lua_pushlstring(lua, kbuf, ksiz);
    int vsiz;
    char *vbuf = tcfdbget2(fdb, kbuf, ksiz, &vsiz);
    if(vbuf){
      lua_pushlstring(lua, vbuf, vsiz);
      tcfree(vbuf);
    } else {
      lua_pushnil(lua);
    }
    tcfree(kbuf);
  } else {
    lua_pushnil(lua);
    lua_pushnil(lua);
  }
  return 2;
}


/* initialization of table database */
static void tdb_init(lua_State *lua){
  lua_pushcfunction(lua, tdb_new);
  lua_setfield(lua, -2, "tdbnew");
}


/* for new function */
static int tdb_new(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "tdbnew: invalid arguments");
    lua_error(lua);
  }
  lua_newtable(lua);
  TDBDATA *data = lua_newuserdata(lua, sizeof(*data));
  data->tdb = tctdbnew();
  tctdbsetmutex(data->tdb);
  lua_newtable(lua);
  lua_pushcfunction(lua, tdb_del);
  lua_setfield(lua, -2, "__gc");
  lua_setmetatable(lua, -2);
  lua_setfield(lua, -2, TDBDATAVAR);
  lua_pushnumber(lua, TCESUCCESS);
  lua_setfield(lua, -2, "ESUCCESS");
  lua_pushnumber(lua, TCETHREAD);
  lua_setfield(lua, -2, "ETHREAD");
  lua_pushnumber(lua, TCEINVALID);
  lua_setfield(lua, -2, "EINVALID");
  lua_pushnumber(lua, TCENOFILE);
  lua_setfield(lua, -2, "ENOFILE");
  lua_pushnumber(lua, TCENOPERM);
  lua_setfield(lua, -2, "ENOPERM");
  lua_pushnumber(lua, TCEMETA);
  lua_setfield(lua, -2, "EMETA");
  lua_pushnumber(lua, TCERHEAD);
  lua_setfield(lua, -2, "ERHEAD");
  lua_pushnumber(lua, TCEOPEN);
  lua_setfield(lua, -2, "EOPEN");
  lua_pushnumber(lua, TCECLOSE);
  lua_setfield(lua, -2, "ECLOSE");
  lua_pushnumber(lua, TCETRUNC);
  lua_setfield(lua, -2, "ETRUNC");
  lua_pushnumber(lua, TCESYNC);
  lua_setfield(lua, -2, "ESYNC");
  lua_pushnumber(lua, TCESTAT);
  lua_setfield(lua, -2, "ESTAT");
  lua_pushnumber(lua, TCESEEK);
  lua_setfield(lua, -2, "ESEEK");
  lua_pushnumber(lua, TCEREAD);
  lua_setfield(lua, -2, "EREAD");
  lua_pushnumber(lua, TCEWRITE);
  lua_setfield(lua, -2, "EWRITE");
  lua_pushnumber(lua, TCEMMAP);
  lua_setfield(lua, -2, "EMMAP");
  lua_pushnumber(lua, TCELOCK);
  lua_setfield(lua, -2, "ELOCK");
  lua_pushnumber(lua, TCEUNLINK);
  lua_setfield(lua, -2, "EUNLINK");
  lua_pushnumber(lua, TCERENAME);
  lua_setfield(lua, -2, "ERENAME");
  lua_pushnumber(lua, TCEMKDIR);
  lua_setfield(lua, -2, "EMKDIR");
  lua_pushnumber(lua, TCERMDIR);
  lua_setfield(lua, -2, "ERMDIR");
  lua_pushnumber(lua, TCEKEEP);
  lua_setfield(lua, -2, "EKEEP");
  lua_pushnumber(lua, TCENOREC);
  lua_setfield(lua, -2, "ENOREC");
  lua_pushnumber(lua, TCEMISC);
  lua_setfield(lua, -2, "EMISC");
  lua_pushnumber(lua, TDBTLARGE);
  lua_setfield(lua, -2, "TLARGE");
  lua_pushnumber(lua, TDBTDEFLATE);
  lua_setfield(lua, -2, "TDEFLATE");
  lua_pushnumber(lua, TDBTBZIP);
  lua_setfield(lua, -2, "TBZIP");
  lua_pushnumber(lua, TDBTTCBS);
  lua_setfield(lua, -2, "TTCBS");
  lua_pushnumber(lua, TDBOREADER);
  lua_setfield(lua, -2, "OREADER");
  lua_pushnumber(lua, TDBOWRITER);
  lua_setfield(lua, -2, "OWRITER");
  lua_pushnumber(lua, TDBOCREAT);
  lua_setfield(lua, -2, "OCREAT");
  lua_pushnumber(lua, TDBOTRUNC);
  lua_setfield(lua, -2, "OTRUNC");
  lua_pushnumber(lua, TDBONOLCK);
  lua_setfield(lua, -2, "ONOLCK");
  lua_pushnumber(lua, TDBOLCKNB);
  lua_setfield(lua, -2, "OLCKNB");
  lua_pushnumber(lua, TDBOTSYNC);
  lua_setfield(lua, -2, "OTSYNC");
  lua_pushnumber(lua, TDBITLEXICAL);
  lua_setfield(lua, -2, "ITLEXICAL");
  lua_pushnumber(lua, TDBITDECIMAL);
  lua_setfield(lua, -2, "ITDECIMAL");
  lua_pushnumber(lua, TDBITTOKEN);
  lua_setfield(lua, -2, "ITTOKEN");
  lua_pushnumber(lua, TDBITQGRAM);
  lua_setfield(lua, -2, "ITQGRAM");
  lua_pushnumber(lua, TDBITVOID);
  lua_setfield(lua, -2, "ITVOID");
  lua_pushnumber(lua, TDBITKEEP);
  lua_setfield(lua, -2, "ITKEEP");
  lua_pushcfunction(lua, tdb_errmsg);
  lua_setfield(lua, -2, "errmsg");
  lua_pushcfunction(lua, tdb_ecode);
  lua_setfield(lua, -2, "ecode");
  lua_pushcfunction(lua, tdb_tune);
  lua_setfield(lua, -2, "tune");
  lua_pushcfunction(lua, tdb_setcache);
  lua_setfield(lua, -2, "setcache");
  lua_pushcfunction(lua, tdb_setxmsiz);
  lua_setfield(lua, -2, "setxmsiz");
  lua_pushcfunction(lua, tdb_setdfunit);
  lua_setfield(lua, -2, "setdfunit");
  lua_pushcfunction(lua, tdb_open);
  lua_setfield(lua, -2, "open");
  lua_pushcfunction(lua, tdb_close);
  lua_setfield(lua, -2, "close");
  lua_pushcfunction(lua, tdb_put);
  lua_setfield(lua, -2, "put");
  lua_pushcfunction(lua, tdb_putkeep);
  lua_setfield(lua, -2, "putkeep");
  lua_pushcfunction(lua, tdb_putcat);
  lua_setfield(lua, -2, "putcat");
  lua_pushcfunction(lua, tdb_out);
  lua_setfield(lua, -2, "out");
  lua_pushcfunction(lua, tdb_get);
  lua_setfield(lua, -2, "get");
  lua_pushcfunction(lua, tdb_vsiz);
  lua_setfield(lua, -2, "vsiz");
  lua_pushcfunction(lua, tdb_iterinit);
  lua_setfield(lua, -2, "iterinit");
  lua_pushcfunction(lua, tdb_iternext);
  lua_setfield(lua, -2, "iternext");
  lua_pushcfunction(lua, tdb_fwmkeys);
  lua_setfield(lua, -2, "fwmkeys");
  lua_pushcfunction(lua, tdb_addint);
  lua_setfield(lua, -2, "addint");
  lua_pushcfunction(lua, tdb_adddouble);
  lua_setfield(lua, -2, "adddouble");
  lua_pushcfunction(lua, tdb_sync);
  lua_setfield(lua, -2, "sync");
  lua_pushcfunction(lua, tdb_optimize);
  lua_setfield(lua, -2, "optimize");
  lua_pushcfunction(lua, tdb_vanish);
  lua_setfield(lua, -2, "vanish");
  lua_pushcfunction(lua, tdb_copy);
  lua_setfield(lua, -2, "copy");
  lua_pushcfunction(lua, tdb_tranbegin);
  lua_setfield(lua, -2, "tranbegin");
  lua_pushcfunction(lua, tdb_trancommit);
  lua_setfield(lua, -2, "trancommit");
  lua_pushcfunction(lua, tdb_tranabort);
  lua_setfield(lua, -2, "tranabort");
  lua_pushcfunction(lua, tdb_path);
  lua_setfield(lua, -2, "path");
  lua_pushcfunction(lua, tdb_rnum);
  lua_setfield(lua, -2, "rnum");
  lua_pushcfunction(lua, tdb_fsiz);
  lua_setfield(lua, -2, "fsiz");
  lua_pushcfunction(lua, tdb_setindex);
  lua_setfield(lua, -2, "setindex");
  lua_pushcfunction(lua, tdb_genuid);
  lua_setfield(lua, -2, "genuid");
  lua_pushcfunction(lua, tdb_foreach);
  lua_setfield(lua, -2, "foreach");
  lua_pushcfunction(lua, tdb_pairs);
  lua_setfield(lua, -2, "pairs");
  lua_pushcfunction(lua, tdb_next);
  lua_setfield(lua, -2, "next");
  lua_newtable(lua);
  lua_pushcfunction(lua, tdb_rnum);
  lua_setfield(lua, -2, "__len");
  lua_pushcfunction(lua, tdb_get);
  lua_setfield(lua, -2, "__index");
  lua_pushcfunction(lua, tdb_put);
  lua_setfield(lua, -2, "__newindex");
  lua_setmetatable(lua, -2);
  return 1;
}


/* for del method */
static int tdb_del(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_isuserdata(lua, 1)){
    lua_pushstring(lua, "__gc: invalid arguments");
    lua_error(lua);
  }
  TDBDATA *data = lua_touserdata(lua, 1);
  TCTDB *tdb = data->tdb;
  tctdbdel(tdb);
  return 0;
}


/* for errmsg method */
static int tdb_errmsg(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "errmsg: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  int ecode = argc > 1 ? lua_tointeger(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "errmsg: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(ecode == -1) ecode = tctdbecode(tdb);
  lua_pushstring(lua, tctdberrmsg(ecode));
  return 1;
}


/* for ecode method */
static int tdb_ecode(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "ecode: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "ecode: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  lua_pushnumber(lua, tctdbecode(tdb));
  return 1;
}


/* for tune method */
static int tdb_tune(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 5 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tune: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  int64_t bnum = argc > 1 ? lua_tonumber(lua, 2) : -1;
  int32_t apow = argc > 2 ? lua_tonumber(lua, 3) : -1;
  int32_t fpow = argc > 3 ? lua_tonumber(lua, 4) : -1;
  int32_t opts = argc > 4 ? lua_tointeger(lua, 5) : 0;
  if(!data){
    lua_pushstring(lua, "tune: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbtune(tdb, bnum, apow, fpow, opts)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for setcache method */
static int tdb_setcache(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 4 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setcache: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  int32_t rcnum = argc > 1 ? lua_tonumber(lua, 2) : -1;
  int32_t lcnum = argc > 2 ? lua_tonumber(lua, 3) : -1;
  int32_t ncnum = argc > 3 ? lua_tonumber(lua, 4) : -1;
  if(!data){
    lua_pushstring(lua, "setcache: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbsetcache(tdb, rcnum, lcnum, ncnum)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for setxmsiz method */
static int tdb_setxmsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setxmsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  int64_t xmsiz = argc > 1 ? lua_tonumber(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "setxmsiz: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbsetxmsiz(tdb, xmsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for setdfunit method */
static int tdb_setdfunit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setdfunit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  int32_t dfunit = argc > 1 ? lua_tonumber(lua, 2) : -1;
  if(!data){
    lua_pushstring(lua, "setdfunit: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbsetdfunit(tdb, dfunit)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for open method */
static int tdb_open(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  const char *path = lua_tostring(lua, 2);
  int omode = argc > 2 ? lua_tointeger(lua, 3) : TDBOREADER;
  if(!data || !path){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbopen(tdb, path, omode)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for close method */
static int tdb_close(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "close: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "close: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbclose(tdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for put method */
static int tdb_put(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  size_t pksiz;
  const char *pkbuf = lua_tolstring(lua, 2, &pksiz);
  if(!data || !pkbuf || !lua_istable(lua, 3)){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  TCMAP *cols = tabletotcmap(lua, 3);
  TCTDB *tdb = data->tdb;
  if(tctdbput(tdb, pkbuf, pksiz, cols)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  tcmapdel(cols);
  return 1;
}


/* for putkeep method */
static int tdb_putkeep(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putkeep: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  size_t pksiz;
  const char *pkbuf = lua_tolstring(lua, 2, &pksiz);
  if(!data || !pkbuf || !lua_istable(lua, 3)){
    lua_pushstring(lua, "putkeep: invalid arguments");
    lua_error(lua);
  }
  TCMAP *cols = tabletotcmap(lua, 3);
  TCTDB *tdb = data->tdb;
  if(tctdbputkeep(tdb, pkbuf, pksiz, cols)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  tcmapdel(cols);
  return 1;
}


/* for putcat method */
static int tdb_putcat(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putcat: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  size_t pksiz;
  const char *pkbuf = lua_tolstring(lua, 2, &pksiz);
  if(!data || !pkbuf || !lua_istable(lua, 3)){
    lua_pushstring(lua, "putcat: invalid arguments");
    lua_error(lua);
  }
  TCMAP *cols = tabletotcmap(lua, 3);
  TCTDB *tdb = data->tdb;
  if(tctdbputcat(tdb, pkbuf, pksiz, cols)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  tcmapdel(cols);
  return 1;
}


/* for out method */
static int tdb_out(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  size_t pksiz;
  const char *pkbuf = lua_tolstring(lua, 2, &pksiz);
  if(!data || !pkbuf){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbout(tdb, pkbuf, pksiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for get method */
static int tdb_get(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "get: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  size_t pksiz;
  const char *pkbuf = lua_tolstring(lua, 2, &pksiz);
  if(!data || !pkbuf){
    lua_pushstring(lua, "get: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  TCMAP *cols = tctdbget(tdb, pkbuf, pksiz);
  if(cols){
    tcmaptotable(lua, cols);
    tcmapdel(cols);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for vsiz method */
static int tdb_vsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  size_t pksiz;
  const char *pkbuf = lua_tolstring(lua, 2, &pksiz);
  if(!data || !pkbuf){
    lua_pushstring(lua, "vsiz: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  lua_pushnumber(lua, tctdbvsiz(tdb, pkbuf, pksiz));
  return 1;
}


/* for iterinit method */
static int tdb_iterinit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "iterinit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "iterinit: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbiterinit(tdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for iternext method */
static int tdb_iternext(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "iternext: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "iternext: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  int pksiz;
  char *pkbuf = tctdbiternext(tdb, &pksiz);
  if(pkbuf){
    lua_pushlstring(lua, pkbuf, pksiz);
    tcfree(pkbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for fwmkeys method */
static int tdb_fwmkeys(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "fwmkeys: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  size_t psiz;
  const char *pbuf = lua_tolstring(lua, 2, &psiz);
  int max = argc > 2 && lua_isnumber(lua, 3) ? lua_tonumber(lua, 3) : -1;
  if(!data || !pbuf){
    lua_pushstring(lua, "fwmkeys: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  TCLIST *pkeys = tctdbfwmkeys(tdb, pbuf, psiz, max);
  tclisttotable(lua, pkeys);
  tclistdel(pkeys);
  return 1;
}


/* for addint method */
static int tdb_addint(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "addint: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  size_t pksiz;
  const char *pkbuf = lua_tolstring(lua, 2, &pksiz);
  int num = lua_tonumber(lua, 3);
  if(!data){
    lua_pushstring(lua, "addint: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  num = tctdbaddint(tdb, pkbuf, pksiz, num);
  if(num == INT_MIN){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/* for adddouble method */
static int tdb_adddouble(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "adddouble: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  size_t pksiz;
  const char *pkbuf = lua_tolstring(lua, 2, &pksiz);
  double num = lua_tonumber(lua, 3);
  if(!data){
    lua_pushstring(lua, "adddouble: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  num = tctdbadddouble(tdb, pkbuf, pksiz, num);
  if(isnan(num)){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/* for sync method */
static int tdb_sync(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "sync: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "sync: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbsync(tdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for optimize method */
static int tdb_optimize(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 5 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "optimize: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  int64_t bnum = argc > 1 ? lua_tonumber(lua, 2) : -1;
  int32_t apow = argc > 2 ? lua_tonumber(lua, 3) : -1;
  int32_t fpow = argc > 3 ? lua_tonumber(lua, 4) : -1;
  int32_t opts = argc > 4 ? lua_tointeger(lua, 5) : UINT8_MAX;
  if(!data){
    lua_pushstring(lua, "optimize: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdboptimize(tdb, bnum, apow, fpow, opts)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for vanish method */
static int tdb_vanish(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vanish: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "vanish: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbvanish(tdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for copy method */
static int tdb_copy(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "copy: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  const char *path = lua_tostring(lua, 2);
  if(!data || !path){
    lua_pushstring(lua, "copy: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbcopy(tdb, path)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for tranbegin method */
static int tdb_tranbegin(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tranbegin: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "tranbegin: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbtranbegin(tdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for trancommit method */
static int tdb_trancommit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "trancommit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "trancommit: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbtrancommit(tdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for tranabort method */
static int tdb_tranabort(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tranabort: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "tranabort: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbtranabort(tdb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for path method */
static int tdb_path(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "path: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "path: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  const char *path = tctdbpath(tdb);
  if(path){
    lua_pushstring(lua, path);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for rnum method */
static int tdb_rnum(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "rnum: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "rnum: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  lua_pushnumber(lua, tctdbrnum(tdb));
  return 1;
}


/* for fsiz method */
static int tdb_fsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "fsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "fsiz: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  lua_pushnumber(lua, tctdbfsiz(tdb));
  return 1;
}


/* for setindex method */
static int tdb_setindex(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setindex: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  const char *name = lua_tostring(lua, 2);
  int type = lua_tointeger(lua, 3);
  if(!data || !name){
    lua_pushstring(lua, "setindex: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(tctdbsetindex(tdb, name, type)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for genuid method */
static int tdb_genuid(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setindex: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  TCTDB *tdb = data->tdb;
  lua_pushnumber(lua, tctdbgenuid(tdb));
  return 1;
}


/* for foreach method */
static int tdb_foreach(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "foreach: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data || !lua_isfunction(lua, 2)){
    lua_pushstring(lua, "foreach: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  FUNCOP *funcop = tcmalloc(sizeof(*funcop));
  funcop->lua = lua;
  funcop->fname = tcsprintf("_iterrec_%llX",
                            (unsigned long long)((tctime() - 1195786800) * 1000000));
  lua_pushvalue(lua, 2);
  lua_setglobal(lua, funcop->fname);
  if(tctdbforeach(tdb, (TCITER)util_iterrec, funcop)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  lua_pushnil(lua);
  lua_setglobal(lua, funcop->fname);
  tcfree(funcop->fname);
  tcfree(funcop);
  return 1;
}


/* for pairs method */
static int tdb_pairs(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "pairs: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "pairs: invalid arguments");
    lua_error(lua);
  }
  lua_pushcfunction(lua, tdb_next);
  lua_pushvalue(lua, 1);
  lua_pushnil(lua);
  return 3;
}


/* for next method */
static int tdb_next(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = data->tdb;
  if(lua_isnil(lua, 2) && !tctdbiterinit(tdb)){
    lua_pushnil(lua);
    return 1;
  }
  int ksiz;
  char *kbuf = tctdbiternext(tdb, &ksiz);
  if(kbuf){
    lua_pushlstring(lua, kbuf, ksiz);
    TCMAP *cols = tctdbget(tdb, kbuf, ksiz);
    if(cols){
      tcmaptotable(lua, cols);
      tcmapdel(cols);
    } else {
      lua_pushnil(lua);
    }
    tcfree(kbuf);
  } else {
    lua_pushnil(lua);
    lua_pushnil(lua);
  }
  return 2;
}


/* for init method */
static void tdbqry_init(lua_State *lua){
  lua_pushcfunction(lua, tdbqry_new);
  lua_setfield(lua, -2, "tdbqrynew");
}


/* iterator for proc method */
static int tdbqry_procrec(const void *pkbuf, int pksiz, TCMAP *cols, FUNCOP *funcop){
  lua_State *lua = funcop->lua;
  int top = lua_gettop(lua);
  tcmaptotable(lua, cols);
  lua_getglobal(lua, funcop->fname);
  lua_pushlstring(lua, pkbuf, pksiz);
  lua_pushvalue(lua, top + 1);
  int rv = lua_pcall(lua, 2, 1, 0) == 0 ? lua_tointeger(lua, -1) : TDBQPSTOP;
  if(rv & TDBQPPUT){
    tcmapclear(cols);
    char nnbuf[NUMBUFSIZ], vnbuf[NUMBUFSIZ];
    lua_pushnil(lua);
    while(lua_next(lua, top + 1) != 0){
      const char *nbuf = NULL;
      size_t nsiz = 0;
      switch(lua_type(lua, -2)){
      case LUA_TNUMBER:
        nsiz = sprintf(nnbuf, "%lld", (long long)lua_tonumber(lua, -2));
        nbuf = nnbuf;
        break;
      case LUA_TSTRING:
        nbuf = lua_tolstring(lua, -2, &nsiz);
        break;
      }
      if(nbuf){
        const char *vbuf = NULL;
        size_t vsiz = 0;
        switch(lua_type(lua, -1)){
        case LUA_TNUMBER:
          vsiz = sprintf(vnbuf, "%lld", (long long)lua_tonumber(lua, -1));
          if(vsiz > sizeof(vnbuf)) vsiz = sizeof(vnbuf);
          vbuf = vnbuf;
          break;
        case LUA_TSTRING:
          vbuf = lua_tolstring(lua, -1, &vsiz);
          break;
        }
        if(vbuf) tcmapput(cols, nbuf, nsiz, vbuf, vsiz);
      }
      lua_pop(lua, 1);
    }
    lua_pop(lua, 1);
  }
  lua_settop(lua, top);
  return rv;
}


/* for new method */
static int tdbqry_new(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "tdbqry: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBDATAVAR);
  TDBDATA *tdbdata = lua_touserdata(lua, -1);
  if(!tdbdata){
    lua_pushstring(lua, "tdbqry: invalid arguments");
    lua_error(lua);
  }
  TCTDB *tdb = tdbdata->tdb;
  lua_newtable(lua);
  TDBQRYDATA *qrydata = lua_newuserdata(lua, sizeof(*qrydata));
  qrydata->qry = tctdbqrynew(tdb);
  lua_newtable(lua);
  lua_pushcfunction(lua, tdbqry_del);
  lua_setfield(lua, -2, "__gc");
  lua_setmetatable(lua, -2);
  lua_setfield(lua, -2, TDBQRYDATAVAR);
  lua_pushvalue(lua, 1);
  lua_setfield(lua, -2, TDBDATAVAR);



  lua_pushnumber(lua, TDBQCSTREQ);
  lua_setfield(lua, -2, "QCSTREQ");
  lua_pushnumber(lua, TDBQCSTRINC);
  lua_setfield(lua, -2, "QCSTRINC");
  lua_pushnumber(lua, TDBQCSTRBW);
  lua_setfield(lua, -2, "QCSTRBW");
  lua_pushnumber(lua, TDBQCSTREW);
  lua_setfield(lua, -2, "QCSTREW");
  lua_pushnumber(lua, TDBQCSTRAND);
  lua_setfield(lua, -2, "QCSTRAND");
  lua_pushnumber(lua, TDBQCSTROR);
  lua_setfield(lua, -2, "QCSTROR");
  lua_pushnumber(lua, TDBQCSTROREQ);
  lua_setfield(lua, -2, "QCSTROREQ");
  lua_pushnumber(lua, TDBQCSTRRX);
  lua_setfield(lua, -2, "QCSTRRX");
  lua_pushnumber(lua, TDBQCNUMEQ);
  lua_setfield(lua, -2, "QCNUMEQ");
  lua_pushnumber(lua, TDBQCNUMGT);
  lua_setfield(lua, -2, "QCNUMGT");
  lua_pushnumber(lua, TDBQCNUMGE);
  lua_setfield(lua, -2, "QCNUMGE");
  lua_pushnumber(lua, TDBQCNUMLT);
  lua_setfield(lua, -2, "QCNUMLT");
  lua_pushnumber(lua, TDBQCNUMLE);
  lua_setfield(lua, -2, "QCNUMLE");
  lua_pushnumber(lua, TDBQCNUMBT);
  lua_setfield(lua, -2, "QCNUMBT");
  lua_pushnumber(lua, TDBQCNUMOREQ);
  lua_setfield(lua, -2, "QCNUMOREQ");
  lua_pushnumber(lua, TDBQCFTSPH);
  lua_setfield(lua, -2, "QCFTSPH");
  lua_pushnumber(lua, TDBQCFTSAND);
  lua_setfield(lua, -2, "QCFTSAND");
  lua_pushnumber(lua, TDBQCFTSOR);
  lua_setfield(lua, -2, "QCFTSOR");
  lua_pushnumber(lua, TDBQCFTSEX);
  lua_setfield(lua, -2, "QCFTSEX");
  lua_pushnumber(lua, TDBQCNEGATE);
  lua_setfield(lua, -2, "QCNEGATE");
  lua_pushnumber(lua, TDBQCNOIDX);
  lua_setfield(lua, -2, "QCNOIDX");
  lua_pushnumber(lua, TDBQOSTRASC);
  lua_setfield(lua, -2, "QOSTRASC");
  lua_pushnumber(lua, TDBQOSTRDESC);
  lua_setfield(lua, -2, "QOSTRDESC");
  lua_pushnumber(lua, TDBQONUMASC);
  lua_setfield(lua, -2, "QONUMASC");
  lua_pushnumber(lua, TDBQONUMDESC);
  lua_setfield(lua, -2, "QONUMDESC");
  lua_pushnumber(lua, TDBMSUNION);
  lua_setfield(lua, -2, "MSUNION");
  lua_pushnumber(lua, TDBMSISECT);
  lua_setfield(lua, -2, "MSISECT");
  lua_pushnumber(lua, TDBMSDIFF);
  lua_setfield(lua, -2, "MSDIFF");
  lua_pushnumber(lua, TCKWMUTAB);
  lua_setfield(lua, -2, "KWMUTAB");
  lua_pushnumber(lua, TCKWMUCTRL);
  lua_setfield(lua, -2, "KWMUCTRL");
  lua_pushnumber(lua, TCKWMUBRCT);
  lua_setfield(lua, -2, "KWMUBRCT");
  lua_pushnumber(lua, TCKWNOOVER);
  lua_setfield(lua, -2, "KWNOOVER");
  lua_pushnumber(lua, TCKWPULEAD);
  lua_setfield(lua, -2, "KWPULEAD");
  lua_pushnumber(lua, TDBQPPUT);
  lua_setfield(lua, -2, "QPPUT");
  lua_pushnumber(lua, TDBQPOUT);
  lua_setfield(lua, -2, "QPOUT");
  lua_pushnumber(lua, TDBQPSTOP);
  lua_setfield(lua, -2, "QPSTOP");
  lua_pushcfunction(lua, tdbqry_addcond);
  lua_setfield(lua, -2, "addcond");
  lua_pushcfunction(lua, tdbqry_setorder);
  lua_setfield(lua, -2, "setorder");
  lua_pushcfunction(lua, tdbqry_setlimit);
  lua_setfield(lua, -2, "setlimit");
  lua_pushcfunction(lua, tdbqry_search);
  lua_setfield(lua, -2, "search");
  lua_pushcfunction(lua, tdbqry_searchout);
  lua_setfield(lua, -2, "searchout");
  lua_pushcfunction(lua, tdbqry_proc);
  lua_setfield(lua, -2, "proc");
  lua_pushcfunction(lua, tdbqry_hint);
  lua_setfield(lua, -2, "hint");
  lua_pushcfunction(lua, tdbqry_metasearch);
  lua_setfield(lua, -2, "metasearch");
  lua_pushcfunction(lua, tdbqry_kwic);
  lua_setfield(lua, -2, "kwic");
  return 1;
}


/* for del method */
static int tdbqry_del(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_isuserdata(lua, 1)){
    lua_pushstring(lua, "__gc: invalid arguments");
    lua_error(lua);
  }
  TDBQRYDATA *data = lua_touserdata(lua, 1);
  TDBQRY *qry = data->qry;
  tctdbqrydel(qry);
  return 0;
}


/* for addcond method */
static int tdbqry_addcond(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 4 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "addcond: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBQRYDATAVAR);
  TDBQRYDATA *data = lua_touserdata(lua, -1);
  const char *name = lua_tostring(lua, 2);
  int type = lua_tointeger(lua, 3);
  const char *expr = lua_tostring(lua, 4);
  if(!data || !name || !expr){
    lua_pushstring(lua, "addcond: invalid arguments");
    lua_error(lua);
  }
  TDBQRY *qry = data->qry;
  tctdbqryaddcond(qry, name, type, expr);
  return 0;
}


/* for setorder method */
static int tdbqry_setorder(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setorder: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBQRYDATAVAR);
  TDBQRYDATA *data = lua_touserdata(lua, -1);
  const char *name = lua_tostring(lua, 2);
  int type = argc > 2 ? lua_tointeger(lua, 3) : TDBQOSTRASC;
  if(!data || !name){
    lua_pushstring(lua, "setorder: invalid arguments");
    lua_error(lua);
  }
  TDBQRY *qry = data->qry;
  tctdbqrysetorder(qry, name, type);
  return 0;
}


/* for setlimit method */
static int tdbqry_setlimit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "setlimit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBQRYDATAVAR);
  TDBQRYDATA *data = lua_touserdata(lua, -1);
  int max = argc > 1 ? lua_tointeger(lua, 2) : -1;
  int skip = argc > 2 ? lua_tointeger(lua, 3) : -1;
  if(!data){
    lua_pushstring(lua, "setorder: invalid arguments");
    lua_error(lua);
  }
  TDBQRY *qry = data->qry;
  tctdbqrysetlimit(qry, max, skip);
  return 0;
}


/* for search method */
static int tdbqry_search(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "search: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBQRYDATAVAR);
  TDBQRYDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "search: invalid arguments");
    lua_error(lua);
  }
  TDBQRY *qry = data->qry;
  TCLIST *res = tctdbqrysearch(qry);
  tclisttotable(lua, res);
  tclistdel(res);
  return 1;
}


/* for searchout method */
static int tdbqry_searchout(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "searchout: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBQRYDATAVAR);
  TDBQRYDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "searchout: invalid arguments");
    lua_error(lua);
  }
  TDBQRY *qry = data->qry;
  if(tctdbqrysearchout(qry)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for proc method */
static int tdbqry_proc(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "proc: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBQRYDATAVAR);
  TDBQRYDATA *data = lua_touserdata(lua, -1);
  if(!data || !lua_isfunction(lua, 2)){
    lua_pushstring(lua, "proc: invalid arguments");
    lua_error(lua);
  }
  TDBQRY *qry = data->qry;
  FUNCOP *funcop = tcmalloc(sizeof(*funcop));
  funcop->lua = lua;
  funcop->fname = tcsprintf("_procrec_%llX",
                            (unsigned long long)((tctime() - 1195786800) * 1000000));
  lua_pushvalue(lua, 2);
  lua_setglobal(lua, funcop->fname);
  if(tctdbqryproc(qry, (TDBQRYPROC)tdbqry_procrec, funcop)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  lua_pushnil(lua);
  lua_setglobal(lua, funcop->fname);
  tcfree(funcop->fname);
  tcfree(funcop);
  return 1;
}


/* for hint method */
static int tdbqry_hint(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "hint: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBQRYDATAVAR);
  TDBQRYDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "hint: invalid arguments");
    lua_error(lua);
  }
  TDBQRY *qry = data->qry;
  lua_pushstring(lua, tctdbqryhint(qry));
  return 1;
}


/* for metasearch method */
static int tdbqry_metasearch(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "metasearch: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBQRYDATAVAR);
  TDBQRYDATA *data = lua_touserdata(lua, -1);
  int type = argc > 2 ? lua_tointeger(lua, 3) : TDBMSUNION;
  if(!data || !lua_istable(lua, 2)){
    lua_pushstring(lua, "metasearch: invalid arguments");
    lua_error(lua);
  }
  TDBQRY *qry = data->qry;
  int num = lua_objlen(lua, 2);
  TDBQRY **qrys = tcmalloc(sizeof(*qrys) * (num + 1));
  int qnum = 0;
  qrys[qnum++] = qry;
  for(int i = 1; i <= num; i++){
    lua_rawgeti(lua, 2, i);
    if(lua_istable(lua, -1)){
      lua_getfield(lua, -1, TDBQRYDATAVAR);
      TDBQRYDATA *odata = lua_touserdata(lua, -1);
      if(odata){
        TDBQRY *oqry = odata->qry;
        qrys[qnum++] = oqry;
      }
      lua_pop(lua, 1);
    }
    lua_pop(lua, 1);
  }
  TCLIST *res = tctdbmetasearch(qrys, qnum, type);
  tclisttotable(lua, res);
  tclistdel(res);
  tcfree(qrys);
  return 1;
}


/* for kwic method */
static int tdbqry_kwic(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 5 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "kwic: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, TDBQRYDATAVAR);
  TDBQRYDATA *data = lua_touserdata(lua, -1);
  const char *name = argc > 2 ? lua_tostring(lua, 3) : NULL;
  int width = argc > 3 ? lua_tointeger(lua, 4) : -1;
  int opts = argc > 4 ? lua_tointeger(lua, 5) : 0;
  if(!data || !lua_istable(lua, 2)){
    lua_pushstring(lua, "kwic: invalid arguments");
    lua_error(lua);
  }
  TDBQRY *qry = data->qry;
  TCMAP *cols;
  if(name){
    cols = tcmapnew2(1);
    lua_getfield(lua, 2, name);
    size_t nsiz;
    const char *nbuf = lua_tolstring(lua, -1, &nsiz);
    if(nbuf) tcmapput(cols, name, strlen(name), nbuf, nsiz);
    lua_pop(lua, 1);
  } else {
    cols = tabletotcmap(lua, 2);
  }
  if(width < 0){
    width = 1 << 30;
    opts |= TCKWNOOVER | TCKWPULEAD;
  }
  TCLIST *texts = tctdbqrykwic(qry, cols, name, width, opts);
  tclisttotable(lua, texts);
  tclistdel(texts);
  tcmapdel(cols);
  return 1;
}


/* initialization of abstract database */
static void adb_init(lua_State *lua){
  lua_pushcfunction(lua, adb_new);
  lua_setfield(lua, -2, "adbnew");
}


/* for new function */
static int adb_new(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "adbnew: invalid arguments");
    lua_error(lua);
  }
  lua_newtable(lua);
  ADBDATA *data = lua_newuserdata(lua, sizeof(*data));
  data->adb = tcadbnew();
  lua_newtable(lua);
  lua_pushcfunction(lua, adb_del);
  lua_setfield(lua, -2, "__gc");
  lua_setmetatable(lua, -2);
  lua_setfield(lua, -2, ADBDATAVAR);
  lua_pushcfunction(lua, adb_open);
  lua_setfield(lua, -2, "open");
  lua_pushcfunction(lua, adb_close);
  lua_setfield(lua, -2, "close");
  lua_pushcfunction(lua, adb_put);
  lua_setfield(lua, -2, "put");
  lua_pushcfunction(lua, adb_putkeep);
  lua_setfield(lua, -2, "putkeep");
  lua_pushcfunction(lua, adb_putcat);
  lua_setfield(lua, -2, "putcat");
  lua_pushcfunction(lua, adb_out);
  lua_setfield(lua, -2, "out");
  lua_pushcfunction(lua, adb_get);
  lua_setfield(lua, -2, "get");
  lua_pushcfunction(lua, adb_vsiz);
  lua_setfield(lua, -2, "vsiz");
  lua_pushcfunction(lua, adb_iterinit);
  lua_setfield(lua, -2, "iterinit");
  lua_pushcfunction(lua, adb_iternext);
  lua_setfield(lua, -2, "iternext");
  lua_pushcfunction(lua, adb_fwmkeys);
  lua_setfield(lua, -2, "fwmkeys");
  lua_pushcfunction(lua, adb_addint);
  lua_setfield(lua, -2, "addint");
  lua_pushcfunction(lua, adb_adddouble);
  lua_setfield(lua, -2, "adddouble");
  lua_pushcfunction(lua, adb_sync);
  lua_setfield(lua, -2, "sync");
  lua_pushcfunction(lua, adb_optimize);
  lua_setfield(lua, -2, "optimize");
  lua_pushcfunction(lua, adb_vanish);
  lua_setfield(lua, -2, "vanish");
  lua_pushcfunction(lua, adb_copy);
  lua_setfield(lua, -2, "copy");
  lua_pushcfunction(lua, adb_tranbegin);
  lua_setfield(lua, -2, "tranbegin");
  lua_pushcfunction(lua, adb_trancommit);
  lua_setfield(lua, -2, "trancommit");
  lua_pushcfunction(lua, adb_tranabort);
  lua_setfield(lua, -2, "tranabort");
  lua_pushcfunction(lua, adb_path);
  lua_setfield(lua, -2, "path");
  lua_pushcfunction(lua, adb_rnum);
  lua_setfield(lua, -2, "rnum");
  lua_pushcfunction(lua, adb_size);
  lua_setfield(lua, -2, "size");
  lua_pushcfunction(lua, adb_foreach);
  lua_setfield(lua, -2, "foreach");
  lua_pushcfunction(lua, adb_pairs);
  lua_setfield(lua, -2, "pairs");
  lua_pushcfunction(lua, adb_next);
  lua_setfield(lua, -2, "next");
  lua_newtable(lua);
  lua_pushcfunction(lua, adb_rnum);
  lua_setfield(lua, -2, "__len");
  lua_pushcfunction(lua, adb_get);
  lua_setfield(lua, -2, "__index");
  lua_pushcfunction(lua, adb_put);
  lua_setfield(lua, -2, "__newindex");
  lua_setmetatable(lua, -2);
  return 1;
}


/* for del method */
static int adb_del(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_isuserdata(lua, 1)){
    lua_pushstring(lua, "__gc: invalid arguments");
    lua_error(lua);
  }
  ADBDATA *data = lua_touserdata(lua, 1);
  TCADB *adb = data->adb;
  tcadbdel(adb);
  return 0;
}


/* for open method */
static int adb_open(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  const char *name = lua_tostring(lua, 2);
  if(!data || !name){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbopen(adb, name)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for close method */
static int adb_close(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "close: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "close: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbclose(adb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for put method */
static int adb_put(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "put: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbput(adb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putkeep method */
static int adb_putkeep(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putkeep: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "putkeep: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbputkeep(adb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for putcat method */
static int adb_putcat(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "putcat: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 3, &vsiz);
  if(!data || !kbuf || !vbuf){
    lua_pushstring(lua, "putcat: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbputcat(adb, kbuf, ksiz, vbuf, vsiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for out method */
static int adb_out(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "out: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbout(adb, kbuf, ksiz)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for get method */
static int adb_get(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "get: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "get: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  int vsiz;
  char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
  if(vbuf){
    lua_pushlstring(lua, vbuf, vsiz);
    tcfree(vbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for vsiz method */
static int adb_vsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  if(!data || !kbuf){
    lua_pushstring(lua, "vsiz: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  lua_pushnumber(lua, tcadbvsiz(adb, kbuf, ksiz));
  return 1;
}


/* for iterinit method */
static int adb_iterinit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "iterinit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "iterinit: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbiterinit(adb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for iternext method */
static int adb_iternext(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "iternext: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "iternext: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  int ksiz;
  char *kbuf = tcadbiternext(adb, &ksiz);
  if(kbuf){
    lua_pushlstring(lua, kbuf, ksiz);
    tcfree(kbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for fwmkeys method */
static int adb_fwmkeys(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2 || argc > 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "fwmkeys: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  size_t psiz;
  const char *pbuf = lua_tolstring(lua, 2, &psiz);
  int max = argc > 2 && lua_isnumber(lua, 3) ? lua_tonumber(lua, 3) : -1;
  if(!data || !pbuf){
    lua_pushstring(lua, "fwmkeys: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  TCLIST *keys = tcadbfwmkeys(adb, pbuf, psiz, max);
  tclisttotable(lua, keys);
  tclistdel(keys);
  return 1;
}


/* for addint method */
static int adb_addint(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "addint: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  int num = lua_tonumber(lua, 3);
  if(!data){
    lua_pushstring(lua, "addint: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  num = tcadbaddint(adb, kbuf, ksiz, num);
  if(num == INT_MIN){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/* for adddouble method */
static int adb_adddouble(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 3 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "adddouble: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 2, &ksiz);
  double num = lua_tonumber(lua, 3);
  if(!data){
    lua_pushstring(lua, "adddouble: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  num = tcadbadddouble(adb, kbuf, ksiz, num);
  if(isnan(num)){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/* for sync method */
static int adb_sync(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "sync: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "sync: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbsync(adb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for optimize method */
static int adb_optimize(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1 || argc > 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "optimize: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  const char *params = argc > 1 ? lua_tostring(lua, 2) : NULL;
  if(!data){
    lua_pushstring(lua, "open: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadboptimize(adb, params)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for vanish method */
static int adb_vanish(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "vanish: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "vanish: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbvanish(adb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for copy method */
static int adb_copy(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "copy: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  const char *path = lua_tostring(lua, 2);
  if(!data || !path){
    lua_pushstring(lua, "copy: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbcopy(adb, path)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for tranbegin method */
static int adb_tranbegin(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tranbegin: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "tranbegin: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbtranbegin(adb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for trancommit method */
static int adb_trancommit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "trancommit: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "trancommit: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbtrancommit(adb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for tranabort method */
static int adb_tranabort(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "tranabort: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "tranabort: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(tcadbtranabort(adb)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  return 1;
}


/* for path method */
static int adb_path(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "path: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "path: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  const char *path = tcadbpath(adb);
  if(path){
    lua_pushstring(lua, path);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for rnum method */
static int adb_rnum(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "rnum: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "rnum: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  lua_pushnumber(lua, tcadbrnum(adb));
  return 1;
}


/* for size method */
static int adb_size(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "size: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "size: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  lua_pushnumber(lua, tcadbsize(adb));
  return 1;
}


/* for foreach method */
static int adb_foreach(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "foreach: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data || !lua_isfunction(lua, 2)){
    lua_pushstring(lua, "foreach: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  FUNCOP *funcop = tcmalloc(sizeof(*funcop));
  funcop->lua = lua;
  funcop->fname = tcsprintf("_iterrec_%llX",
                            (unsigned long long)((tctime() - 1195786800) * 1000000));
  lua_pushvalue(lua, 2);
  lua_setglobal(lua, funcop->fname);
  if(tcadbforeach(adb, (TCITER)util_iterrec, funcop)){
    lua_pushboolean(lua, true);
  } else {
    lua_pushboolean(lua, false);
  }
  lua_pushnil(lua);
  lua_setglobal(lua, funcop->fname);
  tcfree(funcop->fname);
  tcfree(funcop);
  return 1;
}


/* for pairs method */
static int adb_pairs(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "pairs: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "pairs: invalid arguments");
    lua_error(lua);
  }
  lua_pushcfunction(lua, adb_next);
  lua_pushvalue(lua, 1);
  lua_pushnil(lua);
  return 3;
}


/* for next method */
static int adb_next(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2 || !lua_istable(lua, 1)){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  lua_getfield(lua, 1, ADBDATAVAR);
  ADBDATA *data = lua_touserdata(lua, -1);
  if(!data){
    lua_pushstring(lua, "next: invalid arguments");
    lua_error(lua);
  }
  TCADB *adb = data->adb;
  if(lua_isnil(lua, 2) && !tcadbiterinit(adb)){
    lua_pushnil(lua);
    return 1;
  }
  int ksiz;
  char *kbuf = tcadbiternext(adb, &ksiz);
  if(kbuf){
    lua_pushlstring(lua, kbuf, ksiz);
    int vsiz;
    char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    if(vbuf){
      lua_pushlstring(lua, vbuf, vsiz);
      tcfree(vbuf);
    } else {
      lua_pushnil(lua);
    }
    tcfree(kbuf);
  } else {
    lua_pushnil(lua);
    lua_pushnil(lua);
  }
  return 2;
}



/* END OF FILE */
