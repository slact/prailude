#include "cbdb.h"
#include <stdlib.h>
#include <string.h>

cbdb_t *cbdb_create(char *path, cbdb_config_t *cf, cbdb_error_t *err) {
  cbdb_t    *db;
  size_t     sz = sizeof(*db) + cf->id_len + cf->data_len + strlen(path) + 1;
  if((db = malloc(sz)) == NULL) {
    if(err) {
      err->code = CBDB_ERROR_NOMEMORY;
      err->str = "Failed to allocate memory for cbdb struct";
    }
    return NULL;
  }
  memset(db, '\0', sz);
  
  char *cur = (char *)(&db[1]);
  db->buffer.id = cur;
  cur += cf->id_len;
  db->buffer.data = cur;
  cur += cf->data_len;
  db->path = cur;
  
  db->config = *cf;
  strcpy(db->path, path);
  return db;
}
