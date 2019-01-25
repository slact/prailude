#include "cbdb.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>

#include <sys/mman.h>

#if defined _WIN32 || defined __CYGWIN__
#define PATH_SLASH '\\'
#else
#define PATH_SLASH '/'
#endif

#define CBDB_PAGESIZE sysconf(_SC_PAGE_SIZE)

static cbdb_t *cbdb_alloc(char *path, char *name, cbdb_config_t *cf) {
  cbdb_t    *db;
  size_t     sz = sizeof(*db) + cf->id_len + cf->data_len + strlen(path) + 1 + strlen(name) + 1 + CBDB_ERROR_MAX_LEN + 1;
  if((db = malloc(sz)) == NULL) {
    return NULL;
  }
  
  memset(db, '\0', sz);
  
  char *cur = (char *)(&db[1]);
  db->buffer.id = cur;
  cur += cf->id_len;
  db->buffer.data = cur;
  cur += cf->data_len;
  db->path = cur;
  cur += strlen(path)+1;
  db->name = cur;
  cur += strlen(name)+1;
  db->buffer.error = cur;
  
  return db;
}

static void cbdb_free(cbdb_t *db) {
  free(db->data.path);
  free(db);
}

static void cbdb_set_error(cbdb_error_t *err, cbdb_error_code_t code, int errno_global, char *error_string) {
  if(err) {
    err->code = code;
    err->str = error_string;
    err->errno_val = errno_global;
  }
}

static void cbdb_error(cbdb_t *db, cbdb_error_code_t code, int errno_global, char *err_fmt, ...) {
  /*
  int num_args = 0;
  
  char *cur = err_fmt, *end = err_fmt + strlen(err_fmt);
  while(cur <= end && cur != NULL) {
    cur = strchr(cur, '%');
    if(cur && cur+1 <= end && cur[1] != '%') {
      num_args++;
    }
  }
  if(num_args == 0) { //error string is plain with no format arguments
    strncpy(db->buffer.error, err_fmt, CBDB_ERROR_MAX_LEN);
    cbdb_set_error(&db->error, code, db->buffer.error);
    return
  }*/
  va_list ap;
  va_start(ap, err_fmt);
  vsnprintf(db->buffer.error, CBDB_ERROR_MAX_LEN, err_fmt, ap);
  cbdb_set_error(&db->error, code, errno_global, db->buffer.error);
  va_end(ap);
}



//static cbdb_data_create_file(

cbdb_t *cbdb_create(char *path, char *name, cbdb_config_t *cf, cbdb_error_t *err) {
  cbdb_t    *db = cbdb_alloc(path, name, cf);
  if(db == NULL) {
    cbdb_set_error(err, CBDB_ERROR_NOMEMORY, 0, "Failed to allocate memory for cbdb struct");
    return NULL;
  }

  
  db->config = *cf;
  strcpy(db->path, path);
  strcpy(db->name, name);
  if(db->path[strlen(db->path)]==PATH_SLASH) {
    db->path[strlen(db->path)] = '\00';
  }
  
  char buf[4096];
  snprintf(buf, 4096, "%s%c%s.data.cbdb", db->path, PATH_SLASH, db->name);
  
  //file existence check
  if(access(buf, F_OK) != -1){
    cbdb_set_error(err, CBDB_ERROR_FILE_EXISTS, 0, "Data file already exits");
    return NULL;
  }
  
  if((db->data.path = malloc(strlen(buf) + 1)) == NULL) {
    cbdb_set_error(err, CBDB_ERROR_NOMEMORY, 0, "Failed to allocate memory for data file path");
    return NULL;
  }
  strcpy(db->data.path, buf);
  
  if((db->data.fd = open(db->data.path, O_CREAT)) == -1) {
    cbdb_set_error(err, CBDB_ERROR_NOMEMORY, errno, "Failed to open data file");
    return NULL;
  }
  
  db->data.ptr = mmap(NULL, CBDB_PAGESIZE*10, PROT_READ|PROT_WRITE, MAP_SHARED, db->data.fd, 0);
  
  
  return db;
}

void cbdb_close(cbdb_t *db) {
  //TODO: close all the things
  cbdb_free(db);
}
