#include "cbdb.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>

#include <sys/mman.h>
#include <sys/stat.h>

#if defined _WIN32 || defined __CYGWIN__
#define PATH_SLASH '\\'
#else
#define PATH_SLASH '/'
#endif


#define CBDB_PAGESIZE (sysconf(_SC_PAGE_SIZE))

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
  if(db->data.path) {
    free(db->data.path);
  }
  free(db);
}

static void cbdb_set_error(cbdb_error_t *err, cbdb_error_code_t code, char *error_string) {
  if(err) {
    err->code = code;
    err->str = error_string;
    err->errno_val = errno;
  }
}

static void cbdb_error(cbdb_t *db, cbdb_error_code_t code, char *err_fmt, ...) {
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
  cbdb_set_error(&db->error, code, db->buffer.error);
  va_end(ap);
}

static int cbdb_lock(cbdb_t *db) {
  char buf[4096];
  snprintf(buf, 4096, "%s%c%s.lock.cbdb", db->path, PATH_SLASH, db->name);
  printf("%s\n", buf);
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP;
  int fd = open(buf, O_CREAT | O_EXCL, mode);
  if(fd == -1) {
    if(errno == EEXIST) {
      cbdb_error(db, CBDB_ERROR_LOCK_FAILED, "Database is already locked");
    }
    else {
      cbdb_error(db, CBDB_ERROR_LOCK_FAILED, "Can't lock database");
    }
    return 0;
  }
  else {
    //lock file created, i don't think we need to keep its fd open
    close(fd);
    return 1;
  }
}

static int cbdb_unlock(cbdb_t *db) {
  char buf[4096];
  snprintf(buf, 4096, "%s%c%s.lock.cbdb", db->path, PATH_SLASH, db->name);
  if(access(buf, F_OK) == -1){ //no lock present, nothing to unlock
    return 1;
  }
  if(remove(buf) == 0) {
    return 1;
  }
  else {
    return 0;
  }
}

static int file_getsize(int fd, off_t *sz, cbdb_error_t *err) {
  struct stat st;
  if(fstat(fd, &st)) {
    cbdb_set_error(err, CBDB_ERROR_FILE_ACCESS, "Failed to get filesize");
    return 0;
  }
  *sz = st.st_size;
  return 1;
}

static int cbdb_open_data_file(cbdb_t *db, cbdb_error_t *err) {
  off_t sz;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  if((db->data.fd = open(db->data.path, O_CREAT, mode)) == -1) {
    cbdb_set_error(err, CBDB_ERROR_NOMEMORY, "Failed to open data file");
    return 0;
  }
  
  sz = CBDB_PAGESIZE*10;
  db->data.start = mmap(NULL, sz, PROT_READ|PROT_WRITE, MAP_SHARED, db->data.fd, 0);
  db->data.end = (char *)&db->data.start[sz];
  
  if(!file_getsize(db->data.fd, &sz, err)) {
    munmap(db->data.start, db->data.end - db->data.start);
    close(db->data.fd);
    db->data.fd = -1;
    db->data.start = NULL;
    return 0;
  }
  
  return 1;
}


cbdb_t *cbdb_open(char *path, char *name, cbdb_config_t *cf, cbdb_error_t *err) {
  cbdb_t    *db = cbdb_alloc(path, name, cf);
  if(db == NULL) {
    cbdb_set_error(err, CBDB_ERROR_NOMEMORY, "Failed to allocate memory for cbdb struct");
    return NULL;
  }

  db->config = *cf;
  strcpy(db->path, path);
  strcpy(db->name, name);
  int len = strlen(db->path);
  if(len > 0 && db->path[len-1]==PATH_SLASH) {
    db->path[len-1] = '\00';
  }
  printf("%s\n", db->path);
  if(!cbdb_lock(db)) {
    if(err) {
      *err = db->error;
    }
    cbdb_free(db);
    return NULL;
  }
  
  char buf[4096];
  snprintf(buf, 4096, "%s%c%s.data.cbdb", db->path, PATH_SLASH, db->name);
  
  /*
  //file existence check
  if(access(buf, F_OK) != -1){
    cbdb_set_error(err, CBDB_ERROR_FILE_EXISTS, "Data file already exits");
    cbdb_free(db);
    return NULL;
  }
  */
  
  if((db->data.path = malloc(strlen(buf) + 1)) == NULL) {
    cbdb_set_error(err, CBDB_ERROR_NOMEMORY, "Failed to allocate memory for data file path");
    cbdb_free(db);
    return NULL;
  }
  strcpy(db->data.path, buf);
  
  if(!cbdb_open_data_file(db, err)) {
    return NULL;
  }
  
  
  return db;
}

void cbdb_close(cbdb_t *db) {
  //TODO: close all the things
  cbdb_unlock(db);
  cbdb_free(db);
}

