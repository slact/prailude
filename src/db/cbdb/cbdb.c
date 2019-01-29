#include "cbdb.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/stat.h>

#if defined _WIN32 || defined __CYGWIN__
#define PATH_SLASH '\\'
#else
#define PATH_SLASH '/'
#endif


#define CBDB_PAGESIZE (sysconf(_SC_PAGE_SIZE))

static off_t cbdb_filename(cbdb_t *db, char *what, char *buf, off_t maxlen) {
  return snprintf(buf, maxlen, "%s%c%s.%s.cbdb", db->path, PATH_SLASH, db->name, what);
}

static cbdb_t *cbdb_alloc_memset(char *path, char *name, cbdb_config_t *cf, cbdb_config_index_t *index_cf) {
  cbdb_t    *db;
  size_t     sz;
  int        i;
  
  //initialize indices
  cbdb_config_index_t primary = {
    .name = "primary", .type = CBDB_INDEX_HASHTABLE, .start = CBDB_INDEX_ID, .len = cf->id_len
  };
  
  sz = sizeof(*db) + strlen(path) + 1 + strlen(name) + 1 + CBDB_ERROR_MAX_LEN + 1;
  if(cf) {
    sz += cf->id_len + cf->data_len;
  }
  
  sz += sizeof(cbdb_index_t); // no need to allocate for primary index name -- see comments below.
  if(cf) {
    for(i=0; i<cf->index_count; i++) {
      sz += sizeof(cbdb_index_t) + strlen(index_cf[i].name)+1;
    }
  }
  
  if((db = malloc(sz)) == NULL) {
    return NULL;
  }
  
  memset(db, '\0', sz);
  
  db->index = (cbdb_index_t *)(&db[1]);
  
  char *cur = (char *)(&db->index[1]);
  
  //primary index
  db->index[0].config = primary;
  //no need to copy name, it's a static string
  // althought it might be worth it to keep the names adjacent in memory later
  
  if(cf) {
    //and now the rest of the indices
    for(i=1; i<=cf->index_count; i++) {
      db->index[i].config = index_cf[i-1];
      db->index[i].config.name = cur;
      strcpy(db->index[i].config.name, index_cf[i-1].name);
      cur += strlen(index_cf[i-1].name) + 1;
    }
    
    db->config.index_count++; //add 1 for the primary index
    
    db->buffer.id = cur;
    cur += cf->id_len;
    db->buffer.data = cur;
    cur += cf->data_len;
  }
  else {
    db->config.index_count = 1; //just the primary index then
  }
  db->path = cur;
  cur += strlen(path)+1;
  db->name = cur;
  cur += strlen(name)+1;
  db->buffer.error = cur;
  
  strcpy(db->path, path);
  strcpy(db->name, name);
  
  sz = strlen(db->path);
  if(sz > 0 && db->path[sz-1]==PATH_SLASH) { // remove trailing slash
    db->path[sz-1] = '\00';
  }
  
  return db;
}

static void cbdb_free(cbdb_t *db) {
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
  char buf[1024];
  cbdb_filename(db, "lock", buf, 1024);
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
    char buf[1024];
  cbdb_filename(db, "lock", buf, 1024);
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

static int cbdb_file_getsize(cbdb_t *db, int fd, off_t *sz) {
  struct stat st;
  if(fstat(fd, &st)) {
    cbdb_error(db, CBDB_ERROR_FILE_ACCESS, "Failed to get filesize");
    return 0;
  }
  *sz = st.st_size;
  return 1;
}

static int cbdb_munmap_file(cbdb_t *db, cbdb_mmap_t *mm) {
  if(mm->start && mm->start != MAP_FAILED) {
    munmap(mm->start, mm->end - mm->start);
  }
  if(mm->fd != -1) {
    close(mm->fd);
    mm->fd = -1;
  }
  mm->start = NULL;
  return 1;
}

static int cbdb_mmap_file(cbdb_t *db, char *what, cbdb_mmap_t *mm) {
  off_t sz;
  char path[1024];
  cbdb_filename(db, what, path, 1024);
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP;
  if((mm->fd = open(path, O_RDWR | O_CREAT, mode)) == -1) {
    cbdb_error(db, CBDB_ERROR_FILE_ACCESS, "Failed to open file %s", path);
    return 0;
  }
  
  sz = CBDB_PAGESIZE*10;
  mm->start = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, mm->fd, 0);
  if(mm->start == MAP_FAILED) {
    cbdb_error(db, CBDB_ERROR_FILE_ACCESS, "Failed to mmap file %s", path);
    cbdb_munmap_file(db, mm);
    return 0;
  }
  
  mm->end = &((char *)mm->start)[sz];
  
  if(!cbdb_file_getsize(db, mm->fd, &sz)) {
    cbdb_munmap_file(db, mm);
//     return 0;
  }
  
  return 1;
}

static int cbdb_mmap_index_file(cbdb_t *db, int index_n) {
  char index_name[128];
  snprintf(index_name, 128, "index.%s", db->index[index_n].config.name);
  return cbdb_mmap_file(db, index_name, &db->index[index_n].data);
}

static off_t cbdb_data_header_write(cbdb_t *db) {
  char     *cur = db->data.start;
  int       i;
  cbdb_config_index_t *icf;
  strcpy(cur, "cbdb");
  cur += strlen("cbdb")+1;
  //cbdb format version
  *(uint16_t *)cur = htons(CBDB_FORMAT_VERSION);
  cur += sizeof(uint16_t);
  //revision
  *(uint32_t *)cur = htonl(db->config.revision);
  cur += sizeof(uint32_t);
  //length of id and data
  *(uint16_t *)cur = htons(db->config.id_len);
  cur += sizeof(uint16_t);
  *(uint16_t *)cur = htons(db->config.data_len);
  cur += sizeof(uint16_t);
  //index count
  *(uint16_t *)cur = htons(db->config.index_count);
  cur += sizeof(uint16_t);
  
  //indices
  for(i=0; i<db->config.index_count; i++) {
    icf = &db->index[i].config;
    strcpy(cur, icf->name);
    cur += strlen(cur)+1;
    
    *(uint16_t *)cur = htons((uint16_t )icf->type);
    cur += sizeof(uint16_t);
    *(uint16_t *)cur = htons((uint16_t )icf->start);
    cur += sizeof(uint16_t);
    *(uint16_t *)cur = htons((uint16_t )icf->len);
    cur += sizeof(uint16_t);
  }
  return cur - (char *)db->data.start;
}

static int cbdb_index_type_valid(cbdb_index_type_t index_type) {
  switch(index_type) {
    case CBDB_INDEX_HASHTABLE:
    case CBDB_INDEX_BTREE:
      return 1;
  }
  return 0;
}
const char *cbdb_index_type_str(cbdb_index_type_t index_type) {
  switch(index_type) {
    case CBDB_INDEX_HASHTABLE:
      return "hashtable";
    case CBDB_INDEX_BTREE:
      return "B-tree";
  }
  return "???";
}

static off_t cbdb_data_header_read(cbdb_t *db, cbdb_config_t *cf, cbdb_config_index_t *index_cf) {
  char     *cur = db->data.start;
  uint16_t  cbdb_version;
  int       i, n;
  cbdb_config_index_t idx;
  if(strcmp(cur, "cbdb") != 0) {
    cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Data file is not a cbdb file or invalid");
    return 0;
  }
  cur += strlen("cbdb")+1;
  
  cbdb_version = ntohs(*(uint16_t *)cur);
  cur += sizeof(uint16_t);
  
  cf->revision = ntohl(*(uint32_t *)cur);
  cur += sizeof(uint32_t);
  
  cf->id_len = ntohs(*(uint16_t *)cur);
  cur += sizeof(uint16_t);
  
  cf->data_len = ntohs(*(uint16_t *)cur);
  cur += sizeof(uint16_t);
  
  cf->index_count = ntohs(*(uint16_t *)cur);
  cur += sizeof(uint16_t);
  if(cf->index_count > CBDB_INDICES_MAX) {
    cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Data file invalid: too many indices defined");
    return 0;
  }
  
  
  for(i=0, n=1; i<cf->index_count; i++) {
    idx.name = cur;
    cur += strlen(cur)+1;
    
    idx.type = ntohs(*(uint16_t *)cur);
    cur += sizeof(uint16_t);
    
    idx.start = ntohs(*(uint16_t *)cur);
    cur += sizeof(uint16_t);
    
    idx.len = ntohs(*(uint16_t *)cur);
    cur += sizeof(uint16_t);
    
    if(!cbdb_index_type_valid(idx.type)) {
      cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Data file invalid: index %s type is invalid", idx.name);
      return 0;
    }
    if(strcmp(cur, "primary") == 0) {
      index_cf[0] = idx;
    }
    else {
      index_cf[n] = idx;
      n++;
    }
  }
  return cur - (char *)db->data.start;
}

static int cbdb_data_file_exists(cbdb_t *db) {
  char path[1024];
  cbdb_filename(db, "data", path, 1024);
  return access(path, F_OK) != -1;
}

static cbdb_t *cbdb_open_abort(cbdb_t *db, cbdb_error_t *err) {
  static char   errstr[CBDB_ERROR_MAX_LEN];
  if(err) {
    *err = db->error;
    strncpy(errstr, err->str, CBDB_ERROR_MAX_LEN);
    err->str = errstr;
  }
  cbdb_close(db);
  return NULL;
}

cbdb_t *cbdb_open(char *path, char *name, cbdb_config_t *cf, cbdb_config_index_t *index_cf, cbdb_error_t *err) {
  cbdb_t       *db = cbdb_alloc_memset(path, name, cf, index_cf);
  int           new_db = 0, i;
  
  if(db == NULL) {
    cbdb_set_error(err, CBDB_ERROR_NOMEMORY, "Failed to allocate memory for cbdb struct");
    return NULL;
  }

  if(!cbdb_lock(db)) {
    return cbdb_open_abort(db, err);
  }
  
  new_db = !cbdb_data_file_exists(db);
  
  if(!cbdb_mmap_file(db, "data", &db->data)) {
    return cbdb_open_abort(db, err);
  }
  
  if(!new_db) {
    cbdb_config_t         loaded_cf;
    cbdb_config_index_t   loaded_index_cf[CBDB_INDICES_MAX];
    off_t                 header_len;
    if((header_len = cbdb_data_header_read(db, &loaded_cf, loaded_index_cf)) == 0) {
      return cbdb_open_abort(db, err);
    }
    if(!cf) {
      //try again with the loaded config
      cbdb_t *loaded_db = cbdb_open(path, name, &loaded_cf, loaded_index_cf, err);
      cbdb_close(db); //close the one we tried to open
      return loaded_db;
    }
    else {
      //see if the loaded config and the one passed in are the same
      if(loaded_cf.revision != db->config.revision) {
        cbdb_error(db, CBDB_ERROR_REVISION_MISMATCH, "Wrong revision number: expected %"PRIu32", loaded %"PRIu32, db->config.revision, loaded_cf.revision);
        return cbdb_open_abort(db, err);
      }
      if(loaded_cf.id_len != db->config.id_len) {
        cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong id length: expected %"PRIu16", loaded %"PRIu16, db->config.id_len, loaded_cf.id_len);
        return cbdb_open_abort(db, err);
      }
      if(loaded_cf.data_len != db->config.data_len) {
        cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong data length: expected %"PRIu16", loaded %"PRIu32, db->config.data_len, loaded_cf.data_len);
        return cbdb_open_abort(db, err);
      }
      if(loaded_cf.index_count != db->config.index_count) {
        cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong index count: expected %"PRIu16", loaded %"PRIu16, db->config.index_count, loaded_cf.index_count);
        return cbdb_open_abort(db, err);
      }
      
      //compare indices
      cbdb_config_index_t *expected_index_cf;
      for(i=0; i<loaded_cf.index_count; i++) {
        expected_index_cf = &db->index[i].config;
        if(strcmp(expected_index_cf->name, loaded_index_cf[i].name) != 0) {
          cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong index %i name: expected %s, loaded %s", i, expected_index_cf->name, loaded_index_cf[i].name);
          return cbdb_open_abort(db, err);
        }
        if(expected_index_cf->type != loaded_index_cf[i].type) {
          cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong index %i type: expected %s, loaded %s", i, cbdb_index_type_str(expected_index_cf->type), cbdb_index_type_str(loaded_index_cf[i].type));
          return cbdb_open_abort(db, err);
        }
        if(expected_index_cf->start != loaded_index_cf[i].start) {
          cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong index %i start: expected %"PRIu16", loaded %"PRIu16, i, expected_index_cf->start, loaded_index_cf[i].start);
          return cbdb_open_abort(db, err);
        }
        if(expected_index_cf->len != loaded_index_cf[i].len) {
          cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong index %i start: expected %"PRIu16", loaded %"PRIu16, i, expected_index_cf->start, loaded_index_cf[i].start);
          return cbdb_open_abort(db, err);
        }
      }
      //ok, everything matches
    }
    
  }
  
  for(i=0; i<db->config.index_count; i++) {
    if(!cbdb_mmap_index_file(db, i)) {
      return cbdb_open_abort(db, err);
    }
  }
  
  if(new_db) {
    cbdb_data_header_write(db);
  }
  
  return db;
}

void cbdb_close(cbdb_t *db) {
  cbdb_unlock(db);
  
  //unmmap stuff maybe?...
  
  cbdb_free(db);
}
