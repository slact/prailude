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

#include <signal.h>

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
    db->config = *cf;
    
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

static void cbdb_set_errorf(cbdb_error_t *err, cbdb_error_code_t code, char *err_fmt, ...) {
  static char buf[CBDB_ERROR_MAX_LEN];
  va_list ap;
  va_start(ap, err_fmt);
  vsnprintf(buf, CBDB_ERROR_MAX_LEN, err_fmt, ap);
  cbdb_set_error(err, code, buf);
  va_end(ap);
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

static int cbdb_file_ensure_size(cbdb_t *db, cbdb_file_t *f, size_t desired_min_sz) {
  size_t current_sz = f->file.end - f->file.start;
  if(current_sz < desired_min_sz) {
    if(lseek(f->fd, desired_min_sz - current_sz, SEEK_END) == -1) {
      cbdb_error(db, CBDB_ERROR_FILE_SIZE, "Failed to seek to end of file");
      return 0;
    }
    if(write(f->fd, "\00", 1) == -1) {
      cbdb_error(db, CBDB_ERROR_FILE_SIZE, "Failed to grow file");
      return 0;
    }
  }
  return 1;
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

static int cbdb_file_close(cbdb_t *db, cbdb_file_t *f) {
  if(f->mmap.start && f->mmap.start != MAP_FAILED) {
    munmap(f->mmap.start, f->mmap.end - f->mmap.start);
  }
  if(f->fp) {
    fclose(f->fp);
    f->fp = NULL;
    //since fp was fdopen()'d, the fd is now also closed
    f->fd = -1;
  }
  if(f->fd != -1) {
    close(f->fd);
    f->fd = -1;
  }
  f->mmap.start = NULL;
  f->mmap.end = NULL;
  f->file.start = NULL;
  f->file.end = NULL;
  f->data.start = NULL;
  f->data.end = NULL;
  
  if(f->path) {
    free(f->path);
    f->path = NULL;
  }
  return 1;
}

static int cbdb_file_open(cbdb_t *db, char *what, cbdb_file_t *f) {
  off_t sz;
  char path[2048];
  cbdb_filename(db, what, path, 2048);
  
  if((f->path = malloc(strlen(path)+1)) == NULL) { //useful for debugging
    cbdb_error(db, CBDB_ERROR_NOMEMORY, "Failed to allocate memory for file path %s", path);
    return 0;
  }
  strcpy(f->path, path);
  
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP;
  if((f->fd = open(path, O_RDWR | O_CREAT, mode)) == -1) {
    cbdb_error(db, CBDB_ERROR_FILE_ACCESS, "Failed to open file %s", path);
    cbdb_file_close(db, f);
    return 0;
  }
  
  if((f->fp = fdopen(f->fd, "r+")) == NULL) {
    cbdb_error(db, CBDB_ERROR_FILE_ACCESS, "Failed to fdopen file %s", path);
    cbdb_file_close(db, f);
  }
  
  sz = CBDB_PAGESIZE*10;
  f->mmap.start = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, f->fd, 0);
  if(f->mmap.start == MAP_FAILED) {
    cbdb_error(db, CBDB_ERROR_FILE_ACCESS, "Failed to mmap file %s", path);
    cbdb_file_close(db, f);
    return 0;
  }
  f->mmap.end = &f->mmap.start[sz]; //last mmapped address
  
  f->file.start = f->mmap.start;
  if(!cbdb_file_getsize(db, f->fd, &sz)) {
    cbdb_file_close(db, f);
    return 0;
  }
  f->file.end = &f->file.start[sz];
  
  f->data = f->file;
  
  return 1;
}

static int cbdb_file_open_index(cbdb_t *db, int index_n) {
  char index_name[128];
  snprintf(index_name, 128, "index.%s", db->index[index_n].config.name);
  return cbdb_file_open(db, index_name, &db->index[index_n].data);
}

static int cbdb_index_type_valid(cbdb_index_type_t index_type) {
  switch(index_type) {
    case CBDB_INDEX_HASHTABLE:
    case CBDB_INDEX_BTREE:
      return 1;
    case CBDB_INDEX_INVALID:
      return 0;
  }
  return 0;
}

static const char *cbdb_index_type_str(cbdb_index_type_t index_type) {
  switch(index_type) {
    case CBDB_INDEX_HASHTABLE:
      return "hashtable";
    case CBDB_INDEX_BTREE:
      return "B-tree";
    case CBDB_INDEX_INVALID:
      return "invalid";
  }
  return "???";
}

static off_t cbdb_data_header_write(cbdb_t *db) {
  FILE     *fp = db->data.fp;
  int       rc, i;
  int       total_written = 0;
  cbdb_config_index_t *idxcf;
  
  if(fseek(fp, 0, SEEK_SET) == -1) {
    cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Failed seeking to start of data file %s", db->data.path);
    return 0;
  }
  
  const char *fmt = 
    "cbdb\n"
    "format revision: %i\n"
    "database revision: %"PRIu32"\n"
    "id_len: %"PRIu16"\n"
    "data_len: %"PRIu16"\n"
    "index_count: %"PRIu16"\n"
    "indices:\n";
  rc = fprintf(fp, fmt, CBDB_FORMAT_VERSION, db->config.revision, db->config.id_len, db->config.data_len, db->config.index_count);
  if(rc <= 0) {
    cbdb_error(db, CBDB_ERROR_FILE_ACCESS, "Failed writing header to data file %s", db->data.path);
    return 0;
  }
  total_written += rc;
  
  const char *index_fmt = 
    "  - name: %s\n"
    "    type: %s\n"
    "    start: %"PRIu16"\n"
    "    len: %"PRIu16"\n";
    
  for(i=0; i<db->config.index_count; i++) {
    idxcf = &db->index[i].config;
    rc = fprintf(fp, index_fmt, idxcf->name, cbdb_index_type_str(idxcf->type), idxcf->start, idxcf->len);
    if(rc <= 0){
      cbdb_error(db, CBDB_ERROR_FILE_ACCESS, "Failed writing header to data file %s", db->data.path);
      return 0;
    }
    total_written += rc;
  }
  return total_written;
}

static cbdb_index_type_t cbdb_index_type(char *str) {
  if(strcmp(str, "hashtable") == 0) {
    return CBDB_INDEX_HASHTABLE;
  }
  else if(strcmp(str, "B-tree") == 0) {
    return CBDB_INDEX_BTREE;
  }
  else {
    return CBDB_INDEX_INVALID;
  }
}

static int cbdb_read(cbdb_t *db, int fd, char *buf, size_t count, cbdb_error_code_t errcode) {
  ssize_t bytes_read = read(fd, buf, count);
  if(bytes_read == -1) {
    cbdb_error(db, errcode, "Failed reading data from file");
    return 0;
  }
  else if(bytes_read != count) {
    cbdb_error(db, errcode, "Attempted to read past end of file");
    return 0;
  }
  else {
    return 1;
  }
}

#define QUOTE(str) #str
#define EXPAND_AND_QUOTE(str) QUOTE(str)
#define CBDB_INDEX_NAME_MAX_LEN_STR EXPAND_AND_QUOTE(CBDB_INDEX_NAME_MAX_LEN)

static off_t cbdb_data_header_read(cbdb_t *db, cbdb_config_t *cf, cbdb_config_index_t *index_cf, char *buf, off_t buflen) {
  char     *cur = buf;
  uint16_t  cbdb_version;
  int       i, n;
  FILE     *fp = db->data.fp;
  char     *buf_end = &buf[buflen];
  char      index_type_buf[32];
  cbdb_config_index_t idx;
  if(fseek(fp, 0, SEEK_SET) == -1) {
    cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Failed seeking to start of data file");
    return 0;
  }
  
  const char *fmt = 
    "cbdb\n"
    "format revision: %hu\n"
    "database revision: %u\n"
    "id_len: %hu\n"
    "data_len: %hu\n"
    "index_count: %hu\n"
    "indices:\n";
  int rc = fscanf(fp, fmt, &cbdb_version, &cf->revision, &cf->id_len, &cf->data_len, &cf->index_count);
  if(rc < 5){
    cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Data file is not a cbdb file or is corrupted");
    return 0;
  }
  if(cbdb_version != CBDB_FORMAT_VERSION) {
    cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Data format version mismatch, expected %i, loaded %"PRIu16, CBDB_FORMAT_VERSION, cbdb_version);
    return 0;
  }
  if(cf->index_count > CBDB_INDICES_MAX) {
    cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Data file invalid: too many indices defined");
    return 0;
  }
  
  const char *index_fmt = "  - name: %" CBDB_INDEX_NAME_MAX_LEN_STR "s\n"
    "    type: %32s\n"
    "    start: %hu\n"
    "    len: %hu\n";
    
  for(i=0, n=1; i<cf->index_count; i++) {
    if(buf_end - cur < CBDB_INDEX_NAME_MAX_LEN) {
      cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Data file invalid: not enough space to read index names");
      return 0;
    }
    rc = fscanf(fp, index_fmt, cur, index_type_buf, &idx.start, &idx.len);
    idx.name = cur;
    cur += strlen(idx.name)+1;
    if(rc < 4){
      cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Data file invalid: index specification is invalid");
      return 0;
    }
    if((idx.type = cbdb_index_type(index_type_buf)) == CBDB_INDEX_INVALID) {
      cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Data file invalid: index \"%s\" type is invalid", idx.name);
      return 0;
    }
    if(strcmp(idx.name, "primary") == 0) {
      index_cf[0] = idx;
    }
    else {
      index_cf[n] = idx;
      n++;
    }
  }
  
  off_t pos = ftell(fp);
  if(pos == -1) {
    cbdb_error(db, CBDB_ERROR_FILE_INVALID, "Data file invalid: unable to ftell() header size");
    return 0;
  }
  return pos;
}

static int cbdb_config_match(cbdb_t *db, cbdb_config_t *loaded_cf, cbdb_config_index_t *loaded_index_cf) {
  int i;
  //see if the loaded config and the one passed in are the same
  if(loaded_cf->revision != db->config.revision) {
    cbdb_error(db, CBDB_ERROR_REVISION_MISMATCH, "Wrong revision number: expected %"PRIu32", loaded %"PRIu32, db->config.revision, loaded_cf->revision);
    return 0;
  }
  if(loaded_cf->id_len != db->config.id_len) {
    cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong id length: expected %"PRIu16", loaded %"PRIu16, db->config.id_len, loaded_cf->id_len);
    return 0;
  }
  if(loaded_cf->data_len != db->config.data_len) {
    cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong data length: expected %"PRIu16", loaded %"PRIu32, db->config.data_len, loaded_cf->data_len);
    return 0;
  }
  if(loaded_cf->index_count != db->config.index_count) {
    cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong index count: expected %"PRIu16", loaded %"PRIu16, db->config.index_count, loaded_cf->index_count);
    return 0;
  }
      
  //compare indices
  cbdb_config_index_t *expected_index_cf;
  for(i=0; i<loaded_cf->index_count; i++) {
    expected_index_cf = &db->index[i].config;
    if(strcmp(expected_index_cf->name, loaded_index_cf[i].name) != 0) {
      cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong index %i name: expected %s, loaded %s", i, expected_index_cf->name, loaded_index_cf[i].name);
      return 0;
    }
    if(expected_index_cf->type != loaded_index_cf[i].type) {
      cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong index %i type: expected %s, loaded %s", i, cbdb_index_type_str(expected_index_cf->type), cbdb_index_type_str(loaded_index_cf[i].type));
      return 0;
    }
    if(expected_index_cf->start != loaded_index_cf[i].start) {
      cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong index %i start: expected %"PRIu16", loaded %"PRIu16, i, expected_index_cf->start, loaded_index_cf[i].start);
      return 0;
    }
    if(expected_index_cf->len != loaded_index_cf[i].len) {
      cbdb_error(db, CBDB_ERROR_CONFIG_MISMATCH, "Wrong index %i length: expected %"PRIu16", loaded %"PRIu16, i, expected_index_cf->len, loaded_index_cf[i].len);
      return 0;
    }
  }
  return 1;
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
  cbdb_t       *db;
  int           new_db = 0, i;
  
  if(index_cf && !cf) {
    cbdb_set_error(err, CBDB_ERROR_BAD_CONFIG, "Cannot call cbdb_open with index_cf but without cf");
    return NULL;
  }
  if(index_cf) {
    for(i=0; i < cf->index_count; i++) {
      if(strspn(index_cf[i].name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != strlen(index_cf[i].name)) {
        cbdb_set_errorf(err, CBDB_ERROR_BAD_CONFIG, "Index name \"%s\" invalid: must consist of only ASCII alphanumeric characters and underscores", index_cf[i].name);
        return NULL;
      }
      if(!cbdb_index_type_valid(index_cf[i].type)) {
        cbdb_set_errorf(err, CBDB_ERROR_BAD_CONFIG, "Index \"%s\" type for is invalid", index_cf[i].name);
        return NULL;
      }
      if(index_cf[i].start == CBDB_INDEX_ID && index_cf[i].len > cf->id_len) {
        cbdb_set_errorf(err, CBDB_ERROR_BAD_CONFIG, "Index \"%s\" of id is too long: expected max %"PRIu16", got %"PRIu16, index_cf[i].name, cf->id_len, index_cf[i].len);
        return NULL;
      }
      if(index_cf[i].start != CBDB_INDEX_ID && index_cf[i].start > cf->data_len) {
        cbdb_set_errorf(err, CBDB_ERROR_BAD_CONFIG, "Index \"%s\" of data is out of bounds: data length is %"PRIu16", but index is set to start at %"PRIu16, index_cf[i].name, cf->data_len, index_cf[i].start);
        return NULL;
      }
      if(index_cf[i].start + cf->data_len > cf->data_len) {
        cbdb_set_errorf(err, CBDB_ERROR_BAD_CONFIG, "Index \"%s\" of data is out of bounds: data length is %"PRIu16", but index is set to end at %"PRIu16, index_cf[i].name, cf->data_len, index_cf[i].start + cf->data_len);
        return NULL;
      }
    }
  }
  
  db = cbdb_alloc_memset(path, name, cf, index_cf);
  
  if(db == NULL) {
    cbdb_set_error(err, CBDB_ERROR_NOMEMORY, "Failed to allocate memory for cbdb struct");
    return NULL;
  }
  
  if(!cbdb_lock(db)) {
    return cbdb_open_abort(db, err);
  }
  
  new_db = !cbdb_data_file_exists(db);
  
  if(!cbdb_file_open(db, "data", &db->data)) {
    return cbdb_open_abort(db, err);
  }
  
  if(!new_db) {
    cbdb_config_t         loaded_cf;
    cbdb_config_index_t   loaded_index_cf[CBDB_INDICES_MAX];
    off_t                 header_len;
    char                  buf[(CBDB_INDEX_NAME_MAX_LEN + 1) * CBDB_INDICES_MAX];
    if((header_len = cbdb_data_header_read(db, &loaded_cf, loaded_index_cf, buf, (CBDB_INDEX_NAME_MAX_LEN + 1) * CBDB_INDICES_MAX)) == 0) {
      return cbdb_open_abort(db, err);
    }
    if(!cf) {
      //try again with the loaded config
      cbdb_t *loaded_db = cbdb_open(path, name, &loaded_cf, loaded_index_cf, err);
      cbdb_close(db); //close the one we tried to open
      return loaded_db;
    }
    else {
      //compare configs
      if(cbdb_config_match(db, &loaded_cf, loaded_index_cf) == 0) {
        return cbdb_open_abort(db, err);
      }
      //ok, everything matches
    }
  }
  for(i=0; i<db->config.index_count; i++) {
    if(!cbdb_file_open_index(db, i)) {
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
