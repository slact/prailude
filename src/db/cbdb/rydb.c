#include "rydb.h"
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


#define RYDB_PAGESIZE (sysconf(_SC_PAGE_SIZE))

static int is_little_endian(void) {
  volatile union {
    uint8_t  c[4];
    uint32_t i;
  } u;
  u.i = 0x01020304;
  return u.c[0] == 0x04;
}

void rydb_error_print(rydb_error_t *err) {
  if(err->errno_val != 0) {
    printf("ERROR [%d]: %s, errno [%d]: %s\n", err->code, err->str, err->errno_val, strerror(err->errno_val));
  }
  else {
    printf("ERROR [%d]: %s\n", err->code, err->str);
  }
}

static off_t rydb_filename(rydb_t *db, char *what, char *buf, off_t maxlen) {
  return snprintf(buf, maxlen, "%s%c%s.%s.rydb", db->path, PATH_SLASH, db->name, what);
}

static rydb_t *rydb_alloc_memset(char *path, char *name, rydb_config_t *cf, rydb_config_index_t *index_cf) {
  rydb_t    *db;
  size_t     sz;
  int        i, n=0;
  
  //initialize indices
  rydb_config_index_t primary = {
    .name = "primary", .type = RYDB_INDEX_HASHTABLE, .start = 0, .len = cf->id_len
  };
  
  sz = sizeof(*db) + strlen(path) + 1 + strlen(name) + 1;
  
  sz += sizeof(rydb_index_t); // no need to allocate for primary index name -- see comments below.
  if(cf) {
    for(i=0; i<cf->index_count; i++) {
      sz += sizeof(rydb_index_t) + strlen(index_cf[i].name)+1;
    }
  }
  
  if((db = malloc(sz)) == NULL) {
    return NULL;
  }
  
  memset(db, '\0', sz);
  
  db->index = (rydb_index_t *)(&db[1]);
  char *cur = (char *)(&db->index[1]);
  if(cf) {
    db->config = *cf;
    
    //primary index, if there's a nonzero id
    if(db->config.id_len > 0) {
      db->index[n++].config = primary;
      //no need to copy name, it's a static string
      // althought it might be worth it to keep the names adjacent in memory later
      db->config.index_count++; //just the primary index then
    }
    
    
    //and now the rest of the indices
    for(i=0; i<cf->index_count; i++) {
      db->index[n].config = index_cf[i];
      db->index[n].config.name = cur;
      strcpy(db->index[n].config.name, index_cf[i].name);
      cur += strlen(index_cf[i].name) + 1;
      n++;
    }
  }
  db->path = cur;
  cur += strlen(path)+1;
  db->name = cur;
  cur += strlen(name)+1;
  
  strcpy(db->path, path);
  strcpy(db->name, name);
  
  sz = strlen(db->path);
  if(sz > 0 && db->path[sz-1]==PATH_SLASH) { // remove trailing slash
    db->path[sz-1] = '\00';
  }
  
  return db;
}

static void rydb_free(rydb_t *db) {
  free(db);
}

static void rydb_set_error(rydb_error_t *err, rydb_error_code_t code, char *error_string) {
  if(err) {
    err->code = code;
    strncpy(err->str, error_string, RYDB_ERROR_MAX_LEN-1);
    err->errno_val = errno;
  }
}

static void rydb_set_errorf(rydb_error_t *err, rydb_error_code_t code, char *err_fmt, ...) {
  if(err) {
    err->code = code;
    va_list ap;
    va_start(ap, err_fmt);
    vsnprintf(err->str, RYDB_ERROR_MAX_LEN-1, err_fmt, ap);
    err->errno_val = errno;
    va_end(ap);
  }
}

static void rydb_error(rydb_t *db, rydb_error_code_t code, char *err_fmt, ...) {
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
    strncpy(db->buffer.error, err_fmt, RYDB_ERROR_MAX_LEN);
    rydb_set_error(&db->error, code, db->buffer.error);
    return
  }*/
  va_list ap;
  va_start(ap, err_fmt);
  vsnprintf(db->error.str, RYDB_ERROR_MAX_LEN-1, err_fmt, ap);
  db->error.code = code;
  db->error.errno_val = errno;
  va_end(ap);
}

static int rydb_lock(rydb_t *db) {
  char buf[1024];
  rydb_filename(db, "lock", buf, 1024);
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP;
  int fd = open(buf, O_CREAT | O_EXCL, mode);
  if(fd == -1) {
    if(errno == EEXIST) {
      rydb_error(db, RYDB_ERROR_LOCK_FAILED, "Database is already locked");
    }
    else {
      rydb_error(db, RYDB_ERROR_LOCK_FAILED, "Can't lock database");
    }
    return 0;
  }
  else {
    //lock file created, i don't think we need to keep its fd open
    close(fd);
    return 1;
  }
}

static int rydb_unlock(rydb_t *db) {
    char buf[1024];
  rydb_filename(db, "lock", buf, 1024);
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

static int rydb_file_ensure_size(rydb_t *db, rydb_file_t *f, size_t desired_min_sz) {
  size_t current_sz = f->file.end - f->file.start;
  if(current_sz < desired_min_sz) {
    if(lseek(f->fd, desired_min_sz - current_sz, SEEK_END) == -1) {
      rydb_error(db, RYDB_ERROR_FILE_SIZE, "Failed to seek to end of file");
      return 0;
    }
    if(write(f->fd, "\00", 1) == -1) {
      rydb_error(db, RYDB_ERROR_FILE_SIZE, "Failed to grow file");
      return 0;
    }
  }
  return 1;
}

static int rydb_file_getsize(rydb_t *db, int fd, off_t *sz) {
  struct stat st;
  if(fstat(fd, &st)) {
    rydb_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to get filesize");
    return 0;
  }
  *sz = st.st_size;
  return 1;
}

static int rydb_file_close(rydb_t *db, rydb_file_t *f) {
  int ok = 1;
  if(f->mmap.start && f->mmap.start != MAP_FAILED) {
    if(munmap(f->mmap.start, f->mmap.end - f->mmap.start) == -1) {
      ok = 0;
      rydb_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to munmap file %s", f->path);
    }
  }
  if(f->fp) {
    if(fclose(f->fp) == EOF && ok) {
       //failed to close file
      ok = 0;
      rydb_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to close file pointer for %s", f->path);
    }
    f->fp = NULL;
    //since fp was fdopen()'d, the fd is now also closed
    f->fd = -1;
  }
  if(f->fd != -1) {
    if(close(f->fd) == -1 && ok) {
      ok = 0;
      rydb_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to close file descriptor for %s", f->path);
    }
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
  return ok;
}

static int rydb_file_open(rydb_t *db, char *what, rydb_file_t *f) {
  off_t sz;
  char path[2048];
  rydb_filename(db, what, path, 2048);
  
  if((f->path = malloc(strlen(path)+1)) == NULL) { //useful for debugging
    rydb_error(db, RYDB_ERROR_NOMEMORY, "Failed to allocate memory for file path %s", path);
    return 0;
  }
  strcpy(f->path, path);
  
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP;
  if((f->fd = open(path, O_RDWR | O_CREAT, mode)) == -1) {
    rydb_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to open file %s", path);
    rydb_file_close(db, f);
    return 0;
  }
  
  if((f->fp = fdopen(f->fd, "r+")) == NULL) {
    rydb_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to fdopen file %s", path);
    rydb_file_close(db, f);
  }
  
  sz = RYDB_PAGESIZE*10;
  f->mmap.start = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, f->fd, 0);
  if(f->mmap.start == MAP_FAILED) {
    rydb_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to mmap file %s", path);
    rydb_file_close(db, f);
    return 0;
  }
  f->mmap.end = &f->mmap.start[sz]; //last mmapped address
  
  f->file.start = f->mmap.start;
  if(!rydb_file_getsize(db, f->fd, &sz)) {
    rydb_file_close(db, f);
    return 0;
  }
  f->file.end = &f->file.start[sz];
  
  f->data = f->file;
  
  return 1;
}

static int rydb_file_open_index(rydb_t *db, int index_n) {
  char index_name[128];
  snprintf(index_name, 128, "index.%s", db->index[index_n].config.name);
  return rydb_file_open(db, index_name, &db->index[index_n].data);
}

static int rydb_index_type_valid(rydb_index_type_t index_type) {
  switch(index_type) {
    case RYDB_INDEX_HASHTABLE:
    case RYDB_INDEX_BTREE:
      return 1;
    case RYDB_INDEX_INVALID:
      return 0;
  }
  return 0;
}

static const char *rydb_index_type_str(rydb_index_type_t index_type) {
  switch(index_type) {
    case RYDB_INDEX_HASHTABLE:
      return "hashtable";
    case RYDB_INDEX_BTREE:
      return "B-tree";
    case RYDB_INDEX_INVALID:
      return "invalid";
  }
  return "???";
}

static off_t rydb_data_header_write(rydb_t *db) {
  FILE     *fp = db->data.fp;
  int       rc, i;
  int       total_written = 0;
  rydb_config_index_t *idxcf;
  
  if(fseek(fp, 0, SEEK_SET) == -1) {
    rydb_error(db, RYDB_ERROR_FILE_INVALID, "Failed seeking to start of data file %s", db->data.path);
    return 0;
  }
  
  const char *fmt = 
    "rydb\n"
    "format revision: %i\n"
    "database revision: %"PRIu32"\n"
    "endianness: %s\n"
    "row_len: %"PRIu16"\n"
    "id_len: %"PRIu16"\n"
    "indexes: %"PRIu16"\n";
  rc = fprintf(fp, fmt, RYDB_FORMAT_VERSION, db->config.revision, is_little_endian() ? "little" : "big", db->config.row_len, db->config.id_len, db->config.index_count);
  if(rc <= 0) {
    rydb_error(db, RYDB_ERROR_FILE_ACCESS, "Failed writing header to data file %s", db->data.path);
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
    rc = fprintf(fp, index_fmt, idxcf->name, rydb_index_type_str(idxcf->type), idxcf->start, idxcf->len);
    if(rc <= 0){
      rydb_error(db, RYDB_ERROR_FILE_ACCESS, "Failed writing header to data file %s", db->data.path);
      return 0;
    }
    total_written += rc;
  }
  return total_written;
}

static rydb_index_type_t rydb_index_type(char *str) {
  if(strcmp(str, "hashtable") == 0) {
    return RYDB_INDEX_HASHTABLE;
  }
  else if(strcmp(str, "B-tree") == 0) {
    return RYDB_INDEX_BTREE;
  }
  else {
    return RYDB_INDEX_INVALID;
  }
}

#define QUOTE(str) #str
#define EXPAND_AND_QUOTE(str) QUOTE(str)
#define RYDB_INDEX_NAME_MAX_LEN_STR EXPAND_AND_QUOTE(RYDB_INDEX_NAME_MAX_LEN)

static off_t rydb_data_header_read(rydb_t *db, rydb_config_t *cf, rydb_config_index_t *index_cf, char *buf, off_t buflen) {
  char     *cur = buf;
  uint16_t  rydb_version;
  int       i, n;
  FILE     *fp = db->data.fp;
  char     *buf_end = &buf[buflen];
  char      index_type_buf[32];
  char      endianness_buf[16];
  int       little_endian;
  rydb_config_index_t idx;
  if(fseek(fp, 0, SEEK_SET) == -1) {
    rydb_error(db, RYDB_ERROR_FILE_INVALID, "Failed seeking to start of data file");
    return 0;
  }
  
  const char *fmt = 
    "rydb\n"
    "format revision: %hu\n"
    "database revision: %u\n"
    "endianness: %15s\n"
    "row_len: %hu\n"
    "id_len: %hu\n"
    "indexes: %hu\n";
  int rc = fscanf(fp, fmt, &rydb_version, &cf->revision, endianness_buf, &cf->row_len, &cf->id_len, &cf->index_count);
  if(rc < 6){
    rydb_error(db, RYDB_ERROR_FILE_INVALID, "Data file is not a rydb file or is corrupted");
    return 0;
  }
  
  if(rydb_version != RYDB_FORMAT_VERSION) {
    rydb_error(db, RYDB_ERROR_FILE_INVALID, "Data format version mismatch, expected %i, loaded %"PRIu16, RYDB_FORMAT_VERSION, rydb_version);
    return 0;
  }
  
  if(cf->index_count > RYDB_INDICES_MAX) {
    rydb_error(db, RYDB_ERROR_FILE_INVALID, "Data file invalid: too many indices defined");
    return 0;
  }
  
  if(strcmp(endianness_buf, "big") == 0) {
    little_endian = 0;
  }
  else if(strcmp(endianness_buf, "little") == 0) {
    little_endian = 1;
  }
  else {
    rydb_error(db, RYDB_ERROR_FILE_INVALID, "Data file invalid: unexpected endianness %s", endianness_buf);
    return 0;
  }
  
  if(is_little_endian() != little_endian) {
    //TODO: convert data to host endianness
    rydb_error(db, RYDB_ERROR_WRONG_ENDIANNESS, "Data file has wrong endianness");
    return 0;
  }
  
  const char *index_fmt = "  - name: %" RYDB_INDEX_NAME_MAX_LEN_STR "s\n"
    "    type: %32s\n"
    "    start: %hu\n"
    "    len: %hu\n";
    
  for(i=0, n=1; i<cf->index_count; i++) {
    if(buf_end - cur < RYDB_INDEX_NAME_MAX_LEN) {
      rydb_error(db, RYDB_ERROR_FILE_INVALID, "Data file invalid: not enough space to read index names");
      return 0;
    }
    rc = fscanf(fp, index_fmt, cur, index_type_buf, &idx.start, &idx.len);
    idx.name = cur;
    cur += strlen(idx.name)+1;
    if(rc < 4){
      rydb_error(db, RYDB_ERROR_FILE_INVALID, "Data file invalid: index specification is invalid");
      return 0;
    }
    if((idx.type = rydb_index_type(index_type_buf)) == RYDB_INDEX_INVALID) {
      rydb_error(db, RYDB_ERROR_FILE_INVALID, "Data file invalid: index \"%s\" type is invalid", idx.name);
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
    rydb_error(db, RYDB_ERROR_FILE_INVALID, "Data file invalid: unable to ftell() header size");
    return 0;
  }
  return pos;
}

static int rydb_config_match(rydb_t *db, rydb_config_t *loaded_cf, rydb_config_index_t *loaded_index_cf) {
  int i;
  //see if the loaded config and the one passed in are the same
  if(loaded_cf->revision != db->config.revision) {
    rydb_error(db, RYDB_ERROR_REVISION_MISMATCH, "Wrong revision number: expected %"PRIu32", loaded %"PRIu32, db->config.revision, loaded_cf->revision);
    return 0;
  }
  if(loaded_cf->row_len != db->config.row_len) {
    rydb_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong row length: expected %"PRIu16", loaded %"PRIu32, db->config.row_len, loaded_cf->row_len);
    return 0;
  }
  if(loaded_cf->id_len != db->config.id_len) {
    rydb_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong id length: expected %"PRIu16", loaded %"PRIu16, db->config.id_len, loaded_cf->id_len);
    return 0;
  }
  if(loaded_cf->index_count != db->config.index_count) {
    rydb_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong index count: expected %"PRIu16", loaded %"PRIu16, db->config.index_count, loaded_cf->index_count);
    return 0;
  }
      
  //compare indices
  rydb_config_index_t *expected_index_cf;
  for(i=0; i<loaded_cf->index_count; i++) {
    expected_index_cf = &db->index[i].config;
    if(strcmp(expected_index_cf->name, loaded_index_cf[i].name) != 0) {
      rydb_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong index %i name: expected %s, loaded %s", i, expected_index_cf->name, loaded_index_cf[i].name);
      return 0;
    }
    if(expected_index_cf->type != loaded_index_cf[i].type) {
      rydb_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong index %i type: expected %s, loaded %s", i, rydb_index_type_str(expected_index_cf->type), rydb_index_type_str(loaded_index_cf[i].type));
      return 0;
    }
    if(expected_index_cf->start != loaded_index_cf[i].start) {
      rydb_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong index %i start: expected %"PRIu16", loaded %"PRIu16, i, expected_index_cf->start, loaded_index_cf[i].start);
      return 0;
    }
    if(expected_index_cf->len != loaded_index_cf[i].len) {
      rydb_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong index %i length: expected %"PRIu16", loaded %"PRIu16, i, expected_index_cf->len, loaded_index_cf[i].len);
      return 0;
    }
  }
  return 1;
}

static int rydb_data_file_exists(rydb_t *db) {
  char path[1024];
  rydb_filename(db, "data", path, 1024);
  return access(path, F_OK) != -1;
}

static rydb_t *rydb_open_abort(rydb_t *db, rydb_error_t *err) {
  if(err) {
    *err = db->error;
  }
  rydb_close(db);
  return NULL;
}

rydb_t *rydb_open(char *path, char *name, rydb_config_t *cf, rydb_config_index_t *index_cf, rydb_error_t *err) {
  rydb_t       *db;
  int           new_db = 0, i;
  
  if(index_cf && !cf) {
    rydb_set_error(err, RYDB_ERROR_BAD_CONFIG, "Cannot call rydb_open with index_cf but without cf");
    return NULL;
  }
  if(index_cf) {
    for(i=0; i < cf->index_count; i++) {
      if(strlen(index_cf[i].name) > RYDB_INDEX_NAME_MAX_LEN) {
        rydb_set_errorf(err, RYDB_ERROR_BAD_CONFIG, "Index name \"%s\" too long, must be at most %i characters", index_cf[i].name, RYDB_INDEX_NAME_MAX_LEN);
      }
      if(strspn(index_cf[i].name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != strlen(index_cf[i].name)) {
        rydb_set_errorf(err, RYDB_ERROR_BAD_CONFIG, "Index name \"%s\" invalid: must consist of only ASCII alphanumeric characters and underscores", index_cf[i].name);
        return NULL;
      }
      if(!rydb_index_type_valid(index_cf[i].type)) {
        rydb_set_errorf(err, RYDB_ERROR_BAD_CONFIG, "Index \"%s\" type for is invalid", index_cf[i].name);
        return NULL;
      }
      if(index_cf[i].start > cf->row_len) {
        rydb_set_errorf(err, RYDB_ERROR_BAD_CONFIG, "Index \"%s\" is out of bounds: row length is %"PRIu16", but index is set to start at %"PRIu16, index_cf[i].name, cf->row_len, index_cf[i].start);
        return NULL;
      }
      if(index_cf[i].start + cf->row_len > cf->row_len) {
        rydb_set_errorf(err, RYDB_ERROR_BAD_CONFIG, "Index \"%s\" is out of bounds: row length is %"PRIu16", but index is set to end at %"PRIu16, index_cf[i].name, cf->row_len, index_cf[i].start + cf->row_len);
        return NULL;
      }
    }
  }
  
  db = rydb_alloc_memset(path, name, cf, index_cf);
  
  if(db == NULL) {
    rydb_set_error(err, RYDB_ERROR_NOMEMORY, "Failed to allocate memory for rydb struct");
    return NULL;
  }
  
  if(!rydb_lock(db)) {
    return rydb_open_abort(db, err);
  }
  
  new_db = !rydb_data_file_exists(db);
  
  if(!rydb_file_open(db, "data", &db->data)) {
    return rydb_open_abort(db, err);
  }
  
  if(!new_db) {
    rydb_config_t         loaded_cf;
    rydb_config_index_t   loaded_index_cf[RYDB_INDICES_MAX];
    off_t                 header_len;
    char                  buf[(RYDB_INDEX_NAME_MAX_LEN + 1) * RYDB_INDICES_MAX];
    if((header_len = rydb_data_header_read(db, &loaded_cf, loaded_index_cf, buf, (RYDB_INDEX_NAME_MAX_LEN + 1) * RYDB_INDICES_MAX)) == 0) {
      return rydb_open_abort(db, err);
    }
    if(!cf) {
      //try again with the loaded config
      rydb_t *loaded_db = rydb_open(path, name, &loaded_cf, loaded_index_cf, err);
      rydb_close(db); //close the one we tried to open
      return loaded_db;
    }
    else {
      //compare configs
      if(rydb_config_match(db, &loaded_cf, loaded_index_cf) == 0) {
        return rydb_open_abort(db, err);
      }
      //ok, everything matches
    }
  }
  for(i=0; i<db->config.index_count; i++) {
    if(!rydb_file_open_index(db, i)) {
      return rydb_open_abort(db, err);
    }
  }
  
  if(new_db) {
    rydb_data_header_write(db);
  }
  
  return db;
}

void rydb_close(rydb_t *db) {
  int i;
  
  rydb_file_close(db, &db->data);
  rydb_file_close(db, &db->meta);
  for(i=0; i<db->config.index_count; i++) {
    rydb_file_close(db, &db->index[i].data);
  }
  rydb_unlock(db);
  rydb_free(db);
}