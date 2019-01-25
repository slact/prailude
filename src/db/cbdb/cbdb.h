#include <inttypes.h>
#include <limits.h>
#include <stddef.h>

typedef uint32_t cbdb_rownum_t;
#define CBDB_ROWNUM_MAX  ((cbdb_rownum_t ) -1)
#define CBDB_ROWNUM_NULL ((cbdb_rownum_t ) 0)

#define CBDB_ROW_LINKS_MAX 4
#define CBDB_INDICES_MAX 16

typedef struct {
  char           *str;
  uint16_t        len;
} cbdb_str_t;

typedef struct {
  cbdb_rownum_t   n;
  cbdb_str_t      id;
  cbdb_str_t      data;
  cbdb_rownum_t   link[CBDB_ROW_LINKS_MAX];
} cbdb_row_t;

typedef struct {
  char    *path;
  void    *ptr; //pointer to first byte in the file
  void    *start; //location of first data byte (may not be first byte of file due to headers)
  void    *end; //location of last data byte
} cbdb_mmap_t;

typedef enum {
  CBDB_INDEX_HASHTABLE=1,
  CBDB_INDEX_BTREE=2
} cbdb_index_type_t;

typedef struct {
  char              *name;
  cbdb_index_type_t  type;
  cbdb_mmap_t        mmap;
} cbdb_index_t;

typedef enum {
  CBDB_NO_ERROR             = 0,
  CBDB_ERROR_UNSPECIFIED    = 1,
  CBDB_ERROR_NOMEMORY       = 2,
} cbdb_error_code_t;

typedef struct {
  cbdb_error_code_t    code;
  char                *str;
} cbdb_error_t;

typedef struct {
  uint16_t id_len;
  uint16_t data_len;
  
} cbdb_config_t;

typedef struct {
  char         *path;
  cbdb_mmap_t   data;
  cbdb_config_t config;
  struct {
    int           count;
    cbdb_index_t  array[CBDB_INDICES_MAX];
  }             index;
  struct {
    char         *id;
    char         *data;
  }             buffer;
  cbdb_error_t  error;
} cbdb_t;



cbdb_t *cbdb_open(char *path, cbdb_error_t *err);
cbdb_t *cbdb_create(char*path, cbdb_config_t *cf, cbdb_error_t *err);
int cbdb_close(cbdb_t *cbdb);

int cbdb_insert(cbdb_t *cbdb, cbdb_str_t *id, cbdb_str_t *data);
int cbdb_insert_row(cbdb_t *cbdb, cbdb_row_t *row); //id and data should be pre-filled
int cbdb_find(cbdb_t *cbdb, cbdb_str_t *id); //return 1 if found, 0 if not found
int cbdb_find_row(cbdb_t *cbdb, cbdb_row_t *row); //id should be pre-filled
