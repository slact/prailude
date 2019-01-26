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
  int      fd;
  void    *start; //first valid mmapped address, also the first byte of the file
  void    *fist; //lfirst data byte (may not be first byte of file due to headers)
  void    *last; //last data byte
  void    *end; //last valid mmapped address
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


#define CBDB_ERROR_MAX_LEN 1024
typedef enum {
  CBDB_NO_ERROR             = 0,
  CBDB_ERROR_UNSPECIFIED    = 1,
  CBDB_ERROR_NOMEMORY       = 2,
  CBDB_ERROR_FILE_NOT_FOUND = 3,
  CBDB_ERROR_FILE_EXISTS    = 4,
  CBDB_ERROR_LOCK_FAILED    = 5,
  CBDB_ERROR_FILE_ACCESS    = 6,
} cbdb_error_code_t;

typedef struct {
  cbdb_error_code_t    code;
  char                *str;
  int                  errno_val;
} cbdb_error_t;

typedef struct {
  uint16_t id_len;
  uint16_t data_len;
  
} cbdb_config_t;

typedef struct {
  char         *path;
  char         *name;
  cbdb_mmap_t   data;
  cbdb_config_t config;
  struct {
    int           count;
    cbdb_index_t  array[CBDB_INDICES_MAX];
  }             index;
  struct {
    char         *id;
    char         *data;
    char         *error;
  }             buffer;
  cbdb_error_t  error;
} cbdb_t;



//cbdb_t *cbdb_open(char *path, cbdb_error_t *err);
cbdb_t *cbdb_open(char *path, char *name, cbdb_config_t *cf, cbdb_error_t *err);
void cbdb_close(cbdb_t *cbdb);

int cbdb_insert(cbdb_t *cbdb, cbdb_str_t *id, cbdb_str_t *data);
int cbdb_insert_row(cbdb_t *cbdb, cbdb_row_t *row); //id and data should be pre-filled
int cbdb_find(cbdb_t *cbdb, cbdb_str_t *id); //return 1 if found, 0 if not found
int cbdb_find_row(cbdb_t *cbdb, cbdb_row_t *row); //id should be pre-filled
