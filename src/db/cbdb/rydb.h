#ifndef _RYDB_H
#define _RYDB_H
#include <inttypes.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>

#define RYDB_FORMAT_VERSION 1

typedef uint32_t rydb_rownum_t;
#define RYDB_ROWNUM_MAX  ((rydb_rownum_t ) -1)
#define RYDB_ROWNUM_NULL ((rydb_rownum_t ) 0)

#define RYDB_ROW_LINKS_MAX 8

#define RYDB_INDEX_NAME_MAX_LEN 64
#define RYDB_INDICES_MAX 32

typedef struct {
  char           *str;
  uint16_t        len;
} rydb_str_t;

typedef struct {
  rydb_rownum_t   n;
  
  rydb_str_t      id;
  rydb_str_t      data;
  rydb_rownum_t   link[RYDB_ROW_LINKS_MAX];
} rydb_row_t;

typedef struct {
  char *start;
  char *end;
} rydb_char_range_t;

typedef struct {
  int      fd;
  FILE    *fp;
  rydb_char_range_t mmap;
  rydb_char_range_t file;
  rydb_char_range_t data;
  char    *path;
} rydb_file_t;

typedef enum {
  RYDB_INDEX_INVALID=0,
  RYDB_INDEX_HASHTABLE=1,
  RYDB_INDEX_BTREE=2
} rydb_index_type_t;

typedef struct {
  enum {
    RYDB_HASH_CRC32 =     0,
    RYDB_HASH_NOHASH =    1, //treat the value as if it's already a good hash
    RYDB_HASH_SIPHASH =   2
  }            hash_function;
  
  //storing the value in the hashtable prevents extra datafile reads at the cost of possibly much larger hashtable entries
  unsigned     store_value:1;
  
  //direct mapping uses open-address linear probing, ideal for a 1-to-1 unique primary index. <2 reads avg.
  unsigned     direct_mapping:1;
} rydb_config_index_hashtable_t;


typedef struct {
  char              *name;
  rydb_index_type_t  type;
  uint16_t           start; // start of indexable value in row
  uint16_t           len; //length of indexable data
} rydb_config_index_t;

typedef struct {
  rydb_config_index_t  config;
  rydb_file_t          data;
} rydb_index_t;

typedef struct {
  char *name;
  char *reverse_name; //optional linked row reverse link name for doubly-linked rows
} rydb_row_link_t;

#define RYDB_ERROR_MAX_LEN 1024
typedef enum {
  RYDB_NO_ERROR             = 0,
  RYDB_ERROR_UNSPECIFIED    = 1,
  RYDB_ERROR_NOMEMORY       = 2,
  RYDB_ERROR_FILE_NOT_FOUND = 3,
  RYDB_ERROR_FILE_EXISTS    = 4,
  RYDB_ERROR_LOCK_FAILED    = 5,
  RYDB_ERROR_FILE_ACCESS    = 6,
  RYDB_ERROR_FILE_INVALID   = 7,
  RYDB_ERROR_FILE_SIZE      = 8,
  RYDB_ERROR_CONFIG_MISMATCH= 9,
  RYDB_ERROR_VERSION_MISMATCH= 11,
  RYDB_ERROR_REVISION_MISMATCH= 12,
  RYDB_ERROR_BAD_CONFIG= 13,
  RYDB_ERROR_WRONG_ENDIANNESS = 14
} rydb_error_code_t;

typedef struct {
  rydb_error_code_t    code;
  int                  errno_val;
  char                 str[RYDB_ERROR_MAX_LEN];
} rydb_error_t;

typedef struct {
  uint32_t revision;
  uint16_t row_len;
  uint16_t id_len; //id is part of the row, starting at row[0]
  uint16_t index_count;
} rydb_config_t;

typedef struct {
  char           *path;
  char           *name;
  rydb_file_t     data;
  rydb_file_t     meta;
  rydb_config_t   config;
  rydb_index_t   *index;
  rydb_error_t    error;
} rydb_t;



rydb_t *rydb_open(char *path, char *name, rydb_config_t *cf, rydb_config_index_t *index, rydb_error_t *err);
void rydb_close(rydb_t *rydb);

int rydb_insert(rydb_t *rydb, rydb_str_t *id, rydb_str_t *data);
int rydb_find(rydb_t *rydb, rydb_str_t *id); //return 1 if found, 0 if not found
int rydb_find_row(rydb_t *rydb, rydb_row_t *row); //id should be pre-filled

void rydb_error_print(rydb_error_t *err);



#endif //_RYDB_H
