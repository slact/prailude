#include <inttypes.h>
#include <limits.h>

#define CBDB_INDEX_MAX  ((uint16_t ) -1)
#define CBDB_INDEX_NULL ((uint16_t ) 0)

typedef uint16_t cbdb_index_t;

#define CBDB_ROW_LINKS_MAX 4

typedef struct {
  char           *str;
  uint16_t        len;
} cbdb_str_t;

typedef struct {
  cbdb_index_t    index;
  cbdb_str_t      id;
  cbdb_str_t      data;
  cbdb_index_t    link[CBDB_ROW_LINKS_MAX];
} cbdb_row_t;

