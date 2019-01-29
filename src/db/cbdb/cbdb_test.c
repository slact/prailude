#include "cbdb.h"
#include <stdio.h>
#include <errno.h>
#include <string.h>

int main(void) {
  cbdb_config_t cf = {
    .id_len = 10,
    .data_len = 50,
    .index_count = 0
  };
  cbdb_error_t err;
  cbdb_t *db = cbdb_open("./" ,"foo", &cf, NULL, &err);
  if(db) {
    printf("opened ok\n");
    cbdb_close(db);
    return 1;
  }
  else {
    printf("ERROR: %d: %s, errno %d %s\n", err.code, err.str, err.errno_val, strerror(err.errno_val));
    return 0;
  }
}
